"""Regression tests for the request-size bounds.

A RecordBatch with no buffers (Null-typed or zero-column) lets a caller declare
``num_rows`` in the billions from a few hundred bytes of IPC metadata. The
server must reject such a batch before it drives per-row allocation or task
submission, and the scalar-function executor must not queue an unbounded number
of futures for a legitimate batch.
"""

import os
import socket
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from tidbcloudlake_udf import UDFServer
from tidbcloudlake_udf.udf import DEFAULT_MAX_BATCH_ROWS

_SERVER_SCRIPT = os.path.join(os.path.dirname(__file__), "servers", "limits_server.py")
_PYTHON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _spawn_server(**env_overrides):
    port = _free_port()
    env = os.environ.copy()
    env["PYTHONPATH"] = _PYTHON_DIR
    env.update(env_overrides)
    process = subprocess.Popen(
        [sys.executable, _SERVER_SCRIPT, str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
    for _ in range(50):
        if process.poll() is not None:
            _, stderr = process.communicate()
            raise AssertionError(f"limits server failed to start: {stderr}")
        try:
            client.get_flight_info(fl.FlightDescriptor.for_path("builtin_healthy"))
            return port, process
        except Exception:
            time.sleep(0.1)
    process.terminate()
    raise AssertionError("limits server did not become ready")


def _stop(process):
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def small_batch_server():
    """Server with a small per-batch row bound."""
    port, process = _spawn_server(UDF_MAX_BATCH_ROWS="8")
    try:
        yield port
    finally:
        _stop(process)


@pytest.fixture
def default_batch_server():
    """Server with the shipped default per-batch row bound."""
    port, process = _spawn_server()
    try:
        yield port
    finally:
        _stop(process)


def _call_raw(port: int, function_name: str, batch: pa.RecordBatch):
    """Send an arbitrary RecordBatch through DoExchange and return the result."""
    client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
    descriptor = fl.FlightDescriptor.for_path(function_name)
    writer, reader = client.do_exchange(descriptor)
    with writer:
        writer.begin(batch.schema)
        writer.write_batch(batch)
        writer.done_writing()
        return [chunk.data for chunk in reader]


def test_default_batch_row_bound_is_set():
    assert DEFAULT_MAX_BATCH_ROWS == 1_000_000


def test_invalid_max_batch_rows_rejected():
    with pytest.raises(ValueError, match="max_batch_rows"):
        UDFServer(f"127.0.0.1:{_free_port()}", max_batch_rows=0)
    with pytest.raises(ValueError, match="max_batch_rows"):
        UDFServer(f"127.0.0.1:{_free_port()}", max_batch_rows=-1)


def test_oversized_declared_rows_rejected(default_batch_server):
    """A tiny batch declaring a billion rows is refused, not materialized."""
    batch = pa.RecordBatch.from_arrays([pa.nulls(1_000_000_000)], names=["x"])
    assert batch.nbytes == 0
    with pytest.raises(Exception, match="max_batch_rows"):
        _call_raw(default_batch_server, "builtin_healthy", batch)


def test_batch_within_bound_is_processed(small_batch_server):
    port = small_batch_server
    batch = pa.RecordBatch.from_arrays([pa.nulls(8)], names=["x"])
    batches = _call_raw(port, "builtin_healthy", batch)
    rows = sum(result.num_rows for result in batches)
    assert rows == 8


def test_batch_above_bound_is_rejected(small_batch_server):
    port = small_batch_server
    batch = pa.RecordBatch.from_arrays([pa.nulls(9)], names=["x"])
    with pytest.raises(Exception, match="max_batch_rows"):
        _call_raw(port, "builtin_healthy", batch)


def test_bounded_submission_keeps_row_order(default_batch_server):
    """Windowed future submission must not reorder or drop rows."""
    port = default_batch_server
    values = list(range(300))
    batch = pa.RecordBatch.from_arrays(
        [pa.array(values, type=pa.int32())], names=["value"]
    )
    batches = _call_raw(port, "add_one", batch)
    results = [value.as_py() for result in batches for value in result.column(0)]
    assert results == [value + 1 for value in values]


@pytest.mark.parametrize(
    "name", ["max_batch_bytes", "max_stream_rows", "max_stream_bytes"]
)
def test_invalid_resource_limits(name):
    with pytest.raises(ValueError, match=name):
        UDFServer(f"127.0.0.1:{_free_port()}", **{name: 0})


def test_input_byte_limit_over_flight():
    port, process = _spawn_server(UDF_MAX_BATCH_BYTES="16")
    try:
        batch = pa.record_batch(
            [pa.array(list(range(8)), type=pa.int32())], names=["value"]
        )
        with pytest.raises(Exception, match="max_batch_bytes"):
            _call_raw(port, "add_one", batch)
    finally:
        _stop(process)


def test_total_input_limit_across_batches():
    port, process = _spawn_server(UDF_MAX_STREAM_ROWS="8")
    try:
        client = fl.FlightClient(f"grpc://127.0.0.1:{port}")
        writer, reader = client.do_exchange(fl.FlightDescriptor.for_path("add_one"))
        batch = pa.record_batch(
            [pa.array(list(range(5)), type=pa.int32())], names=["value"]
        )
        with pytest.raises(Exception, match="max_stream_rows"):
            with writer:
                writer.begin(batch.schema)
                writer.write_batch(batch)
                writer.write_batch(batch)
                writer.done_writing()
                list(reader)
    finally:
        _stop(process)


def test_infinite_output_is_chunked_and_stopped_by_stream_limit():
    port, process = _spawn_server(UDF_MAX_STREAM_ROWS="2048")
    try:
        batch = pa.record_batch([pa.array([1], type=pa.int32())], names=["value"])
        with pytest.raises(Exception, match="max_stream_rows"):
            _call_raw(port, "endless", batch)
    finally:
        _stop(process)
