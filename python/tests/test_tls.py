"""Regression tests for transport security (TLS) reachability.

pyarrow derives the transport security of both the Flight server and the
Flight client from the URI scheme, so a hardcoded ``grpc://`` makes TLS
unreachable even when certificates are supplied. These tests pin the fix:
the scheme is configurable end to end and TLS options can no longer be
silently dropped.
"""

import os
import socket
import subprocess
import sys
import time

import pytest

from tidbcloudlake_udf import UDFClient, UDFServer
from tidbcloudlake_udf.udf import _build_create_function_sql

_SERVER_SCRIPT = os.path.join(os.path.dirname(__file__), "servers", "tls_server.py")
_PYTHON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _read(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


@pytest.fixture(scope="module")
def tls_certificates(tmp_path_factory):
    """Generate a self-signed certificate for localhost/127.0.0.1."""
    directory = tmp_path_factory.mktemp("tls")
    cert_path = directory / "cert.pem"
    key_path = directory / "key.pem"
    try:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                str(key_path),
                "-out",
                str(cert_path),
                "-days",
                "2",
                "-nodes",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:  # pragma: no cover
        pytest.skip(f"openssl is required to generate test certificates: {exc}")
    return str(cert_path), str(key_path)


def _spawn_tls_server(cert_path: str, key_path: str, port: int, **env_overrides):
    """Start the TLS test server in its own process and wait until it serves TLS."""
    env = os.environ.copy()
    env["PYTHONPATH"] = _PYTHON_DIR
    env["UDF_TLS_CERT"] = cert_path
    env["UDF_TLS_KEY"] = key_path
    env.update(env_overrides)
    process = subprocess.Popen(
        [sys.executable, _SERVER_SCRIPT, str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    probe = UDFClient(
        "127.0.0.1", port, scheme="grpc+tls", tls_root_certs=_read(cert_path)
    )
    for _ in range(50):
        if process.poll() is not None:
            _, stderr = process.communicate()
            raise AssertionError(f"TLS server failed to start: {stderr}")
        try:
            if probe.call_function("builtin_healthy") == [1]:
                return process
        except Exception:
            pass
        time.sleep(0.1)
    process.terminate()
    raise AssertionError("TLS server did not become ready")


def _stop(process):
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def tls_server(tls_certificates):
    cert_path, key_path = tls_certificates
    port = _free_port()
    process = _spawn_tls_server(cert_path, key_path, port)
    try:
        yield port
    finally:
        _stop(process)


def test_plaintext_scheme_rejects_tls_material(tls_certificates):
    """TLS options with a plaintext location must fail instead of downgrading."""
    cert_path, key_path = tls_certificates
    certificates = [(_read(cert_path), _read(key_path))]
    with pytest.raises(ValueError, match="grpc\\+tls"):
        UDFServer(f"127.0.0.1:{_free_port()}", tls_certificates=certificates)


def test_plaintext_scheme_rejects_verify_client():
    with pytest.raises(ValueError, match="grpc\\+tls"):
        UDFServer(f"127.0.0.1:{_free_port()}", verify_client=True)


def test_tls_scheme_requires_certificates():
    with pytest.raises(ValueError, match="tls_certificates"):
        UDFServer(f"127.0.0.1:{_free_port()}", scheme="grpc+tls")


def test_conflicting_schemes_rejected(tls_certificates):
    cert_path, key_path = tls_certificates
    certificates = [(_read(cert_path), _read(key_path))]
    with pytest.raises(ValueError, match="Conflicting transport schemes"):
        UDFServer(
            f"grpc://127.0.0.1:{_free_port()}",
            scheme="grpc+tls",
            tls_certificates=certificates,
        )


def test_unsupported_scheme_rejected():
    with pytest.raises(ValueError, match="Unsupported transport scheme"):
        UDFServer(f"127.0.0.1:{_free_port()}", scheme="https")


def test_tls_server_serves_over_tls(tls_server, tls_certificates):
    """A grpc+tls server is reachable by a TLS client."""
    cert_path, _ = tls_certificates
    client = UDFClient(
        "127.0.0.1",
        tls_server,
        scheme="grpc+tls",
        tls_root_certs=_read(cert_path),
    )
    assert client.call_function("builtin_healthy") == [1]
    assert client.call_function("builtin_echo", "secure") == ["secure"]


def test_plaintext_client_cannot_reach_tls_server(tls_server):
    """The listener is really TLS, not a plaintext listener with certificates."""
    client = UDFClient("127.0.0.1", tls_server)
    with pytest.raises(Exception):
        client.call_function("builtin_healthy")


def test_location_may_carry_the_scheme(tls_certificates):
    """Operators may pass a full grpc+tls:// location instead of a scheme."""
    cert_path, key_path = tls_certificates
    port = _free_port()
    process = _spawn_tls_server(
        cert_path,
        key_path,
        port,
        UDF_LOCATION=f"grpc+tls://127.0.0.1:{port}",
    )
    try:
        client = UDFClient(
            "127.0.0.1",
            port,
            scheme="grpc+tls",
            tls_root_certs=_read(cert_path),
        )
        assert client.call_function("builtin_healthy") == [1]
    finally:
        _stop(process)


def test_client_rejects_tls_options_on_plaintext(tls_certificates):
    cert_path, _ = tls_certificates
    with pytest.raises(ValueError, match="grpc\\+tls"):
        UDFClient("127.0.0.1", 1, tls_root_certs=_read(cert_path))


def test_client_default_scheme_stays_plaintext():
    """The default stays backward compatible for existing deployments."""
    client = UDFClient("127.0.0.1", 1)
    assert client.scheme == "grpc"
    assert client.location == "grpc://127.0.0.1:1"


def test_ddl_address_follows_transport_scheme():
    plaintext = _build_create_function_sql("f", "", "INT", "h:8815", "grpc")
    assert "ADDRESS = 'http://h:8815';" in plaintext
    encrypted = _build_create_function_sql("f", "", "INT", "h:8815", "grpc+tls")
    assert "ADDRESS = 'https://h:8815';" in encrypted
