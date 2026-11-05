#!/usr/bin/env python3
"""UDF server used by the request-size regression tests.

``UDF_MAX_BATCH_ROWS`` overrides the server-side per-batch row bound so a test
can exercise the rejection path with a tiny batch.
"""

import os
import sys

from tidbcloudlake_udf import UDFServer, udf
from tidbcloudlake_udf.udf import TableFunction


@udf(input_types=["INT"], result_type="INT", io_threads=4)
def add_one(value: int) -> int:
    return value + 1


def create_server(port: int) -> UDFServer:
    kwargs = {}
    max_batch_rows = os.environ.get("UDF_MAX_BATCH_ROWS")
    if max_batch_rows:
        kwargs["max_batch_rows"] = int(max_batch_rows)
    for name in ("max_batch_bytes", "max_stream_rows", "max_stream_bytes"):
        if os.environ.get("UDF_" + name.upper()):
            kwargs[name] = int(os.environ["UDF_" + name.upper()])
    server = UDFServer(f"127.0.0.1:{port}", **kwargs)
    server.add_function(add_one)

    def endless(value):
        while True:
            yield (value,)

    server.add_function(
        TableFunction(endless, ["INT"], [("value", "INT")], name="endless")
    )
    return server


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    create_server(port).serve()
