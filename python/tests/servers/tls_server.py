#!/usr/bin/env python3
"""TLS-enabled UDF server used by the transport-security tests.

Configured through the environment so a single script covers both ways of
enabling TLS (an explicit ``scheme`` or a ``grpc+tls://`` location):

- ``UDF_TLS_CERT`` / ``UDF_TLS_KEY``: PEM files for the server certificate.
- ``UDF_LOCATION``: location passed verbatim to ``UDFServer`` (may carry a
  ``grpc+tls://`` prefix).
- ``UDF_SCHEME``: transport scheme when the location has no prefix.
"""

import logging
import os
import sys

from tidbcloudlake_udf import UDFServer

logging.basicConfig(level=logging.INFO)


def create_server(port: int) -> UDFServer:
    location = os.environ.get("UDF_LOCATION") or f"127.0.0.1:{port}"
    kwargs = {}
    if "://" not in location and os.environ.get("UDF_SCHEME"):
        kwargs["scheme"] = os.environ["UDF_SCHEME"]
    cert_path = os.environ.get("UDF_TLS_CERT")
    key_path = os.environ.get("UDF_TLS_KEY")
    if cert_path and key_path:
        with open(cert_path, "rb") as cert_file, open(key_path, "rb") as key_file:
            kwargs["tls_certificates"] = [(cert_file.read(), key_file.read())]
    return UDFServer(location, **kwargs)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8815
    create_server(port).serve()
