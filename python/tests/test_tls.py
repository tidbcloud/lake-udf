"""Regression tests for transport security (TLS) reachability.

pyarrow derives the transport security of both the Flight server and the
Flight client from the URI scheme, so a hardcoded ``grpc://`` makes TLS
unreachable even when certificates are supplied. These tests pin the fix:
the scheme is configurable end to end and TLS options are no longer silently
dropped. Because previous releases kept serving plaintext with TLS options in
that configuration, the default is now a loud warning (not a startup error);
``require_tls=True`` / ``TIDBCLOUDLAKE_UDF_REQUIRE_TLS=1`` opts into the strict
fail-closed behaviour.
"""

import os
import socket
import subprocess
import sys
import time

import pytest

from tidbcloudlake_udf import UDFClient, UDFServer
from tidbcloudlake_udf.udf import _build_create_function_sql, _resolve_require_tls

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


def test_plaintext_with_verify_client_is_rejected():
    with pytest.raises(ValueError, match="TLS options require"):
        UDFServer._check_transport_security("grpc", {"verify_client": True})


def test_strict_mode_rejects_plaintext_with_tls_material(tls_certificates):
    cert_path, key_path = tls_certificates
    certificates = [(_read(cert_path), _read(key_path))]
    with pytest.raises(ValueError, match="require_tls"):
        UDFServer._check_transport_security(
            "grpc", {"tls_certificates": certificates}, True
        )


def test_strict_mode_rejects_bare_plaintext():
    with pytest.raises(ValueError, match="require_tls"):
        UDFServer._check_transport_security("grpc", {}, True)


def test_strict_mode_allows_tls(tls_certificates):
    cert_path, key_path = tls_certificates
    certificates = [(_read(cert_path), _read(key_path))]
    UDFServer._check_transport_security(
        "grpc+tls", {"tls_certificates": certificates}, True
    )


def test_server_require_tls_true_rejects_plaintext():
    with pytest.raises(ValueError, match="require_tls"):
        UDFServer(f"127.0.0.1:{_free_port()}", require_tls=True)


def test_server_require_tls_env_var_rejects_plaintext(monkeypatch):
    monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "1")
    with pytest.raises(ValueError, match="require_tls"):
        UDFServer(f"127.0.0.1:{_free_port()}")


def test_env_var_parsing(monkeypatch):
    monkeypatch.delenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", raising=False)
    assert _resolve_require_tls(None) is False
    for value in ("1", "true", "YES", "on"):
        monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", value)
        assert _resolve_require_tls(None) is True
    monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "0")
    assert _resolve_require_tls(None) is False
    # An explicit argument wins over the environment.
    monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "1")
    assert _resolve_require_tls(False) is False


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


def test_client_tls_options_select_tls(tls_server, tls_certificates):
    cert_path, _ = tls_certificates
    client = UDFClient("127.0.0.1", tls_server, tls_root_certs=_read(cert_path))
    assert client.scheme == "grpc+tls"
    assert client.call_function("builtin_healthy") == [1]
    client.client.close()


def test_client_rejects_tls_options_with_explicit_plaintext(tls_certificates):
    cert_path, _ = tls_certificates
    with pytest.raises(ValueError, match="TLS options require"):
        UDFClient("127.0.0.1", 1, scheme="grpc", tls_root_certs=_read(cert_path))


def test_client_require_tls_rejects_plaintext():
    with pytest.raises(ValueError, match="require_tls"):
        UDFClient("127.0.0.1", 1, require_tls=True)


@pytest.mark.parametrize(
    "options",
    [
        {"disable_server_verification": True},
        {"override_hostname": "other.example"},
    ],
)
@pytest.mark.parametrize("strict_from_env", [False, True])
def test_client_require_tls_rejects_verification_overrides(
    monkeypatch, options, strict_from_env
):
    if strict_from_env:
        monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "1")
    else:
        options = {**options, "require_tls": True}
    with pytest.raises(ValueError, match="requires server certificate verification"):
        UDFClient("udf.example", 8815, **options)


def test_client_require_tls_allows_tls(tls_server, tls_certificates):
    cert_path, _ = tls_certificates
    client = UDFClient(
        "127.0.0.1",
        tls_server,
        scheme="grpc+tls",
        tls_root_certs=_read(cert_path),
        disable_server_verification=False,
        require_tls=True,
    )
    assert client.scheme == "grpc+tls"
    assert client.call_function("builtin_healthy") == [1]


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


def _spawn_with_stderr_file(cert_path, key_path, port, stderr_path, **env_overrides):
    env = os.environ.copy()
    env["PYTHONPATH"] = _PYTHON_DIR
    env["UDF_TLS_CERT"] = cert_path
    env["UDF_TLS_KEY"] = key_path
    env.update(env_overrides)
    with open(stderr_path, "w") as stderr_file:
        return subprocess.Popen(
            [sys.executable, _SERVER_SCRIPT, str(port)],
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            text=True,
            env=env,
        )


def test_server_rejects_tls_options_with_explicit_plaintext(tls_certificates):
    cert_path, key_path = tls_certificates
    with pytest.raises(ValueError, match="TLS options require"):
        UDFServer(
            "127.0.0.1:0",
            scheme="grpc",
            tls_certificates=[(_read(cert_path), _read(key_path))],
        )


def test_strict_env_var_refuses_to_start(tmp_path, tls_certificates):
    """TIDBCLOUDLAKE_UDF_REQUIRE_TLS=1 turns the same config into a startup error."""
    cert_path, key_path = tls_certificates
    port = _free_port()
    stderr_path = tmp_path / "server.err"
    process = _spawn_with_stderr_file(
        cert_path,
        key_path,
        port,
        stderr_path,
        UDF_SCHEME="grpc",
        TIDBCLOUDLAKE_UDF_REQUIRE_TLS="1",
    )
    try:
        returncode = process.wait(timeout=30)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
    assert returncode != 0
    stderr = stderr_path.read_text()
    assert "require_tls" in stderr
    assert "plaintext" in stderr
