from unittest.mock import Mock
import pytest
import pyarrow.flight as flight
from tidbcloudlake_udf import UDFClient, UDFServer
from tidbcloudlake_udf.udf import _resolve_require_tls


def test_client_inherits_server_tls_environment(monkeypatch):
    monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "true")
    with pytest.raises(ValueError, match="plaintext"):
        UDFClient()
    client = UDFClient(require_tls=False)
    client.client.close()
    monkeypatch.setenv("TIDBCLOUDLAKE_UDF_REQUIRE_TLS", "typo")
    with pytest.raises(ValueError, match="Invalid"):
        UDFClient()
    with pytest.raises(ValueError, match="Invalid"):
        _resolve_require_tls(None)


def test_verified_identity_replaces_caller_stage_grants():
    server = UDFServer.__new__(UDFServer)
    server._tenant_resolver = lambda identity: (
        "tenant-a" if identity == b"alice" else None
    )
    server._functions = {
        "read_input": Mock(_stage_ref_names=["input"]),
        "no_stage": Mock(_stage_ref_names=[]),
    }
    server._stage_grant_resolver = Mock(
        return_value={
            "input": {"stage": "owned-stage", "relative_path": "safe/"},
            "unrelated": {"stage": "another-stage", "relative_path": "private/"},
        }
    )
    context = Mock()
    context.peer_identity.return_value = b"alice"
    context.get_middleware.return_value = Mock(
        headers={
            "databend-stage-mapping": ['{"endpoint":"http://evil.invalid"}'],
            "DATABEND_STAGE_MAPPING": ['{"endpoint":"http://evil.invalid"}'],
            "x-databend-tenant": ["tenant-b"],
        }
    )
    headers = server._request_headers(context, function_name="read_input")
    server._stage_grant_resolver.assert_called_once_with(b"alice", "tenant-a")
    assert headers.tenant == "tenant-a"
    assert "evil.invalid" not in str(headers.raw_headers)
    assert "owned-stage" in str(headers.raw_headers)
    assert "another-stage" not in str(headers.raw_headers)
    assert not server._request_headers(
        context, function_name="no_stage"
    ).stage_locations
    server._stage_grant_resolver.side_effect = RuntimeError("dummy-secret")
    with pytest.raises(flight.FlightUnauthenticatedError) as error:
        server._request_headers(context, function_name="read_input")
    assert "dummy-secret" not in str(error.value)
