from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow.flight as flight
import pytest
import pyarrow as pa
from tidbcloudlake_udf import UDFServer, udf


def test_strict_auth_rejects_anonymous_configuration():
    with pytest.raises(ValueError, match="requires"):
        UDFServer("127.0.0.1:0", require_auth=True)
    with pytest.raises(ValueError, match="requires"):
        UDFServer("127.0.0.1:0", verify_client=True, require_auth=True)
    with pytest.raises(ValueError, match="requires"):
        UDFServer("127.0.0.1:0", tenant_resolver=lambda _: "tenant")


def policy_server(resolver):
    # Exercise the policy before transport/UDF dispatch without opening a socket.
    server = UDFServer.__new__(UDFServer)
    server._tenant_resolver = resolver
    return server


def test_tenant_comes_from_verified_identity():
    resolver = Mock(return_value="trusted-tenant")
    server = policy_server(resolver)
    context = Mock()
    context.peer_identity.return_value = b"authenticated-user"
    context.get_middleware.return_value = SimpleNamespace(
        headers={"X-Databend-Tenant": ["forged"], "x-databend-tenant": ["also-forged"]}
    )
    headers = server._request_headers(context)
    resolver.assert_called_once_with(b"authenticated-user")
    assert headers.tenant == "trusted-tenant"
    assert headers.get("x-databend-tenant") == "trusted-tenant"
    assert headers.raw_headers == {"x-databend-tenant": ["trusted-tenant"]}


@pytest.mark.parametrize(
    "identity,tenant", [(b"", "trusted"), (b"user", ""), (b"user", None)]
)
def test_unmapped_or_anonymous_peer_is_rejected(identity, tenant):
    server = policy_server(lambda _: tenant)
    context = Mock()
    context.peer_identity.return_value = identity
    context.get_middleware.return_value = None
    with pytest.raises(flight.FlightUnauthenticatedError):
        server._request_headers(context)


class _ServerAuth(flight.ServerAuthHandler):
    def authenticate(self, outgoing, incoming):
        if incoming.read() != b"local-test-secret":
            raise flight.FlightUnauthenticatedError("Invalid credential")
        outgoing.write(b"local-test-token")

    def is_valid(self, token):
        if token != b"local-test-token":
            raise flight.FlightUnauthenticatedError("Invalid token")
        return b"verified-user"


class _ClientAuth(flight.ClientAuthHandler):
    def authenticate(self, outgoing, incoming):
        outgoing.write(b"local-test-secret")
        self.token = incoming.read()

    def get_token(self):
        return self.token


def test_flight_transport_rejects_anonymous_and_accepts_authenticated_peer():
    server = UDFServer(
        "127.0.0.1:0",
        require_auth=True,
        auth_handler=_ServerAuth(),
        tenant_resolver=lambda identity: (
            "tenant" if identity == b"verified-user" else ""
        ),
        function_authorizer=lambda identity, tenant, name: (
            name in {"builtin_healthy", "trusted_tenant"}
        ),
    )
    try:
        descriptor = flight.FlightDescriptor.for_path("builtin_healthy")
        anonymous = flight.FlightClient(f"grpc://127.0.0.1:{server.port}")
        with pytest.raises(flight.FlightUnauthenticatedError):
            anonymous.get_flight_info(descriptor)
        anonymous.close()
        client = flight.FlightClient(f"grpc://127.0.0.1:{server.port}")
        client.authenticate(_ClientAuth())
        assert client.get_flight_info(descriptor).schema is not None
        with pytest.raises(flight.FlightUnauthorizedError):
            client.get_flight_info(
                flight.FlightDescriptor.for_path("other_tenant_function")
            )

        @udf(input_types=["VARCHAR"], result_type="VARCHAR")
        def trusted_tenant(value, headers):
            return headers.tenant

        server.add_function(trusted_tenant)
        options = flight.FlightCallOptions(headers=[(b"x-databend-tenant", b"forged")])
        writer, reader = client.do_exchange(
            flight.FlightDescriptor.for_path("trusted_tenant"), options
        )
        writer.begin(trusted_tenant._input_schema)
        writer.write_batch(
            pa.RecordBatch.from_arrays(
                [pa.array(["value"])], schema=trusted_tenant._input_schema
            )
        )
        writer.done_writing()
        assert reader.read_all().column(0).to_pylist() == ["tenant"]
        writer.close()
        client.close()
    finally:
        server.shutdown()


def test_plaintext_and_anonymous_flight_are_local_by_default(monkeypatch):
    monkeypatch.delenv("TIDBCLOUDLAKE_UDF_ALLOW_INSECURE_NETWORK", raising=False)
    with pytest.raises(ValueError, match="Non-loopback Flight"):
        UDFServer("0.0.0.0:0")


def test_unverified_stage_and_tenant_headers_are_ignored():
    server = policy_server(None)
    context = Mock()
    context.get_middleware.return_value = SimpleNamespace(
        headers={
            "DATABEND_STAGE_MAPPING": ["caller-grant"],
            "X-Databend-Tenant": ["caller-tenant"],
        }
    )
    assert server._request_headers(context).raw_headers == {}
