"""Simple client library for testing TiDB Cloud Lake UDF servers."""

import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.flight as fl

from .udf import _resolve_require_tls


# Wire protocol -- fixed by the query engine, not a brand name to rebrand.
_SCHEMA_METADATA_INPUT_COUNT_KEY = b"x-databend-udf-input-count"

# pyarrow selects the transport security from the URI scheme: ``grpc://`` is
# plaintext, ``grpc+tls://`` is TLS. TLS options passed with a plaintext
# location are dropped, so the scheme and TLS options must stay consistent.
PLAINTEXT_SCHEME = "grpc"
TLS_SCHEME = "grpc+tls"
_SUPPORTED_SCHEMES = (PLAINTEXT_SCHEME, TLS_SCHEME)

logger = logging.getLogger(__name__)


class UDFClient:
    """Simple client for calling UDF functions on a TiDB Cloud Lake UDF server."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8815,
        scheme: str = PLAINTEXT_SCHEME,
        tls_root_certs: bytes = None,
        cert_chain: bytes = None,
        private_key: bytes = None,
        disable_server_verification: bool = None,
        override_hostname: str = None,
        require_tls: Optional[bool] = None,
    ):
        """
        Initialize UDF client.

        Args:
            host: Server host (default: localhost)
            port: Server port (default: 8815)
            scheme: Transport scheme, ``"grpc"`` (plaintext, default) or
                ``"grpc+tls"``.
            tls_root_certs: PEM-encoded CA certificate(s) used to verify the
                server when ``scheme="grpc+tls"``.
            cert_chain: PEM-encoded client certificate, for mutual TLS.
            private_key: PEM-encoded client private key, for mutual TLS.
            disable_server_verification: Skip server certificate verification.
            override_hostname: Override the hostname checked by TLS.
            require_tls: Refuse plaintext; None inherits TIDBCLOUDLAKE_UDF_REQUIRE_TLS,
                matching previous releases, which allows plaintext with a
                warning when TLS options are supplied.
        """
        require_tls = _resolve_require_tls(require_tls)
        scheme = scheme.lower()
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(
                f"Unsupported transport scheme {scheme!r}; expected one of {_SUPPORTED_SCHEMES}"
            )
        tls_options = {
            "tls_root_certs": tls_root_certs,
            "cert_chain": cert_chain,
            "private_key": private_key,
            "disable_server_verification": disable_server_verification,
            "override_hostname": override_hostname,
        }
        tls_options = {
            key: value for key, value in tls_options.items() if value is not None
        }
        if scheme == PLAINTEXT_SCHEME:
            if require_tls:
                raise ValueError(
                    "require_tls=True but the transport scheme is 'grpc', which is "
                    "plaintext. Pass scheme='grpc+tls' to connect over TLS."
                )
            if tls_options:
                logger.warning(
                    "UDF client is connecting over PLAINTEXT: the transport scheme "
                    "is 'grpc', so the TLS options (%s) are being IGNORED and traffic "
                    "is not encrypted. Pass scheme='grpc+tls' to connect over TLS, "
                    "or require_tls=True to make this an error.",
                    ", ".join(sorted(tls_options)),
                )
                tls_options = {}
        self.scheme = scheme
        self.location = f"{scheme}://{host}:{port}"
        self.client = fl.FlightClient(self.location, **tls_options)
        self._schema_cache = {}

    def _get_cached_schema(self, function_name: str) -> pa.Schema:
        """Get function schema with lightweight caching."""
        if function_name not in self._schema_cache:
            info = self.get_function_info(function_name)
            self._schema_cache[function_name] = info.schema
        return self._schema_cache[function_name]

    @staticmethod
    def _get_input_field_count(schema: pa.Schema) -> int:
        """Extract the number of input columns from schema metadata."""
        metadata = schema.metadata or {}
        key = _SCHEMA_METADATA_INPUT_COUNT_KEY
        if metadata and key in metadata:
            try:
                return int(metadata[key].decode("utf-8"))
            except (ValueError, AttributeError):
                pass

        # Fallback for older servers without metadata: assume final column is output.
        return max(len(schema) - 1, 0)

    @staticmethod
    def _decode_result_batch(batch: pa.RecordBatch) -> List[Any]:
        """Convert a result RecordBatch into Python values."""
        num_columns = batch.num_columns
        num_rows = batch.num_rows

        if num_columns == 0:
            return [{} for _ in range(num_rows)]

        if num_columns == 1:
            column = batch.column(0)
            return [column[i].as_py() for i in range(num_rows)]

        field_names = [
            batch.schema.field(i).name or f"col{i}" for i in range(num_columns)
        ]

        rows: List[Dict[str, Any]] = []
        for row_idx in range(num_rows):
            row: Dict[str, Any] = {}
            for col_idx, name in enumerate(field_names):
                row[name] = batch.column(col_idx)[row_idx].as_py()
            rows.append(row)
        return rows

    def _prepare_function_call(
        self, function_name: str, args: tuple = (), kwargs: dict = None
    ) -> tuple:
        """
        Centralized helper to prepare schema and batch for function calls.

        Returns:
            tuple: (input_schema, batch)
        """
        kwargs = kwargs or {}
        schema = self._get_cached_schema(function_name)

        input_field_count = self._get_input_field_count(schema)
        total_fields = len(schema)
        if input_field_count > total_fields:
            raise ValueError(
                f"Function '{function_name}' schema metadata is invalid (input count {input_field_count} > total fields {total_fields})"
            )

        input_fields = [schema.field(i) for i in range(input_field_count)]
        input_schema = pa.schema(input_fields)

        # Validate arguments
        if args and kwargs:
            raise ValueError("Cannot mix positional and keyword arguments")

        if args:
            # Positional arguments - validate count first
            if len(args) != input_field_count:
                raise ValueError(
                    f"Function '{function_name}' expects {input_field_count} arguments, got {len(args)}"
                )

            if len(args) == 0:
                # For zero-argument functions, create a batch with 1 row and no columns
                dummy_array = pa.array([None])
                temp_batch = pa.RecordBatch.from_arrays(
                    [dummy_array], schema=pa.schema([pa.field("dummy", pa.null())])
                )
                batch = temp_batch.select([])
            else:
                arrays = []
                for i, arg in enumerate(args):
                    if isinstance(arg, list):
                        arrays.append(pa.array(arg, type=input_schema.field(i).type))
                    else:
                        arrays.append(pa.array([arg], type=input_schema.field(i).type))
                batch = pa.RecordBatch.from_arrays(arrays, schema=input_schema)
        else:
            # Keyword arguments
            if len(kwargs) == 0:
                input_schema = pa.schema([])
                # Create empty batch with one row
                dummy_array = pa.array([None])
                temp_batch = pa.RecordBatch.from_arrays(
                    [dummy_array], schema=pa.schema([pa.field("dummy", pa.null())])
                )
                batch = temp_batch.select([])
            else:
                # Validate kwargs
                expected_fields = {field.name for field in input_schema}
                provided_fields = set(kwargs.keys())

                missing = expected_fields - provided_fields
                extra = provided_fields - expected_fields

                if missing:
                    raise ValueError(f"Missing required arguments: {missing}")
                if extra:
                    raise ValueError(f"Unexpected arguments: {extra}")

                # Validate array lengths are consistent
                if kwargs:
                    array_lengths = [
                        len(v) if isinstance(v, list) else 1 for v in kwargs.values()
                    ]
                    if len(set(array_lengths)) > 1:
                        raise ValueError(
                            f"All batch arrays must have the same length, got lengths: {array_lengths}"
                        )

                # Create arrays in schema order
                arrays = []
                for field in input_schema:
                    arrays.append(pa.array(kwargs[field.name], type=field.type))
                batch = pa.RecordBatch.from_arrays(arrays, schema=input_schema)

        return input_schema, batch

    def get_function_info(self, function_name: str) -> fl.FlightInfo:
        """
        Get function schema information.

        Args:
            function_name: Name of the UDF function

        Returns:
            FlightInfo containing input and output schema
        """
        descriptor = fl.FlightDescriptor.for_path(function_name)
        return self.client.get_flight_info(descriptor)

    @staticmethod
    def format_stage_mapping(stage_locations: Iterable[Dict[str, Any]]) -> str:
        """Serialize stage mapping entries into the header payload."""

        serialized_entries: List[Dict[str, Any]] = []
        for entry in stage_locations:
            if not isinstance(entry, dict):
                raise ValueError("stage_locations entries must be dictionaries")
            if "param_name" not in entry:
                raise ValueError("stage_locations entry requires 'param_name'")
            serialized_entries.append(entry)

        return json.dumps(serialized_entries)

    @staticmethod
    def _build_flight_headers(
        headers: Dict[str, Any] = None,
        stage_locations: Iterable[Dict[str, Any]] = None,
    ) -> Sequence[Tuple[str, str]]:
        """Construct Flight headers for a UDF call.

        ``stage_locations`` becomes a single header named ``databend-stage-mapping``
        whose value is a JSON array. This mirrors what the query engine sends to
        external UDF servers. Example HTTP-style representation::

            databend-stage-mapping: [
              {
                "param_name": "stage_loc",
                "relative_path": "input/2024/",
                "stage_info": { ... StageInfo JSON ... }
              }
            ]

        Multiple stage parameters simply append more objects to the array.
        Additional custom headers can be supplied through ``headers``.
        """
        headers = headers or {}
        flight_headers: List[Tuple[bytes, bytes]] = []

        for key, value in headers.items():
            if isinstance(value, (list, tuple)):
                for item in value:
                    flight_headers.append(
                        (str(key).encode("utf-8"), str(item).encode("utf-8"))
                    )
            else:
                flight_headers.append(
                    (str(key).encode("utf-8"), str(value).encode("utf-8"))
                )

        if stage_locations:
            payload = UDFClient.format_stage_mapping(stage_locations)
            flight_headers.append((b"databend-stage-mapping", payload.encode("utf-8")))

        return flight_headers

    def call_function(
        self,
        function_name: str,
        *args,
        headers: Dict[str, Any] = None,
        stage_locations: Iterable[Dict[str, Any]] = None,
    ) -> List[Any]:
        """
        Call a UDF function with given arguments.

        Args:
            function_name: Name of the UDF function
            *args: Function arguments

        Returns:
            List of result values
        """
        input_schema, batch = self._prepare_function_call(function_name, args)

        # Call function
        descriptor = fl.FlightDescriptor.for_path(function_name)
        flight_headers = self._build_flight_headers(headers, stage_locations)
        options = (
            fl.FlightCallOptions(headers=flight_headers) if flight_headers else None
        )
        writer, reader = self.client.do_exchange(descriptor=descriptor, options=options)

        with writer:
            writer.begin(input_schema)
            writer.write_batch(batch)
            writer.done_writing()

            results = []
            for result_chunk in reader:
                result_batch = result_chunk.data
                results.extend(self._decode_result_batch(result_batch))

        return results

    def call_function_batch(
        self,
        function_name: str,
        headers: Dict[str, Any] = None,
        stage_locations: Iterable[Dict[str, Any]] = None,
        **kwargs,
    ) -> List[Any]:
        """
        Call a UDF function with batch data.

        Args:
            function_name: Name of the UDF function
            **kwargs: Named arguments with list values

        Returns:
            List of result values
        """
        input_schema, batch = self._prepare_function_call(function_name, kwargs=kwargs)

        # Call function
        descriptor = fl.FlightDescriptor.for_path(function_name)
        flight_headers = self._build_flight_headers(headers, stage_locations)
        options = (
            fl.FlightCallOptions(headers=flight_headers) if flight_headers else None
        )
        writer, reader = self.client.do_exchange(descriptor=descriptor, options=options)

        with writer:
            writer.begin(input_schema)
            writer.write_batch(batch)
            writer.done_writing()

            results = []
            for result_chunk in reader:
                result_batch = result_chunk.data
                results.extend(self._decode_result_batch(result_batch))

        return results

    def list_functions(self) -> List[str]:
        """
        List available functions by testing common function names.

        Returns:
            List of available function names
        """
        # Test built-in functions
        functions = []
        test_functions = ["builtin_echo", "builtin_healthy"]

        for func_name in test_functions:
            try:
                self.get_function_info(func_name)
                functions.append(func_name)
            except Exception:
                pass

        return functions

    def health_check(self) -> bool:
        """
        Check if server is healthy.

        Returns:
            True if server is healthy
        """
        try:
            result = self.call_function("builtin_healthy")
            return result == [1]
        except Exception:
            return False

    def echo(self, message: str) -> str:
        """
        Echo a message (useful for testing connectivity).

        Args:
            message: Message to echo

        Returns:
            Echoed message
        """
        result = self.call_function("builtin_echo", message)
        return result[0] if result else None


def create_client(
    host: str = "localhost",
    port: int = 8815,
    scheme: str = PLAINTEXT_SCHEME,
    **kwargs,
) -> UDFClient:
    """
    Create a UDF client instance.

    Args:
        host: Server host (default: localhost)
        port: Server port (default: 8815)
        scheme: Transport scheme, ``"grpc"`` (plaintext, default) or ``"grpc+tls"``.
        **kwargs: TLS options forwarded to :class:`UDFClient`
            (``tls_root_certs``, ``cert_chain``, ``private_key``, ...).

    Returns:
        UDFClient instance
    """
    return UDFClient(host, port, scheme=scheme, **kwargs)
