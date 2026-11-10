# TiDB Cloud Lake Python UDF

Write user-defined functions in Python for TiDB Cloud Lake.

## Quick Start

```python
from tidbcloudlake_udf import udf, UDFServer


@udf(input_types=["INT", "INT"], result_type="INT")
def add(a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    server = UDFServer("127.0.0.1:8815")
    server.add_function(add)
    server.serve()
```

```bash
python3 my_udf.py
```

```sql
CREATE FUNCTION add (INT, INT) RETURNS INT
LANGUAGE python HANDLER = 'add' ADDRESS = 'http://127.0.0.1:8815';

SELECT add(1, 2);  -- 3
```

## Transport Security (TLS)

The quick start assumes the query engine and UDF server are on the same
machine. A non-loopback listener requires TLS, verified peer identity and a
function authorization policy.

```python
server = UDFServer(
    "0.0.0.0:8815",
    tls_certificates=[(cert_pem, key_pem)],
    verify_client=True,
    root_certificates=ca_pem,
    function_authorizer=authorize_function,
)
server.serve()
```

The advertised `CREATE FUNCTION` address uses `https://`. Supplying TLS
certificates selects TLS when the scheme is omitted. Use `scheme="grpc+tls"`
to select it explicitly. A plaintext scheme with TLS settings is rejected.
Set `require_tls=True` or `TIDBCLOUDLAKE_UDF_REQUIRE_TLS=1` to require TLS for
loopback as well.

```python
client = UDFClient("udf-server.internal", 8815, tls_root_certs=ca_pem)
```

The client defaults to TLS for non-loopback hosts or when TLS options are
supplied. Its default local connection uses plaintext.

### Certificates on the engine side

When the advertised `ADDRESS` is `https://`, the query engine validates the
server certificate against the engine's system root CAs. A self-signed
certificate therefore makes `CREATE FUNCTION` fail with a TLS verification
error; use a certificate issued by a CA that the engine trusts, or install your
CA into the engine's trust store. `verify_client=True` (mTLS) additionally
requires the engine to present a client certificate.

TLS protects data in transit, but it does not by itself authenticate callers or
verify the `x-databend-tenant` header. Enable `verify_client` (mTLS) or pass a
pyarrow `auth_handler` when the endpoint is reachable by untrusted peers.

## Data Types

| SQL Type     | Python Type       | Example                    |
| ------------ | ----------------- | -------------------------- |
| BOOLEAN      | bool              | `True`                     |
| TINYINT      | int               | `127`                      |
| SMALLINT     | int               | `32767`                    |
| INT          | int               | `42`                       |
| BIGINT       | int               | `1000000`                  |
| UINT8        | int               | `255`                      |
| UINT16       | int               | `65535`                    |
| UINT32       | int               | `1000000`                  |
| UINT64       | int               | `1000000`                  |
| FLOAT        | float             | `3.14`                     |
| DOUBLE       | float             | `3.14159`                  |
| DECIMAL(P,S) | decimal.Decimal   | `Decimal("99.99")`         |
| DATE         | datetime.date     | `date(2024, 1, 15)`        |
| TIMESTAMP    | datetime.datetime | `datetime(2024, 1, 15, 10, 30)` |
| VARCHAR      | str               | `"hello"`                  |
| BINARY       | bytes             | `b"data"`                  |
| VARIANT      | any               | `{"key": "value"}`         |
| ARRAY(T)     | list[T]           | `[1, 2, 3]`                |
| MAP(K,V)     | dict[K,V]         | `{"a": 1, "b": 2}`         |
| TUPLE(T...)  | tuple(T...)       | `(1, "hello", True)`       |
| VECTOR(N)    | list[float]       | `[0.1, 0.2, 0.3]`          |

**Note:** SQL `NULL` maps to Python `None`.

## Examples

### Basic Types

```python
from tidbcloudlake_udf import udf
import datetime
from decimal import Decimal


@udf(input_types=["INT"], result_type="INT")
def double(x: int) -> int:
    return x * 2


@udf(input_types=["VARCHAR"], result_type="VARCHAR")
def upper(s: str) -> str:
    return s.upper()


@udf(input_types=["DATE", "INT"], result_type="DATE")
def add_days(d: datetime.date, days: int) -> datetime.date:
    return d + datetime.timedelta(days=days)


@udf(input_types=["DECIMAL(10,2)"], result_type="DECIMAL(10,2)")
def add_tax(price: Decimal) -> Decimal:
    return price * Decimal("1.1")
```

### Complex Types

```python
from typing import List, Dict, Tuple


@udf(input_types=["ARRAY(INT)"], result_type="INT")
def array_sum(arr: List[int]) -> int:
    return sum(arr)


@udf(input_types=["MAP(VARCHAR, INT)"], result_type="INT")
def map_sum(m: Dict[str, int]) -> int:
    return sum(m.values())


@udf(input_types=["TUPLE(INT, VARCHAR)"], result_type="VARCHAR")
def tuple_format(t: Tuple[int, str]) -> str:
    return f"{t[0]}: {t[1]}"


@udf(input_types=["VECTOR(128)"], result_type="VECTOR(128)")
def normalize_vector(v: List[float]) -> List[float]:
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm > 0 else v
```

### NULL Handling

```python
from typing import Optional


# Option 1: skip_null=True - NULL input returns NULL
@udf(input_types=["INT"], result_type="INT", skip_null=True)
def double(x: int) -> int:
    return x * 2


# Option 2: skip_null=False - Handle NULL manually
@udf(input_types=["INT"], result_type="INT", skip_null=False)
def double_or_zero(x: Optional[int]) -> int:
    return x * 2 if x is not None else 0


# Nullable array elements
@udf(input_types=["ARRAY(INT NULL)"], result_type="INT", skip_null=False)
def sum_non_null(arr: List[Optional[int]]) -> int:
    return sum(x for x in arr if x is not None)
```

### Table Functions

```python
@udf(
    input_types=["INT", "INT"],
    result_type=[("left", "INT"), ("right", "INT"), ("sum", "INT")],
    batch_mode=True,
)
def expand_pairs(left: List[int], right: List[int]):
    return [{"left": l, "right": r, "sum": l + r} for l, r in zip(left, right)]
```

```sql
SELECT * FROM expand_pairs([1, 2, 3], [10, 20, 30]);
```

### I/O Bound Functions

For I/O-bound functions (e.g., network requests, file operations), use `io_threads` to enable parallel processing of rows within a batch:

```python
import time


@udf(input_types=["INT"], result_type="INT", io_threads=32)
def fetch_data(id: int) -> int:
    time.sleep(0.1)  # Simulates I/O operation
    return id * 2
```

**Note:** `io_threads` controls parallelism within a single batch, not across requests.

### Concurrency Limiting

To protect your UDF server from being overwhelmed, use `max_concurrency` to limit the number of concurrent requests per function:

```python
from tidbcloudlake_udf import udf, UDFServer


@udf(input_types=["INT"], result_type="INT", max_concurrency=10)
def expensive_operation(x: int) -> int:
    # Only 10 concurrent requests allowed
    # Additional requests will wait for a slot
    return x * 2
```

By default, when the limit is reached, new requests **wait** until a slot becomes available. You can set a timeout to reject requests that wait too long:

```python
@udf(
    input_types=["INT"],
    result_type="INT",
    max_concurrency=10,
    concurrency_timeout=30,  # Wait up to 30 seconds, then reject
)
def expensive_operation(x: int) -> int:
    return x * 2
```

When the timeout expires, a `ConcurrencyLimitExceeded` error is raised.

You can combine `io_threads` and `max_concurrency`:

```python
@udf(
    input_types=["INT"],
    result_type="INT",
    io_threads=32,  # 32 threads for I/O within each request
    max_concurrency=5,  # But only 5 concurrent requests allowed
    concurrency_timeout=60,  # Wait up to 60s for a slot
)
def api_call(id: int) -> int:
    # Protected from too many concurrent requests
    return fetch_from_api(id)
```

### Request Size Limits

`UDFServer` rejects an inbound RecordBatch that declares more than
`max_batch_rows` rows (default `1_000_000`) before doing any per-row work. The
row count of a RecordBatch is metadata: a batch with no buffers can claim
billions of rows from a few hundred bytes, so this bound is what keeps a peer
from driving per-row allocation and task submission. Raise it (or pass `None`
to disable the bound) only if the query engine is configured to send larger
batches, and remember that larger batches mean more memory per request:

```python
server = UDFServer("127.0.0.1:8815", max_batch_rows=4_000_000)
```

Per-row work is also submitted in windows sized to `io_threads`, so the number
of in-flight tasks is bounded by the pool size rather than by the row count.

## Configuration

### TiDB Cloud Lake Config

Edit the TiDB Cloud Lake query service config:

```toml
[query]
enable_udf_server = true
udf_server_allow_list = ["http://127.0.0.1:8815"]
```

### UDF Decorator Parameters

| Parameter             | Type               | Default | Description                                          |
| --------------------- | ------------------ | ------- | ---------------------------------------------------- |
| `input_types`         | `List[str]`        | -       | Input SQL types                                      |
| `result_type`         | `str \| List[Tuple]` | -     | Return SQL type or table schema                      |
| `name`                | `str`              | None    | Custom function name                                 |
| `skip_null`           | `bool`             | False   | Auto-return NULL for NULL inputs                     |
| `io_threads`          | `int`              | 32      | Thread pool size for parallel row processing         |
| `batch_mode`          | `bool`             | False   | Enable for table functions                           |
| `max_concurrency`     | `int`              | None    | Max concurrent requests (None = unlimited)           |
| `concurrency_timeout` | `float`            | None    | Seconds to wait for a slot (None = wait forever)     |

## Additional Resources

- [Type Examples](tests/servers/types_server.py) - Scalar, complex, nullable, and VECTOR types
- [Type Test Cases](tests/test_types.py) - How types are encoded/decoded and used in UDFs
- [UDF Usage Examples](tests/test_udf.py) - Scalar and table UDF usage patterns
- [Server Usage](tests/test_server.py) - Starting a UDF server and basic connectivity

## Release Notes

### 0.3.0

- **Transport security is configurable end to end.** `UDFServer` accepts
  `scheme="grpc+tls"` (or a `grpc+tls://host:port` location) together with
  `tls_certificates`, `verify_client` and `root_certificates`; `UDFClient`
  accepts `scheme`, `tls_root_certs`, `cert_chain`, `private_key`,
  `disable_server_verification` and `override_hostname`. Previously both sides
  hardcoded `grpc://`, so TLS was unreachable and TLS options were silently
  ignored. The advertised `CREATE FUNCTION` DDL now uses `https://` when
  serving over TLS.
- **New `require_tls` strict mode** on `UDFServer` (and `UDFClient`), plus the
  `TIDBCLOUDLAKE_UDF_REQUIRE_TLS` environment variable, to refuse plaintext
  instead of only warning.
- **Request-size bounds:** `UDFServer(max_batch_rows=1_000_000)` rejects
  inbound batches that declare more rows than the bound, and per-row work is
  submitted in windows sized to `io_threads` instead of one future per row.
- **Upgrade behaviour:** a plaintext scheme combined with TLS material keeps
  starting (as in previous releases) but logs a warning; it no longer serves
  that combination silently. See the Transport Security section above.

## Development

Format code with Ruff:

```bash
pip install ruff
ruff format python/tidbcloudlake_udf python/tests
```

---

Inspired by [RisingWave Python API](https://pypi.org/project/risingwave/).

### Requiring authentication and binding tenant identity

`UDFServer(..., require_auth=True, auth_handler=handler)` refuses to start
without an Arrow Flight authentication handler. TLS with `verify_client=True`
also satisfies this guard. Combine this with `scheme="grpc+tls"` and
`require_tls=True`; authentication alone does not encrypt query data or stage
credentials. Pass `function_authorizer` to choose which functions each verified peer can call.

For shared services, pass `tenant_resolver=resolve_tenant` to map the verified
`context.peer_identity()` bytes to an authorized tenant string. It requires an
authentication configuration and a nonempty peer identity, and replaces the
caller-supplied tenant header before invoking a UDF. Returning an empty tenant
rejects the request. Configure the resolver with your trusted identity-to-tenant
mapping; it must not derive the tenant from request headers. Verify that your
engine supports the selected Flight authentication or mTLS configuration. mTLS
alone does not guarantee Arrow exposes a peer identity suitable for this mapping.
Without a resolver, `headers.tenant` is empty.

### Request resource budgets

`UDFServer` defaults to 64 MiB per decoded input/output batch and, separately for
each direction, 10 million rows / 256 MiB per DoExchange stream. At most 32 exchanges run concurrently by default.
Configure concurrency with `max_concurrent_requests` and the stream limits
with positive `max_batch_bytes`, `max_stream_rows` and `max_stream_bytes` values.
The existing per-batch row limit also applies to output. Table-function iterables
and column mappings are consumed in chunks of at most 1024 rows, so even an
infinite iterable reaches the stream budget instead of collecting forever.
Checks run before input conversion and before each output write. A rejected
stream may already have delivered earlier output batches; consumers must treat
the failed exchange as unsuccessful.

These are framework budgets after Arrow decodes each incoming batch. Python's
FlightServerBase does not expose gRPC receive-message-size options; use the
caller/runtime's transport limit for the native decoding boundary. Function code
is trusted: a function that allocates memory or never yields cannot be sandboxed
by these counters. Arbitrary function isolation requires a separate worker design.

### Verified stage grants

For a shared authenticated server, pass `tenant_resolver(identity)` and
`stage_grant_resolver(identity, tenant)` alongside `auth_handler` or mTLS. The
stage resolver returns the stage-mapping dictionary from trusted configuration.
The server forwards only grants for the requested function’s declared stage
references. An empty mapping grants no stages. Resolver failures reject the
request without exposing credentials. The resolver runs on each invocation of
a function that declares stage references.
Legacy caller-delegated stage credentials remain available when no stage resolver
is configured, and should only be accepted from trusted callers.

Both server and client inherit `TIDBCLOUDLAKE_UDF_REQUIRE_TLS` when
`require_tls=None`. Invalid values fail closed; an explicit boolean overrides
the environment. Authentication and storage grants do not substitute for TLS.


### Network and function access policy

Flight and metrics bind to loopback by default when using the example server.
A non-loopback Flight service requires TLS and an authentication handler or
verified client certificates. The unauthenticated metrics listener accepts only
loopback addresses. The operator may set `TIDBCLOUDLAKE_UDF_ALLOW_INSECURE_NETWORK=1` for a trusted
legacy network. Keep it disabled for any network reachable by untrusted clients.

With `tenant_resolver`, configure `function_authorizer(identity, tenant, name)`
to return `True` for the functions that identity may discover and invoke.
Caller-supplied tenant headers are ignored. Stage grants come from
`stage_grant_resolver` by default. To run a trusted legacy integration using
caller-supplied stage mappings, set `allow_untrusted_stage_mapping=True`
explicitly and restrict which warehouse peer can connect.
