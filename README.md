## TiDB Cloud Lake UDF Server SDK
This library provides a SDK for creating user-defined functions (UDF) servers for TiDB Cloud Lake.

[![Python](https://img.shields.io/pypi/v/tidbcloudlake-udf)](https://pypi.org/project/tidbcloudlake-udf/)

### Introduction
TiDB Cloud Lake supports user-defined functions implemented as external functions. With the TiDB Cloud Lake Python UDF API, users can define custom UDFs using Python and start a Python process as a UDF server. Then users can call the customized UDFs in TiDB Cloud Lake. TiDB Cloud Lake will remotely access the UDF server to execute the defined functions.

## Quick Start

This SDK follows the external UDF pattern: define a Python function, run it in a
reachable UDF server, register its endpoint in TiDB Cloud Lake, and call it from
SQL. For background on this pattern, see the [Databend UDF articles](https://www.databend.com/blog/category-product/Databend_UDF/).

Install the Python package:

```bash
pip install tidbcloudlake-udf
```

Create `my_udf.py`:

```python
from tidbcloudlake_udf import UDFServer, udf


@udf(input_types=["INT", "INT"], result_type="INT")
def add(left: int, right: int) -> int:
    return left + right


if __name__ == "__main__":
    server = UDFServer("0.0.0.0:8815")
    server.add_function(add)
    server.serve()
```

Start the server on an address reachable by the query service, then register and
call the function:

```bash
python3 my_udf.py
```

```sql
CREATE FUNCTION add (INT, INT) RETURNS INT
LANGUAGE python HANDLER = 'add' ADDRESS = 'http://<udf-server-host>:8815';

SELECT add(1, 2); -- 3
```

The UDF endpoint must be permitted by your TiDB Cloud Lake UDF server allow
list. Do not use `0.0.0.0` as the `ADDRESS` in `CREATE FUNCTION`; replace the
placeholder with the hostname or IP address that TiDB Cloud Lake can reach.

## More Examples

- [Python SDK guide](python/README.md): supported SQL/Python types, NULL handling, table functions, I/O parallelism, and concurrency limits.
- [Runnable server example](python/example/server.py): scalar, complex, batch, and nullable UDFs.
- [Type examples](python/tests/servers/types_server.py): concrete type encoding and decoding coverage.
