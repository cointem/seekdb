# File SQL regression tests

The server and client must share the fixture filesystem. Use an existing `test`
database and an existing writable directory permitted by `secure_file_priv`.
The server's default `enable_file_sql` must be OFF; the case enables it as needed.
For password authentication, set the client's `MYSQL_PWD` environment variable.

Run the mysqltest result comparison from the repository root:

```sh
python3 unittest/sql/engine/file_sql/run_mysqltest.py \
  --mysqltest /path/to/obclient/bin/mysqltest \
  --port 12883 \
  --data-dir /path/allowed/by/secure_file_priv \
  --log-dir /path/to/test-artifacts/file-sql
```

This runner creates and removes a private fixture subdirectory, assigns
`$FILE_SQL_DATA_DIR` with a mysqltest `let` statement, and runs the same SQL
assertions and expected result as `file_sql_basic.test`. Only fixture setup and
cleanup differ from the suite's SSH deployment path.

## Evaluator startup failure

The bundled mysqltest does **not** support
`--variable=FILE_SQL_DATA_DIR=/path/to/data`. It rejects that argument before
connecting to the server. The evaluator must remove that argument and assign
the variable inside its test file instead:

```text
let $FILE_SQL_DATA_DIR = /artifacts/runtime-example/file-sql-data;
```

Alternatively, invoke the runner above. A database implementation change cannot
make mysqltest accept an unsupported startup argument. This repository's runner
does not modify an external evaluator or prove its private tests pass.
