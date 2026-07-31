# Python datasources on BigQuery

A python datasource is a script referenced by a `file` address:

```
datasource fib_numbers(
    index: fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;
```

The contract is the same on every engine: the script is run with
`uv run --no-project --quiet <script>` and writes an **Arrow IPC stream** to
stdout.

## How DuckDB does it

DuckDB reads the stream in-process. `get_python_datasource_setup_sql` installs
a `uv_run` macro over `read_arrow(...)`, and `render_source` emits
`uv_run('/abs/path/script.py')` straight into the FROM clause. Nothing is
staged; the script runs when the query runs.

## How BigQuery does it

BigQuery cannot run a local process, so the script's output is streamed to a
parquet object in GCS and the query reads that object. Batches are written as
they arrive (`pyarrow.parquet.ParquetWriter` over a `pyarrow.fs` output
stream), so neither the Arrow stream nor the parquet file is ever fully
buffered in memory or spilled to local disk.

There are two ways to make BigQuery read the object.

### Temp table definitions (default)

BigQuery has no inline object-store table function — there is no
`read_parquet('gs://…')`. But a *query job* can carry `tableDefinitions`:
per-job external tables that exist only for that job. The query then references
a bare name:

```sql
SELECT count(`states`.`name`) AS `state_count`
FROM `trilogy_py_fib_c24cc58f7d45` AS `states`
```

Nothing is written to the catalog, there is no DDL round-trip, and no dataset
permissions are needed. This is the default whenever `staging_dataset` is unset.

`tableDefinitions` is job configuration, which SQLAlchemy has no way to pass
through to the cursor — so this mode requires trilogy's native
`BigQueryEngine` (see below).

### External tables (`staging_dataset` set)

`CREATE OR REPLACE EXTERNAL TABLE` points a catalog entry at the object.
External tables hold no data, so this is a metadata-only statement, but it
costs an extra job per script. Use it when you want the staged table to be
queryable outside trilogy, or when you are pinned to `use_sqlalchemy=True`.

## The native BigQuery engine

`trilogy/dialect/bigquery_engine.py` implements trilogy's `ExecutionEngine` /
`EngineConnection` protocols directly over `google-cloud-bigquery`, and is the
default for the `bigquery` dialect.

`BigQueryConfig(use_sqlalchemy=True)` restores the old sqlalchemy-bigquery path
(and forces external-table staging). **It is a migration escape hatch kept for
one release** — nothing else in trilogy needs a SQLAlchemy engine for BigQuery:
schema introspection goes through `information_schema` SQL, and BigQuery sets
none of the `*_NOT_FOUND_PATTERN` error classifiers. Expect it to be removed;
report anything that only works with it set.

Two things it does differently from the SQLAlchemy path, both deliberate:

- **Query parameters.** Bind markers are rewritten `:name` → `@name` and sent
  as BigQuery query parameters, rather than inlined as literals. BigQuery
  escapes strings with backslashes (SQLAlchemy's literal renderer would
  mis-quote them) and has no literal renderer for arrays at all.
- **Rows.** BigQuery's `Row` is not tuple-equal, which SQLAlchemy `Row`
  consumers rely on, so results are re-wrapped as namedtuples — index,
  attribute and tuple-equality access all behave as before. The re-wrap is
  lazy (`streamed_rows`): `job.result()` still blocks until the query finishes,
  so a failed query raises from `execute` inside the retry wrapper, but the
  rows themselves page off the job as they are read. An unbounded select never
  has to fit in memory. This is safe because a BigQuery `RowIterator` belongs
  to its own job rather than to a shared cursor, so it stays valid after the
  connection runs the next statement — a driver without that property must use
  `buffered_rows` instead.

BigQuery has no transactions outside a script, so the connection reports
`in_transaction() == False` and the executor never adopts or commits an
implicit transaction.

## Naming and cleanup

The staged name is keyed on **script path + args**, not script contents: a
given script maps to one stable artifact. Hashing contents would leave an
orphaned object behind every edit.

A script is staged **once per executor**, not once per statement — the first
query that touches it pays for the run, later ones reuse the artifact.

| | Object path | Cleaned up on `executor.close()` |
| --- | --- | --- |
| Temp definitions | `<staging_uri>/<executor-uuid>/…` | Yes |
| External tables | `<staging_uri>/…` | No — deleting it would leave the table dangling |

Teardown cleanup is best-effort and never raises. It also cannot run at all if
the process is killed, so **put a lifecycle rule on the staging bucket** — that
is the backstop for both cases, and the only cleanup that exists in
external-table mode.

## Bucket lifecycle

GCS has no per-object TTL. (Object *retention* in GCS is a deletion **lock** —
the opposite of expiry.) Expiry is a bucket-level lifecycle rule, so give the
staging bucket — or a dedicated prefix — an age-based rule:

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 1, "matchesPrefix": ["trilogy-staging/"]}
      }
    ]
  }
}
```

```bash
gcloud storage buckets update gs://my-bucket --lifecycle-file=lifecycle.json
```

Use a bucket (or prefix) dedicated to staging. `age` is in days and is the
minimum granularity GCS offers; one day is ample given trilogy deletes its own
objects at teardown in the default mode.

The same applies to any other object storage trilogy stages into — DuckDB's
`[staging] path`, COPY targets — with the local-filesystem case being the only
one trilogy cleans up unconditionally (`StagingConfig.register_cleanup`).

## Configuration

```toml
[engine]
dialect = "bigquery"

[engine.config]
project = "my-project"
staging_uri = "gs://my-bucket/trilogy-staging"
enable_python_datasources = true
# staging_dataset = "trilogy_staging"   # opt into persistent external tables
# use_sqlalchemy = true                 # opt back into sqlalchemy-bigquery
```

`staging_uri` may be omitted if `[staging] path` is a `gs://` URI; the
`[engine.config]` value wins when both are set. From python:

```python
Dialects.BIGQUERY.default_executor(
    conf=BigQueryConfig(
        staging_uri="gs://my-bucket/trilogy-staging",
        enable_python_datasources=True,
    ),
)
```

Application default credentials need read/write/delete on the staging prefix,
and `bigquery.tables.create` on the staging dataset if you use external-table
mode.

## Where the code lives

| Concern | Module |
| --- | --- |
| uv command, retry markers, Arrow→parquet streaming, staged names | `trilogy/dialect/python_source.py` |
| DuckDB `uv_run` macro | `trilogy/dialect/duckdb.py` |
| Windows uv wrapper (temp-file workaround) | `trilogy/dialect/duckdb_uv.py` |
| GCS staging, external table DDL, teardown cleanup | `trilogy/dialect/bigquery_staging.py` |
| Native client, query parameters, job config | `trilogy/dialect/bigquery_engine.py` |
| Address → table reference, staging config resolution | `trilogy/dialect/bigquery.py` |

Shared by every engine adapter that does not go through SQLAlchemy (BigQuery,
chdb): `statement_to_sql` and `NonTransactionalConnection` in `trilogy/engine.py`,
and `buffered_rows` / `namedtuple_row_class` in `trilogy/dialect/results.py`.

The generic hooks are `BaseDialect.prepare_sources(addresses, executor)` and
`BaseDialect.teardown()`. When a dialect sets `REQUIRES_SOURCE_PREPARATION`,
the executor calls `prepare_sources` with every leaf source address a query
reads (`collect_source_addresses`) before compiling, so a dialect that can only
reference a source by name gets a chance to create it; `teardown` runs from
`Executor.close()`. DuckDB and every other dialect leave the flag off and skip
the walk entirely.

`prepare_sources` fires from `Executor.compile_for_execution` (and its plural
`compile_statements_for_execution`), which is the single seam between a
processed statement and SQL that is about to run — selects, persists, copies
and chart layers all route through it. `generator.compile_statement` stays
side-effect free, so the render-only paths (`generate_sql`, `show`, metadata)
never run a script or write to GCS just to display SQL. **A new execution path
must call `compile_for_execution`, not `generator.compile_statement`.**
