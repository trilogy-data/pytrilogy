# Script datasources: the IO contract

A datasource can be a program. Trilogy runs it, passes it a request on the
command line, and reads an Arrow IPC stream back from stdout.

```
datasource landmarks(
    id: landmark_id,
    name: landmark_name,
    state: landmark_state
)
grain (landmark_id)
file `./landmarks.py`;
```

Two libraries make writing that program a few lines: `trilogy.io` (python,
ships in the wheel) and the [`trilogy-io`](../crates/trilogy-io) crate (rust,
published to crates.io on the same version stream). Neither is required --
the contract is the command line, and any executable that honors it works.

## Writing one in python

```python
#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "pytrilogy"]
# ///
import pandas as pd
from trilogy.io import run


def landmarks() -> pd.DataFrame:
    return pd.read_csv("https://example.org/landmarks.csv")


if __name__ == "__main__":
    raise SystemExit(run(landmarks))
```

Return whatever you already have. `run` accepts:

| you return | how it is handled |
| --- | --- |
| `pa.Table`, `pa.RecordBatch`, `pa.RecordBatchReader` | directly |
| polars / duckdb / ibis / datafusion / pandas 3 | the Arrow PyCapsule interface (`__arrow_c_stream__`) |
| a pandas 2 `DataFrame` | `pa.Table.from_pandas` |
| `list[dict]` / `dict[str, list]` | `from_pylist` / `from_pydict` |
| an iterator of any of the above | streamed, never materialized |
| `None` or an empty list | an empty result at the declared `schema=` |

Anything else: `register_adapter(predicate, converter)`.

A `schema=` is authoritative whichever of those you returned: the output is cast
onto it, reordered to it, and narrowed to the columns it names, so `--describe`
cannot disagree with the stream. A column it names that the source did not
produce is an error. Without one, the source's own schema is used and only an
empty result is a problem — there is nothing to infer from.

`emit(fn)` is the older entrypoint and still works; it goes through the same
pipeline, so scripts written against it pick all of this up unchanged.

## Writing one in rust

```rust
use trilogy_io::{Field, Result, SourceRequest};
use arrow::array::RecordBatch;

fn landmarks(request: &SourceRequest) -> Result<Vec<RecordBatch>> { ... }

fn main() -> std::process::ExitCode {
    trilogy_io::source(landmarks).pushdown(&[Field::Limit]).run()
}
```

The binary implements the same contract, so `trilogy source describe|preview|check`
work against it and any consumer that invokes it directly gets the same answers
as from a python source.

**It is a `file` address like a script.** A path whose last segment has no
extension (or is a `.exe`) parses as `AddressType.EXECUTABLE` and is run
directly instead of through `uv`:

```
datasource landmarks (id: id, name: name, state: state)
grain (id)
file `./target/release/landmarks`;
```

It needs a path separator (`./landmarks`, not `landmarks`), which is what keeps
it apart from a backtick-quoted identifier. Declared without the extension, the
same model works on Windows, where the launcher resolves `landmarks.exe`.

## Several sources in one program

One program can serve several sources, so related sources share one
dependency set, one `uv` environment and, under VGI, one resident worker:

```python
@source(schema=PROCESS_SCHEMA)
def process() -> pa.Table: ...

@source(schema=HOST_SCHEMA)
def host() -> pa.Table: ...

if __name__ == "__main__":
    raise SystemExit(run({"process": process, "host": host}))
```

```rust
trilogy_io::sources()
    .add("process", trilogy_io::source(process).schema(process_schema()))
    .add("host", trilogy_io::source(host))
    .run()
```

A datasource picks one with a `#name` fragment on its address, which becomes
`--source name` on the program's command line:

```
datasource processes (pid: pid, name: name)
grain (pid)
file `./ingest/osquery.py#process`;
```

In python, a mapping takes each source's `schema` and `watermark` from its
`@source(...)`; passing `schema=` or `watermark=` to `run` alongside a mapping is
a `TypeError`. A source name is an identifier (`[A-Za-z_][A-Za-z0-9_]*`);
registering any other name fails at registration, before the program serves
anything.

## The contract

```
--limit N            --columns a,b,c        --filter 'col op value'  (repeatable)
--order-by k:desc,k2 --since VALUE          --partition k=v          (repeatable)
--format arrow|parquet|csv|json            --output URI
--describe           --source NAME
```

`--source` selects one source of a program that serves several:

- **Several sources.** `--source` is required. Without it, or naming a source
  the program does not serve, the program fails as a `ContractError` (exit 65)
  whose message lists the available sources, sorted:
  `unknown source 'x'. Available sources: host, process` or
  `this program serves several sources; choose one with --source. Available sources: host, process`.
- **One source.** `--source` is refused the same way:
  `unknown source 'x': this program serves a single source`.
- `--describe --source NAME` describes that source, and its datasource stub
  addresses it as `` file `<program>#NAME` ``.
- Under `--vgi` a program still serves one `source(args)` table function;
  `--source` travels inside `args`, so one resident worker serves every source.

Operators: `=` `!=` `<` `<=` `>` `>=` `in` `not in` `like`. Values parse as JSON
first, so `--filter 'state in ["CA","NY"]'` and `--filter 'id >= 90'` both work;
a bare word stays a string.

### Pushdown is an optimization, never a requirement

A field is satisfied one of two ways:

- **Pushed down** -- in python, by declaring a parameter named for it (`limit`,
  `columns`, `filters`, `since`, `partition`, or one annotated `SourceRequest`);
  in rust, through `.pushdown(&[...])`. The source owns it. This is what lets an
  API-backed source send the predicate to the API instead of fetching everything
  and discarding most of it.
- **Fallback** -- everything not claimed is enforced on the output stream by the
  wrapper.

**The contract is always honored; claiming a field only changes where.** A
script written by someone who never read this page still answers `--limit 10`
correctly.

`since` and `partition` are the exception: only the source knows which column
carries its watermark or how it is partitioned, so they have no fallback and are
ignored when not pushed down.

One rule is applied automatically. If filters *or an ordering* are being enforced
locally, `limit` is taken back from the source, because a source that truncates
first and gets filtered or sorted second returns the wrong rows. Correctness over
the wasted scan.

### Sideband metadata

Facts about the stream ride in the Arrow schema's key-value metadata, so they
survive the parquet staging path BigQuery uses and the consumer reads them off
`reader.schema` with no extra plumbing (`python_source.script_metadata`):

```
trilogy.contract    trilogy.pushdown    trilogy.watermark
```

Only what is known before the first batch belongs there -- the schema is written
first, so row counts cannot be.

### Errors

A failing source exits **65** and writes one machine-readable line to stderr
ahead of its traceback:

```
trilogy-io-error: {"type":"ValueError","message":"...","contract":1,"retryable":false}
```

The exit code separates "the source's own logic failed" from "uv could not
resolve dependencies", and `retryable` lets the source say whether repeating the
attempt is worth it -- both previously guessed at by pattern-matching stderr.

## Inspecting a source

```
trilogy source describe ./landmarks.py    # schema, pushdown, datasource block
trilogy source preview  ./landmarks.py --limit 5 --filter 'state = CA'
trilogy source check    ./landmarks.py    # implements the contract?
```

`describe` prints a ready-to-paste `datasource` block, so the schema does not
have to be transcribed by hand:

```
$ trilogy source describe ./landmarks.py
contract v1

columns:
  id     bigint
  name   string
  state  string

pushdown: limit

datasource landmarks(
    id: id,
    name: name,
    state: state
)
grain (id)
file `./landmarks.py`;
```

It reports what the source produces, not what a particular request would return,
so narrowing flags are ignored.

## The wire contract is the interface

The libraries are conveniences. The flag names, exit codes, `--describe` payload
and metadata keys are what Trilogy actually depends on, and the python and rust
implementations are held to identical answers by `tests/io/test_conformance.py`,
which runs the same flags through
[`tests/io/conformance/landmarks.py`](../tests/io/conformance/landmarks.py) and
the crate's `landmarks` example and compares rows, schema, metadata, describe
output, error payloads and the csv/json bytes. The one place they are allowed to
differ is which member of a tie group an incomplete ORDER BY returns -- pyarrow
sorts stably and arrow-rs does not, and SQL does not specify it either.

That is why the engine does not need to know what language a source is written
in, and why extending the contract is additive: a new field means a new flag
plus a fallback, after which existing sources keep working and silently gain it.

## Where things live

| | |
| --- | --- |
| adapters (anything → Arrow) | `trilogy/io/adapters.py` |
| the request, pushdown, fallbacks | `trilogy/io/contract.py` |
| output formats and URI sinks | `trilogy/io/sinks.py` |
| CLI, metadata, error reporting | `trilogy/io/runner.py` |
| `--describe` payload | `trilogy/io/describe.py` |
| click options, for authors who want them | `trilogy/io/click_support.py` |
| `trilogy source` | `trilogy/scripts/source.py` |
| consumer side (retry, metadata, errors) | `trilogy/dialect/python_source.py` |
| rust implementation | `crates/trilogy-io/` |

Engine-specific execution -- DuckDB's `uv_run` macro, BigQuery's GCS staging --
is documented in [bigquery_python_datasources.md](bigquery_python_datasources.md).

## Planner pushdown

A query's own `WHERE` reaches the script automatically on DuckDB
(`trilogy/dialect/source_pushdown.py`):

```
where state = 'CA' and id > 10
select id, state;
```

renders as

```sql
uv_run('/abs/path/src.py', args := '--filter ''state=CA'' --filter ''id>10''')
```

Each filter is single-quoted because the DuckDB transport concatenates `args`
into a shell command line, where the `<` and `>` operators would otherwise be
redirects; the doubling is that quote surviving the enclosing SQL literal.

**Pushdown is a hint, and that is the whole safety argument.** The rendered SQL
keeps its `WHERE` unchanged, so anything the script receives is redundant; the
only way pushdown could change an answer is if the script dropped a row SQL
would have kept. So a predicate is pushed only when it is:

- a conjunct of a top-level `AND` -- a disjunct is not implied by the whole
- `<column> <op> <literal>`, where the concept is a real column of *this*
  datasource (a derived concept is never pushed)
- one of `=` `!=` `<` `<=` `>` `>=`, whose pyarrow semantics match SQL's for
  non-null operands. `like` is excluded (escaping differs), `in` too (the list
  would have to survive shell splitting), and a NULL literal never qualifies
- **transport-safe**: `args` is embedded in a SQL string literal, concatenated
  into a shell command (`shellfs` pipe, or a `cmd.exe` call on Windows) and then
  `shlex.split`. Rather than escape correctly for three layers, only values
  needing no escaping at all are pushed -- so `state = 'new york'` is filtered
  by SQL alone

Anything not pushed is simply filtered by SQL as before. `tests/engine/scripts/test_source_pushdown.py`
runs each case with pushdown on and off and requires identical rows.

### The limit travels with its ordering

A limit is *not* redundant -- truncating the source changes which rows exist --
so unlike a filter it can only be pushed when everything that decides *which*
rows goes with it:

- the condition serialized **completely** (a leftover predicate applied after
  truncation returns fewer than N)
- the `order_by` serialized completely. `LIMIT` renders after `ORDER BY`, so
  rather than refuse the limit, the ordering is pushed *too* and the source
  returns the same top N:

  ```sql
  uv_run('/abs/src.py', args := '--filter ''state=CA'' --order-by id:asc --limit 3')
  ```
- no joins or parent CTEs (a join can drop rows, so SQL would have read past N)
- no grouping (N groups is not N input rows)

Only plain `asc`/`desc` travel; every nulls-first/last variant changes which rows
survive a limit and would have to be matched against pyarrow's null placement
rather than assumed.

**Ties are the one visible non-determinism.** `order by state limit 5` over
duplicate states has no single right answer, and the source's five need not be
the ones SQL would have picked. That is SQL's own non-determinism for an
incomplete ORDER BY, but it does mean pushdown can change *which* equally-valid
rows come back. Add a unique tiebreaker if that matters.

## DuckDB transports: shell (default) and VGI (opt-in)

By default DuckDB reads a source through the `uv_run` macro. On POSIX that is a
`shellfs` pipe; on Windows a wrapper stages the stream to a temp `.arrow` file
that `read_arrow` then scans. Either way the source is started on every scan.

`python_datasource_transport = "vgi"` reads it through
[VGI](https://query.farm/vgi/docs/python/) instead. VGI is a DuckDB extension
that runs table functions in pooled worker processes and speaks Arrow IPC over
their stdio:

```toml
[engine.config]
enable_python_datasources = true
python_datasource_transport = "vgi"
```

Each source is served one of two ways, chosen per source and remembered for the
executor's life:

- **Resident.** Before a query runs, `prepare_sources` starts the source itself
  as a pooled worker: `uv run --with vgi-python src.py --vgi`, or `landmarks --vgi`.
  It renders as `trilogy_src_<hash>.main.source('<args>')`. The worker stays up,
  so a warm query is a function call in an already-running process.
  - `--vgi` is part of both libraries: `run(fn)` in python and `Source::run` in
    rust (with the crate's `vgi` feature). It serves a `trilogy_source.main.source(args)`
    table function, where `args` is the same command line, parsed by the same
    parser, so the contract is unchanged; it is delivered per call instead of
    per process. `tests/io/test_vgi_conformance.py` holds both implementations'
    served answers to their one-shot output.
  - The alias hashes the source's path and mtime, so an edited script or a
    rebuilt binary gets a fresh worker rather than the stale pooled one.
  - Startup is lazy: a source is attached by the first query that reads it,
    never ahead of time. All of that query's new sources start in parallel, so
    it waits for the slowest startup rather than the sum.
  - Each attach runs on its own side cursor. A failure there cannot abort the
    transaction the query runs in, and the separate cursors are what let the
    attaches overlap.
  - Idle workers are kept for an hour (`POOL_TIMEOUT`, `RESIDENT_IDLE_SECONDS`).
    The vgi default is 5s, shorter than the gap between an agent's queries, so
    nearly every query would otherwise restart the source.
  - A query scanning one source twice (a self-join, a recursive CTE beside its
    seed) gets a second pooled worker. Bind answers from the source's declared
    `schema` when the request cannot reshape it, without running the source,
    so the two scans run at once rather than one after another inside binds.
    A source with no declared schema, or one that projects its own columns,
    still runs at bind.
  - `.github/workflows/vgi.yml` installs haybarn and runs these tests on
    Ubuntu and Windows. Elsewhere they skip.
- **Bridge.** A source that cannot serve itself falls back to the bridge: a raw
  script, a published pytrilogy without `--vgi`, or a binary built without the
  feature. The bridge is one worker (`trilogy/dialect/duckdb_vgi.py`), attached
  by the first query that reads such a source, so a model whose sources all run
  resident never starts it. It renders `trilogy_io.main.script('/abs/src.py', '<args>')`.
  It starts the source per scan like `uv_run`, but streams its stdout straight
  to DuckDB with no staging.

In both modes DuckDB's projection is applied in the worker, and a failure still
surfaces as `PythonDatasourceError`. The worker reports it as a
`trilogy-script-failure {json}` payload carrying the exit code and stderr a
one-shot run would have produced.

Requirements and caveats:

- **It needs `haybarn`**, Query.Farm's DuckDB build. The `vgi` extension is not
  in DuckDB's public community repository, so stock `duckdb` cannot `LOAD` it.
  The transport switches the SQLAlchemy URL to `haybarn://` (`haybarn_engine.py`,
  a thin subclass of duckdb_engine's dialect). Install with `pytrilogy[vgi]`,
  which needs python >= 3.13 for `vgi-python`.
- **License:** `vgi`, `vgi-python` and the `vgi` crate are under the Query Farm
  Source-Available License, not OSI open source. That is why they are an
  optional extra and an opt-in cargo feature, never a default.
- **A resident source owns the worker's stdout.** The python side redirects
  `print` to stderr while the source runs. Anything writing to fd 1 directly
  (C code) would corrupt the stream.
- Pushdown args are unchanged, so the same transport-safety limits apply, even
  though VGI has no shell layer (see [Planner pushdown](#planner-pushdown)).

Benchmarks live in `local_scripts/vgi_benchmark/`. Windows warm medians, full
3-column scan:

| source | rows | current | vgi bridge | vgi resident |
| --- | --- | --- | --- | --- |
| python | 1M | 0.38s | 0.30s | 0.04s |
| python | 10M | 1.89s | 0.68s | 0.24s |
| rust | 1M | 0.23s | 0.05s | 0.04s |
| rust | 10M | 0.79s | 0.30s | 0.27s |

The current transport also stages ~290MB to disk per 10M-row scan on Windows.

## Not done yet

**VGI filter pushdown.** Resident workers take the planner's predicates through
`args`, exactly as the other transports do. VGI can also hand a worker DuckDB's
own filters (`filter_pushdown`), with no shell-safety limits; mapping those onto
`SourceRequest` would let more predicates reach the source.
