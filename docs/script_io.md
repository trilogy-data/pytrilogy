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

Point the datasource at the compiled binary. Trilogy dispatches on the address:
`.py` runs under `uv run`, anything else is executed directly.

## The contract

```
--limit N            --columns a,b,c        --filter 'col op value'  (repeatable)
--since VALUE        --partition k=v        (repeatable)
--format arrow|parquet|csv|json            --output URI
--describe
```

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

One rule is applied automatically. If filters are being enforced locally,
`limit` is taken back from the source, because a source that truncates first and
gets filtered second returns fewer rows than were asked for. Correctness over
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
implementations are held byte-identical by `tests/io/test_conformance.py`, which
runs the same flags through
[`tests/io/conformance/landmarks.py`](../tests/io/conformance/landmarks.py) and
the crate's `landmarks` example and compares rows, schema, metadata, describe
output, error payloads and the csv/json bytes.

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

## Not done yet

The planner does not yet compute a `SourceRequest` from a query, so `--limit`
and `--filter` reach a source only when a human passes them (`trilogy source
preview`) or when a datasource declaration carries them. The scan side of that
-- flags, fallbacks, both implementations, `--describe` for capability
discovery -- is in place; what is missing is the grammar for static arguments on
a `file` address and the planner change that pushes a query's own limit and
predicates into a script scan.
