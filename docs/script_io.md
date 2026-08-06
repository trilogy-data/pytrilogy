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

The binary implements the same contract, so `trilogy source describe|preview|check`
work against it and any consumer that invokes it directly gets the same answers
as from a python source.

**It cannot be a `file` address yet.** The parser maps a datasource address to a
type by file extension (`.py` → python script), so an extensionless binary is
rejected at parse time. Pointing a datasource at a compiled source needs a
runner abstraction on the engine side — see [Not done yet](#not-done-yet).

## The contract

```
--limit N            --columns a,b,c        --filter 'col op value'  (repeatable)
--order-by k:desc,k2 --since VALUE          --partition k=v          (repeatable)
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
uv_run('/abs/path/src.py', args := '--filter state=CA --filter id>10')
```

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
  uv_run('/abs/src.py', args := '--filter state=CA --order-by id:asc --limit 3')
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

## Not done yet

**A compiled binary cannot be a datasource address.** `_FILE_TYPE_MAP` in
`trilogy/parsing/v2/rules/datasource_rules.py` keys `AddressType` off the file
extension, so only `.py` reaches `AddressType.PYTHON_SCRIPT` and an
extensionless binary fails at parse. Making `trilogy-io` binaries first-class
means generalizing `build_uv_command` into a runner chosen per address (`.py` →
`uv run`, executable → exec directly) and giving the grammar a way to say "this
address is a program". Until then the crate is usable through `trilogy source`
and by downstream consumers, but not from a `file` clause.
