# Datasource write order (`order by`) and storage options (`storage`)

A file target that trilogy writes can declare the order its rows are written
in, and options for the format it is written as. Together they let a reader
that filters on a sort-correlated column skip most of a parquet file: DuckDB
(native and WASM) prunes row groups by the min/max statistics in the footer,
which only helps when each row group covers a narrow range.

## Syntax

Both clauses go after `partition by` and before any `assert`:

```trilogy
import std.geography;

template datasource city_tree_browser (
    tree_id,
    latitude: ?latitude,
    longitude: ?longitude,
)
grain (tree_id)
order by @hilbert_index(latitude, longitude)
storage (row_group_size = 10240);

datasource usbos_tree_browser extends city_tree_browser ()
file `usbos_tree_browser.parquet`;
```

- **`order by <expr> [asc|desc] [nulls first|last], ...`** uses the select
  `order by` grammar. A term may be any row-level expression over the
  datasource's columns. It does not need to be a stored column:
  `@hilbert_index(...)` above is computed for the sort and is not written. A
  term that reads something the datasource neither stores nor derives row by
  row is a parse error. Only `parquet` and `csv` file targets accept it.
- **`storage (key = value, ...)`** holds options for the written format. Each
  format accepts its own keys (`STORAGE_OPTIONS` in
  `trilogy/core/models/datasource.py`). A key the target's format does not
  accept, or a value of the wrong type, is a parse error, never silently
  dropped. For now there is one: parquet `row_group_size` (int), passed to
  DuckDB's `COPY` as `ROW_GROUP_SIZE`. DuckDB fills row groups in whole
  2048-row vectors, so a size that is not a multiple of 2048 is rounded up.

Both are inherited through `extends` like `grain`: a child that declares its
own replaces the inherited one as a whole.

## Semantics

Both clauses only affect the write. They do not change what the datasource
answers or how the planner picks sources. They do count toward the
datasource's fingerprint, so adding or changing either one rebuilds the asset
once.

Every file write path keeps the order:

- **A build** wraps the select it copies in an outer
  `SELECT ... ORDER BY <keys>`. The keys are planned as a trilogy select of the
  datasource pinned to the build's rows (`trilogy/execution/write_order.py`),
  so an order term derived from the columns is computed the same way it would
  be in a query.
- **An append** to a file rewrites the whole file from the old rows plus the
  new ones. The sort applies to that union, not just to the new rows.
- **The local stage** is used for audited writes to targets that cannot be
  staged in place. Its write-through re-reads the audited file in file order:
  by `file_row_number` for parquet, and by DuckDB's insertion order for csv.
  It refuses to run when `preserve_insertion_order` is off.

## `@hilbert_index(lat, lon)` (`std.geography`)

The position of a point on a Hilbert curve over the whole globe (longitude
-180..180, latitude -90..90), at 16 bits per axis, as a `bigint`. Nearby
points get nearby keys, and no bounds argument is needed: whole-world bounds
prune as well as city-fitted ones at city scale. It is NULL when either
coordinate is.

It is an ordinary `def`, not a builtin, written with two generic pieces:

- **`bit_and`, `bit_or`, `bit_xor`, `bit_shift_left`, `bit_shift_right`**:
  integer bitwise functions, rendered as each dialect's operators or functions.
- **`array_reduce(array, @step, initial)`**: a left fold. `@step` is a
  two-argument `def` taking the state and an element; the state has the type
  of `initial`. DuckDB renders `list_reduce`, Snowflake `REDUCE`, Presto/Trino
  `reduce` and ClickHouse `arrayFold`; dialects with no fold (BigQuery,
  Postgres, ...) refuse it.

The walk visits the 16 levels of the curve most significant first. Its state
packs the index so far with the rotated x and y. It runs as two folds of 8
levels each so the packed state stays under 2^48 and fits a signed `bigint`.
