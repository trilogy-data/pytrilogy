# Audit: `trilogy unit` mock interface datatype coverage

**STATUS: FIXED 2026-08-13.** Everything below landed in
`trilogy/dialect/mock.py` except GEOGRAPHY and regex-validated strings, which
stay unsupported by design but now fail with an error naming the offending
concept. Extras beyond this audit, found once mocking got far enough to run
full validation:

- Columns feeding a datasource-declared cast (`_date_string::date: date`)
  now mock as values that survive the cast (`MockManager.cast_targets`);
  random strings in a date-cast column were aborting the whole validation
  transaction.
- `map<k,v>` columns register as true DuckDB MAP columns via an explicit
  arrow type; dict rows otherwise infer as STRUCT.
- Bare `list` spelling parsed to a crash (`'list' is not a valid DataType`);
  added to `_DATATYPE_SPELLING_ALIASES`.
- Two stale hidden-and-unbound declarations (`date.preql` `--d_week_seq1`,
  `web_sales.preql` `--key return_order_number`) removed from the corpus
  models; validation correctly flags hidden concepts bound to no datasource
  (the corpus idiom is hidden-but-bound).

`trilogy unit` now passes on store_sales, catalog_sales, and web_sales.
Regression coverage in `tests/test_mocking.py`. Original audit below.

**Audited 2026-08-13 against HEAD.** Companion to
`bug_q89_numeric_type_unit_mocking.md`, which reported one instance
(`NumericType(15,2)`). This audit swept every type declarable in the grammar
(`trilogy/parsing/trilogy.lark:798-800`) through `mock_datatype`
(`trilogy/dialect/mock.py:108-194`) both directly and end-to-end via
`mock datasource` on DuckDB. The q89 gap is one of ten; two additional defects
produce *silently wrong* mock data rather than a crash.

Amplifier (from the q89 report, applies to every row below): `trilogy unit`
mocks every concrete column of every imported datasource, so one unsupported
column type aborts unit validation for all queries against that model.

## Crash gaps (unhandled `NotImplementedError`)

| Declarable type | Direct + e2e result | Exposure |
|---|---|---|
| `numeric(p,s)` (`NumericType`) | crash | **HIGH** - all TPC-DS sales models (`store_sales.preql` etc.); unit mode is dead on the eval corpus. The q89 bug. |
| `numeric(p,s)::trait` | crash (trait unwrap re-dispatches the inner `NumericType`) | HIGH - same models (`::usd`) |
| `array<numeric(p,s)>` | crash (array branch re-dispatches element type) | follows from the above |
| `bigint` | crash | MEDIUM - common warehouse type; no current corpus model uses it, but `bigint[1..10]` (validated) *works*, bare `bigint` doesn't |
| `number` | crash | LOW - same inconsistency: supported under `ValidatedType`, not bare |
| `map<k,v>` / bare `map` | crash | LOW-MED - iris only binds its map column via `raw()`, which the mocker skips; direct binding is grammar-legal and fails |
| `struct<...>` / bare `struct` | crash | LOW-MED - same |
| `bytes` | crash | LOW |
| `geography` | crash | LOW - `std.geography` uses traits over string/float (those unwrap fine); the bare keyword is grammar-legal |
| `string['regex']` (`ValidatedType.pattern`) | crash - explicit raise at `mock.py:53-55` | LOW; at least deliberate/loud |

Not reachable as column types (parse/function-internal only), no action needed:
`DATE_PART`, `NULL`, `UNKNOWN`, `ANY`, `UNIX_SECONDS` (only appears as a trait
base, which unwraps).

## Silently-wrong-data defects (no crash - worse)

1. **`ArrayType` non-key rows are double-nested** (`mock.py:190-193`). Each
   row's value is `[mock_datatype(...)]` - a list *containing* the 5-element
   list - so an `array<int>` column is registered in DuckDB as
   `INT[][]` with rows like `[[670487, 116739, ...]]`. Mock data contradicts
   the declared type; any unit query using array functions on it misbehaves.
2. **`ArrayType` key rows mix element types** (`mock.py:186-189`): uniqueness
   is manufactured by appending the row index, producing `[first_elem, i]`.
   For `array<string>` this is `['mock_string_x', 0]` and pyarrow raises
   `ArrowTypeError: Expected bytes, got a 'int' object`; for `array<int>` it
   silently yields 2-element arrays unrelated to the declared shape.

Minor: the `DataType.NUMERIC` key path returns floats (`mock.py:150-151`),
so exact-numeric key columns are backed by binary floats; the bug report's
`Decimal` recommendation applies here too.

## Root cause

The dispatcher compares `datatype` to `DataType` enum members only. Every
concrete type (`NumericType`, `MapType`, `StructType`, `ArrayType`, ...)
already exposes a `.data_type` property returning its base enum
(`trilogy/core/models/core.py`), but there is no normalization step, so each
concrete class needs - and mostly lacks - its own `isinstance` branch.
`mock_validated` independently grew support for `BIGINT`/`NUMBER`/date bases,
which is why validated variants work while the bare types crash.

## Fix direction

1. Normalize at the top of the enum dispatch: for non-parameterized concrete
   types fall back to `datatype.data_type`; keep dedicated branches where the
   parameters matter.
2. `NumericType`: generate `Decimal` values honoring precision/scale (per the
   q89 report).
3. `BIGINT`/`NUMBER`: reuse the INTEGER/NUMERIC generators.
4. `MapType`/`StructType`: compose from element-type generators, mirroring the
   array branch.
5. Fix the array branch: non-key rows must be the element list itself (not
   wrapped); key uniqueness should vary a same-typed element, not append an
   index.
6. `BYTES`: trivial generator; `GEOGRAPHY` and regex-validated strings can
   stay unimplemented but should fail with a clear "unsupported in unit mode"
   message naming the column, not a bare `NotImplementedError` from a leaf.

## Ready-made repro: kitchen-sink datasource

Verified 2026-08-13: each column below individually crashes `mock datasource`
with the noted error (except `tags`, which silently double-nests). One model
exercises every gap; drop the crash columns as they get fixed and this becomes
the regression fixture.

```preql
type usd numeric;

key id int;
property id.price numeric(15,2);              # crash: NumericType
property id.cost numeric(15,2)::usd;          # crash: trait unwrap -> NumericType
property id.prices array<numeric(15,2)>;      # crash: array element NumericType
property id.big bigint;                       # crash: BIGINT
property id.num number;                       # crash: NUMBER
property id.tags_by_ct map<string, int>;      # crash: MapType
property id.nested struct<a: int, b: string>; # crash: StructType
property id.blob bytes;                       # crash: BYTES
property id.geo geography;                    # crash: GEOGRAPHY
property id.code string['[A-Z]{2}'];          # crash: regex ValidatedType
property id.tags array<string>;               # SILENT: rows double-nested

datasource spicy (
    id: id,
    price: price, cost: cost, prices: prices, big: big, num: num,
    tags_by_ct: tags_by_ct, nested: nested, blob: blob, geo: geo,
    code: code, tags: tags,
)
grain (id)
address spicy_tbl;

mock datasource spicy;
```

## Regression coverage to add

Existing `tests/test_mocking.py` covers enums and validated ranges only. Add
the q89 report's six cases, plus: bare `bigint`/`number` columns; a directly
bound (non-`raw()`) `map`/`struct`/`array` column each; array mock shape
equals declared element type (would have caught both silent defects); and a
sweep asserting every type in `data_type` (`trilogy.lark:800`) either mocks or
raises the clear per-column unsupported error.
