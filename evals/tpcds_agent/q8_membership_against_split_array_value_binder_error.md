# Bug: membership `<scalar> in split(x, ',')` (RHS is an array-valued expression) renders as SQL `IN (subquery)` → DuckDB BinderException "Cannot compare VARCHAR and VARCHAR[]" (q8)

**Status:** FIXED 2026-06-27 — `render_expr` BuildConcept-subselect branch now detects an
`ArrayType`-valued RHS concept and wraps the subselect column in `unnest(...)` so `<scalar> IN
(select unnest(arr_col) ...)` yields one scalar per element. Both the normal and inlined-parent
subselect paths handled. Test: `tests/engine/test_duckdb_filter.py::test_membership_against_array_valued_split`.
Was: OPEN — deterministic minimal repro. Surfaced in the full-99 rebaseline.
**Surfaced by:** TPC-DS q8 (run `20260627-181845`). `generate_sql` succeeds; **execute** throws an
uncaught `(_duckdb.BinderException)` → `Unexpected error`, no clean Trilogy message.
**Severity:** HIGH — invalid SQL DuckDB rejects.

## Symptom

```
(_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and VARCHAR[]
in IN/ANY/ALL clause - an explicit cast is required
LINE 29: ... "store_sales_customer_address_zip" ELSE NULL END in (select quizzical."_virt_func_split_..."
```

`split(zips, ',')` is an **array** (`VARCHAR[]`). Trilogy lowers `<scalar> in <array-concept>` to
`<scalar> IN (SELECT <array_col> ...)`, comparing a `VARCHAR` against a `VARCHAR[]` column → DuckDB
rejects it. The `in` membership operator assumes the RHS concept holds **scalar** values, one per
row; here each value is itself an array.

## Trigger (bisected to a one-liner)

```trilogy
import raw.store_sales as ss;
parameter zips string;
auto qual <- ss.customer.address.zip ? ss.customer.address.zip in split(zips, ',');
where ss.date.year = 1998 and ss.store.zip in qual
select ss.store.name as n, sum(ss.net_profit) as p limit 10;
```

The load-bearing piece is `<scalar> in split(...)` — membership whose RHS is an **array-typed
expression** rather than a set/column of scalars.

## Likely fix area

When the membership RHS resolves to an `array`/`list` datatype, render it with array membership
(`<scalar> = ANY(<array>)` / `list_contains(<array>, <scalar>)` / `unnest`) instead of `IN
(subquery)`. The membership lowering currently does not special-case an array-valued RHS. At minimum,
raise a clean author-time error pointing at the `split(...)`/array RHS and suggesting `unnest` or a
scalar set, instead of emitting invalid SQL.

(The original q8 also nests this inside `substring(store.zip,1,2) in substring(qualifying_zip,1,2)` —
same root: the qualifying-zip set is array-shaped where the planner expects scalars.)

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-181845/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.environment.set_parameters(zips='24128,76232,65084')
q = '''import raw.store_sales as ss;
parameter zips string;
auto qual <- ss.customer.address.zip ? ss.customer.address.zip in split(zips, ',');
where ss.date.year = 1998 and ss.store.zip in qual
select ss.store.name as n, sum(ss.net_profit) as p limit 10;'''
sql = eng.generate_sql(q)[-1]   # succeeds
eng.execute_raw_sql(sql)        # BinderException: Cannot compare VARCHAR and VARCHAR[]
```
