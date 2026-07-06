# q05 CURRENT-ENGINE verdict — run `20260706-001731` (710,847 tok, FAILED)

## VERDICT: **(c) agent semantic/authoring error** — float32 precision drift from
`cast(0 as float)` union-arm placeholders. **Not a framework bug** (no wrong-row
logic in the planner); supported by a real **(b) type-system / guidance gap** (Trilogy
has no 8-byte double, so the natural escape hatches also fail).

Same failure family as the prior recheck (`bug_q05_recheck_20260705.md`): the agent again
stacked sales+returns with `union(...)` and used **`cast(0 as float)`** zero placeholders
in the opposite arm. The union unifies the sibling `DECIMAL(7,2)` measure **down to
single-precision `REAL`**, so the final `sum(...)` accumulates in float32 and every row
drifts past the 9-decimal scoring tolerance. This run only renamed the cast form
(`0::float` → `cast(0 as float)`); the mechanism is identical.

## Confirmation on the run's own workspace (READ-ONLY harness)
```
REF grand: (None, None, Decimal('112458734.70'), Decimal('3255243.12'), Decimal('-31584085.44'))
SUB grand: (None, None, 112458734.68504244,      3255243.116688758,     -31584085.444747422)
```
100/100 rows mismatch; every measure column (2,3,4) drifts. Unlike the prior run, the
profit column ALSO drifts here because BOTH `prof_amt` and `loss_amt` arms carry a
`cast(0 as float)` placeholder (lines 13/14, 24/25 of `workspace/query05.preql`), so all
three measures are coerced to `REAL`.

## Single-toggle A/B proof (change ONLY the cast target, nothing else)
| variant | `_multiset(candidate) == _multiset(ref)` |
|---|:---:|
| submitted `cast(0 as float)` | **False (fails)** |
| `cast(0 as float)` → `cast(0 as numeric)` | **True (matches exactly)** |
| `cast(0 as float)` → plain `0` (int literal) | **True (matches)** |
| `cast(0 as float)` → `0.0` (float literal) | False — float literal is ALSO single-precision |
| `cast(0 as float)` → `cast(0 as double)` | `HydrationError` — `double` is not a Trilogy cast target |
| `cast(0 as float)` → `cast(0 as decimal(7,2))` | `InvalidSyntaxException` — parametrized decimal cast unsupported |

Only integer `0` or `::numeric` avoid the drift. The two most natural fixes an author
reaches for (`0.0`, `::double`) both fail — that is the guidance/type gap.

## Trigger matrix (what is / isn't the driver)
| toggle | drives failure? |
|---|:---:|
| `by rollup` on/off | NO — ROLLUP is correctly emitted (fresh `sum(...)` idiom); structure/order/labels/entity-ids all match ref |
| concatenated entity id on/off | NO — `concat('store'/'catalog_page'/'web_site', eid)` values are exact string matches |
| cross-channel `union(...)` on/off | INDIRECT — union is only the vehicle that forces a common column type between a DECIMAL arm and a float placeholder |
| returns arm on/off | NO — both arms are correct; the returns-loss subtraction (`sum(prof)-sum(loss)`) is arithmetically right |
| **`cast(0 as float)` placeholder type** | **YES — sole driver.** Single-precision `REAL` coercion of the DECIMAL union column |

## Minimal repro (2-column union, no rollup / no entity-id / no returns logic)
```
import raw.all_sales as s;
with u as union(
  (where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_text_id is not null
   select s.channel as ch, s.ext_sales_price as v, cast(0 as float) as w),
  (where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_text_id is not null
   select s.channel as ch, cast(0 as float) as v, s.return_amount as w)
) -> (ch, v, w);
select coalesce(sum(u.v),0) as tv, coalesce(sum(u.w),0) as tw;
```
`float` result:   `(109196642.15480667, 3249071.5067131817)`  ← REAL drift
`numeric` result: `(Decimal('109196642.170'), Decimal('3249071.510'))`  ← exact
Generated SQL renders the placeholder literally as `cast(0 as float) as "..."` (single
precision), confirming no widening to DOUBLE.

## Root cause (file:line) — a type-system limitation, not a wrong-row bug
- **Authored defect:** `workspace/query05.preql` lines 12, 14, 22, 24 — `cast(0 as float)`
  placeholders inject single-precision `REAL` into DECIMAL union columns.
- **Why float is single precision:** `trilogy/dialect/base.py:343` renders
  `DataType.FLOAT` → SQL `float` (DuckDB `FLOAT`/`REAL` = 4-byte single precision), and
  `base.py:377-381` folds `double` / `double precision` / `float8` / `real` all into the
  SAME `DataType.FLOAT`. **Trilogy has no distinct 8-byte double type**, so (a) every
  float-typed literal or cast is single precision and (b) `cast(x as double)` has no
  target and raises `HydrationError`. There is no ergonomic way to get an exact/8-byte
  zero placeholder except an untyped integer `0` or `::numeric`.
- **Reference is correct:** `tests/modeling/tpc_ds_duckdb/query05.sql` (raw SQL) returns
  the 100 canonical rows and is the oracle used above. The canonical `query05.preql`
  avoids the whole problem by using ONE model with `coalesce(sale_dim, return_dim)` +
  per-measure gated `sum(metric ? window and dim is not null)` — no union, no float
  placeholder. (It imports `all_sales` relative to the tests dir, so it can't be run
  from the run workspace, but it is the shipped reference and is byte-correct.)

## Why it burned 710k tokens (guidance gap, unchanged from prior recheck)
The query runs clean, returns 100 rows, correct labels/order/rollup/entity-ids — every
artifact the agent can inspect is right. The only scoring signal is "result set differs"
with no numeric/type diff, and the divergence is a float32 error in the ~8th significant
digit, invisible to eyeballing. The agent cannot see that a column is `REAL` vs `DECIMAL`,
so it churns on structure. Same undiagnosable-silent-drift token-sink as the prior two
runs; the specific defect keeps moving (rollup-drop → order/limit → float32) but the
mechanism (no cell-level diff feedback) is constant.

## Recommendations (do NOT implement here)
- **Guidance (highest leverage):** tell the agent that for money/quantity measures in
  `union(...)` arm placeholders, use an untyped integer `0` or `::numeric` — NEVER
  `float`/`0.0`/`::double`. `float` is single-precision `REAL` and drifts past the
  9-decimal scoring tolerance; `::double` is not a valid cast target.
- **Harness diagnostic:** on a same-shape/same-cardinality failure, surface the first
  mismatched cell (value + duckdb column type) so precision/type errors self-diagnose.
- **Framework (separate, larger):** consider a distinct DOUBLE/8-byte float DataType (or
  render `DataType.FLOAT`→`double`) so float-typed literals/casts don't silently truncate
  DECIMAL precision. `base.py:343,377-381`.
