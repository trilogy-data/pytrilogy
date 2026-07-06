# Handoff — `union(...)` silently downgrades a DECIMAL column when one arm's placeholder is `float`

**Origin:** q05 float32-drift token sink (`bug_q05_current_engine_float32_union_placeholder.md`).
The immediate cause there was agent-authored (`cast(0 as float)` money placeholder), now partly
mitigated by adding `double`/`decimal` cast targets. But there is a **framework codegen gap**
underneath that this handoff is about.

## Where the placeholder comes from
It is **agent-authored**, not framework-injected. The agent builds a sales+returns rowset with
`union(...)` (or `union all` arms). The sales arm has no return columns, so the agent pads them:
`select ..., cast(0 as float) as return_amount, ...`, while the returns arm supplies the real
`return_amount` (declared `numeric::usd?`, i.e. DECIMAL(15,2)). So across arms the same output
column is DECIMAL in one arm and `float` in the other.

## The framework gap (the actual bug to fix)
Trilogy's **type derivation is correct** — it is NOT the bug:
```
merge_datatypes([Numeric(7,2), FLOAT]) == Numeric(7,2)   # both arg orders
merge_datatypes([FLOAT, Numeric(7,2)]) == Numeric(7,2)
```
(`trilogy/core/models/core.py` `merge_datatypes`: the `NumericType`-instance branch wins over
FLOAT/DOUBLE/INT regardless of order.) So the union column's declared type is `Numeric`.

The downgrade happens at **SQL rendering**: the generated union emits each arm's raw expression
verbatim —
```sql
SELECT ..., return_amount, ...        FROM returns_arm
UNION ALL
SELECT ..., CAST(0 AS FLOAT), ...     FROM sales_arm     -- <-- not coerced to the unified type
```
DuckDB then resolves the physical `UNION` column type across arms and picks **REAL/FLOAT** (the
narrower single-precision arm), so the DECIMAL measure accumulates in float32 and drifts (q05:
`112458734.685` vs `112458734.70`). Trilogy never injects a cast to coerce each arm's column to
the union's derived output type, so the arm types disagree at the physical layer.

## Two fix directions (owner preference: enforce strict compatibility)
1. **Strict (owner-leaning):** at union construction, require the per-column arm types to be
   *compatible AND non-narrowing* — reject a union whose arms bind the same output column to a
   narrower type than a sibling (DECIMAL vs FLOAT), with an error naming the column + arms and
   suggesting the author align the placeholder type (`cast(0 as numeric)`/plain `0`). This makes
   the q05 footgun a loud parse/build error instead of silent drift.
2. **Coercing (agent-friendly):** render every union arm's column with an explicit
   `CAST(<expr> AS <unified_output_type>)` using the already-correct `merge_datatypes` result, so
   all arms agree and DuckDB cannot pick the narrowest. This auto-fixes mismatched placeholders.

A hybrid is reasonable: coerce when the mismatch is a safe widening (float→numeric/double), error
only when arms are genuinely incompatible (e.g. string vs numeric).

## Entry points
- Union output type: `trilogy/core/functions.py:1388-1391` (`merge_datatypes` over arms) and the
  `FunctionType.UNION` config at `functions.py:640`.
- Union arm SQL rendering: find where union/`union all` arms are emitted in `trilogy/dialect/base.py`
  (search the CTE/union render path) — this is where a per-arm coercion cast would be injected, or
  where a strict pre-check would compare arm column types.
- Type helpers: `merge_datatypes` / `is_compatible_datatype` in `trilogy/core/models/core.py`
  (both already treat the full int/float/double/numeric family as compatible).

## Repro seed
Build a 2-arm `union(...)` rowset where arm A supplies a `numeric(7,2)` measure and arm B pads the
same output column with `cast(0 as float)`; sum the measure and compare to the same query with the
placeholder typed `numeric`/`double`. The float arm drifts; the numeric/double arm does not.
Score against `evals/tpcds_agent/results/20260706-001731/workspace` (q05 workspace).

## Related
- `bug_q05_current_engine_float32_union_placeholder.md` (symptom + A/B proof).
- `double`/`decimal` cast targets + a real `DataType.DOUBLE` were added
  (`core.py`/`base.py`/dialects/grammars) so agents now have a non-narrowing placeholder option;
  this handoff is the remaining, deeper codegen fix.
