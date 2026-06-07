# Bug: 3-arm multi-select with a derive output reusing an arm name → INVALID_REFERENCE_BUG

**Status:** FIXED 2026-06-07 (clean-error route). Found 2026-06-07, enriched eval q78. A surviving
sibling of the (FIXED) `bug_multiselect_duplicate_arm_alias_codegen.md`.

## Fix

Root cause was NOT the derive output reusing an arm name (that compiles fine — verified) but the
**`align` group name reusing an arm output name** (`align yr: yr, yr2, yr3` — the merged group `yr`
collides with arm-1's hidden `yr`). The align group is a new merged concept; keyed by address it
collapses onto arm-1's `yr`, so codegen can't tell which arm CTE the dimension came from and emits
`year(INVALID_REFERENCE_BUG) as "yr"`. `multi_select_statement`
(`trilogy/parsing/v2/rules/multiselect_rules.py`) now scans `align_c.items` after the duplicate-arm
guard and raises `InvalidSyntaxException` when an align group's `aligned_concept` address is already
an arm output: "Multi-select align group '<name>' reuses an arm output name; give the align group
its own name … (e.g. `align <name>_grp: …`)." Clean-error route, same family/shape as the
duplicate-arm fix. Test:
`tests/test_parse_engine_v2.py::test_parse_text_v2_multiselect_align_reuses_arm_name_raise`.
Also added a GOTCHA line to the `aligned-multi-select` agent-info example.

---

**Original report (OPEN):**
**Severity:** medium-high — `generate_sql` itself raises (not a DuckDB execute error), so the query is
unrunnable. Contributed to q78's residual fail after the GROUP-BY fix dropped its tokens 3.79M→1.83M.

## Symptom

```
ValueError: Invalid reference string found in query:
WITH … vacuous as ( SELECT INVALID_REFERENCE_BUG as "itm", … ) …
… this should never occur. Please create an issue to report this.
```
The hidden align dimension (`itm` = `item.text_id`) is emitted as `INVALID_REFERENCE_BUG` in one of
the arm CTEs.

## Trigger shape

A cross-model 3-arm multi-select that combines:
- **distinct output names per arm** (`store_qty` / `web_qty` / `cat_qty`, etc.) — so the same-name-arm
  guard added in `bug_multiselect_duplicate_arm_alias_codegen.md` does NOT fire, and
- a **`derive` output that reuses an arm column name** —
  `coalesce(store_qty, 0) as store_qty` (the derived `store_qty` collides with arm-1's `store_qty`),
  alongside cross-arm combiners (`coalesce(web_qty,0) + coalesce(cat_qty,0) as other_qty`), and
- **hidden (`--`) align dimensions** (`--ps.item.text_id as itm`, `--ps.customer.id as cust`) used as
  align keys but kept out of the projection.

The `INVALID_REFERENCE_BUG` lands on the hidden align dimension (`itm`). Same family as the
derive-output-reuses-arm-name collision noted in
`bug_query_scoped_join_conflicting_filter.md`, but here it surfaces as an unresolved CTE reference in
a 3-arm align rather than an invalid GROUP BY.

## Deterministic reproduction

Run the q78 body the agent wrote against the workspace model. Extract from
`evals/tpcds_agent/results/20260607-160439/agent_log.q78.jsonl` (the file-write whose `run` returned
`INVALID_REFERENCE`; note the content flag is `-c`, not `--content`) and:
```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
WS = Path("evals/tpcds_agent/results/20260607-160439/workspace")
eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=WS))
eng.generate_sql(BODY)[-1]   # raises: Invalid reference string … INVALID_REFERENCE_BUG …
```
Skeleton of `BODY` (3 channel arms over `physical_sales`/`web_sales`/`catalog_sales`, customer-merged):
```trilogy
import raw.physical_sales as ps; import raw.web_sales as ws; import raw.catalog_sales as cs;
merge ps.customer.id into ~ws.billing_customer.id;
merge ps.customer.id into ~cs.bill_customer.id;
select --ps.item.text_id as itm, --ps.customer.id as cust, --sum(ps.quantity) as store_qty, …
merge select --ws.item.text_id as itm2, … --sum(ws.quantity) as web_qty, …
merge select --cs.item.text_id as itm3, … --sum(cs.quantity) as cat_qty, …
align …
derive
  coalesce(store_qty, 0) as store_qty,                 -- derive output reuses arm-1 name
  coalesce(web_qty, 0) + coalesce(cat_qty, 0) as other_qty, …
having other_qty > 0
order by … limit 100;
```

**Minimization status:** not isolated (multi-select minimal repros have been elusive across this
family). The first task for whoever picks this up is to reduce it — strong candidates for the
load-bearing pieces: the derive output reusing an arm name, the 3rd arm, and the hidden align dim.

## Suggested fix

Same root family as the duplicate-arm-alias fix: arm/derive output addresses collapse by address, so
codegen can't resolve which CTE a (hidden) align dimension came from and substitutes
`INVALID_REFERENCE_BUG`. Either disambiguate the derive-output-vs-arm-output address collision
(rename the derived concept internally), or reject it at parse with the same actionable message used
for duplicate arm outputs ("a derive output may not reuse an arm column name; alias it distinctly").
Never emit `INVALID_REFERENCE_BUG`.
