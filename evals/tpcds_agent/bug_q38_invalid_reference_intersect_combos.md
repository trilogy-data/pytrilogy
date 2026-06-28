# Bug: q38 INTERSECT-of-combos rowset emits `INVALID_REFERENCE_BUG` column

## Status
FIXED 2026-06-28 — option (b): a scoped INNER join between two ROWSET operands
inside a rowset body now raises a clean `UnresolvableQueryException` (pointing at
the `union(...)` + `count_distinct(channel)` rewrite) instead of emitting a
sentinel. Option (a) (redirect the dropped side's output onto the surviving
pseudonym) was rejected: it renders but produces *silently wrong* results — the
collapse drops one side's scan entirely, so the intersection is lost (the
"CLEAN" join-only variant in the matrix below already returned the store-only
count, not the intersection). The no-WHERE INNER variant now also raises (was
silently wrong), since it loses the intersection the same way.

Fix: `_validate_cross_rowset_inner_joins` in `rowset_node.py` — after the body
`base_node` is built, for each `select.scoped_joins` entry that is INNER and
whose *both* operands are `Derivation.ROWSET`, raise if either operand is absent
from the body's real (non-pseudonym) materialized addresses. LEFT/FULL operands
(genuinely optional) and fact/dimension key collapses (same entity) are exempt.
The shared-base cross-rowset-JOIN cases that materialize both sides
(`test_cross_rowset_join_rowset_as_set.py` `qual`, the y01/y02 yoy shapes) are
unaffected. Test: `tests/test_cross_rowset_inner_join_intersect.py`.

## Symptom
TPC-DS **q38** (count of distinct `(last_name, first_name, d_date)` tuples present in
store AND catalog AND web sales in year 2000). The eval agent modeled the three-way
INTERSECT as per-channel "combos" rowsets and then a `store_and_catalog` rowset that
**inner-joins** two combos rowsets *and* repeats the join keys in a `where`. Running it
crashes:

```
ValueError: Invalid reference string found in query: ...
cooperative as (
SELECT
    INVALID_REFERENCE_BUG as "store_and_catalog_last_name"
FROM
    "thoughtful"
WHERE
    "thoughtful"."_catalog_combos_last_name" = "thoughtful"."_catalog_combos_last_name"
    and "thoughtful"."_catalog_combos_first_name" = "thoughtful"."_catalog_combos_first_name"
    and "thoughtful"."_catalog_combos_sale_date" = "thoughtful"."_catalog_combos_sale_date"
GROUP BY 1)
```

Note the WHERE is a degenerate self-comparison (`_catalog_combos_x = _catalog_combos_x`):
both operands of `store_combos.x = catalog_combos.x` were collapsed onto the **catalog**
side, the **store** datasource was dropped, and the projected output column
(`store_and_catalog_last_name`, whose lineage bottoms out at `store_sales.customer.last_name`)
has no source in the surviving CTE → sentinel. `generate_sql` "succeeds"; execution throws.
The agent burned ~1.8M tokens thrashing on this. (Its final accepted query abandoned the
rowset-join/INTERSECT shape for a `union(...) -> (...)` + `count_distinct(channel)=3` form,
which renders cleanly.)

Agent artifacts:
- failing construct: write at line 36 of `agent_log.q38.jsonl`; failing run / SQL at line 40.
- final (working) query: `results/20260628-042638_enriched/workspace/query38.preql`.

## Minimal repro
The trigger is a third rowset that **scoped-`inner join`s two other rowsets AND repeats
the same equality in a `where`**, then re-projects one side. The redundant WHERE is
required — join-only is clean.

```trilogy
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

rowset store_combos <- where store_sales.date.year = 2000
    select store_sales.customer.last_name as last_name;
rowset catalog_combos <- where catalog_sales.sold_date.year = 2000
    select catalog_sales.bill_customer.last_name as last_name;

rowset sc <-
    where store_combos.last_name = catalog_combos.last_name        -- TRIGGER (redundant)
    select store_combos.last_name as last_name
    inner join store_combos.last_name = catalog_combos.last_name;

select count(sc.last_name) as c;
```

Command (model dir = `evals/tpcds_agent/results/20260628-042638_enriched/workspace`):
```bash
.venv/Scripts/python.exe - <<'PY'
from trilogy import Environment, Dialects
env=Environment(working_path='evals/tpcds_agent/results/20260628-042638_enriched/workspace')
ex=Dialects.DUCK_DB.default_executor(environment=env)
sql=ex.generate_sql(open('repro.preql').read())[-1]
print('SENTINEL' if 'INVALID_REFERENCE_BUG' in sql else 'CLEAN')
PY
```

Observed matrix (single key, two single-column rowsets):
- `inner join` only, no WHERE → **CLEAN**
- `where <key1=key2>` + `inner join` → **SENTINEL** (either side projected fails)
- `where` only, no join clauses → DisconnectedConceptsException (separate, expected)

## Root cause (hypothesis)
The `sc` rowset's body is built by `get_query_node` with the rowset's own scoped joins
(`rowset_node.py:108-126`). The scoped `inner join store_combos.last_name = catalog_combos.last_name`
collapses the two operands into one equivalence class and **drops the store
datasource scan entirely** — only the catalog body CTE (`thoughtful`) is materialized,
exposing `_catalog_combos_*` columns.

The rowset's redundant WHERE is then resolved through the **pseudonym/equivalence
closure**, so both operands render as the surviving `_catalog_combos_*` column (the
self-comparison). But the rowset's *output* concept `sc.last_name`
(BuildRowsetItem → BASIC alias `store_combos.last_name` → base property
`store_sales.customer.last_name`) is **not** rewritten through that same closure: its
lineage still bottoms out at `store_sales.customer.last_name`, which has no entry in the
surviving CTE's `source_map`.

Emission site: `trilogy/dialect/base.py:1159-1179` — `_render_concept_sql` calls
`safe_get_cte_value(...)` for `store_sales.customer.last_name`, gets `None`, and (with
`raise_invalid=False`) falls through to
`rval = INVALID_REFERENCE_STRING(f"Missing source reference to {c.address}")` (bare
`INVALID_REFERENCE_BUG`). Confirmed via stack trace: the missing address is
`store_sales.customer.last_name`.

The asymmetry is the bug: **WHERE-operand resolution follows the scoped-join equivalence
collapse, but the rowset OUTPUT-column resolution does not** — the translation/wrapper
SelectNode (`rowset_node.py:175-196`) carries `store_and_catalog.last_name` whose source
maps to the collapsed-away store base column. The fix should either (a) record a
source_map redirect for the dropped side's base concept onto the surviving collapsed
column (pseudonym closure on output projection, mirroring the WHERE path), or (b) detect
the unresolvable output at build time and raise a clean `UnresolvableQueryException`
(naming a `union(...)`/`count_distinct=3` rewrite) instead of emitting a sentinel.

The cross-rowset merge branch in `gen_rowset_node` (`rowset_node.py:221-271`,
`_condition_operands_resolved`) is **NOT** the path taken here — it guards the *outer*
query's WHERE; this failure is entirely inside the rowset body build, so that guard never
runs (verified by instrumentation).

## Resemblance to prior fixed cases
- **`project_q64_membership_in_cross_rowset_join_set`** — closest: a cross-rowset-JOIN
  rowset whose own WHERE compares its operands crashed `INVALID_REFERENCE_BUG` even when
  selected directly. That fix made `gen_rowset_node` source all `conditions.row_arguments`.
  This case is the *next layer*: the collapse now resolves the WHERE operands fine (hence
  the self-comparison rather than a missing operand), but the re-projected **output
  column** from the dropped side is still sourceless.
- **`project_root_outer_source_key_no_coalesce`** / `get_alias` pseudonym-closure work —
  same family: a key collapsed by an OUTER/equivalence join renders raw/missing because
  one resolution path walks the pseudonym closure and another doesn't.
- **`project_q2_expr_join_filtered_div_membership_invalid_ref`** and
  **`project_q37_cross_import_membership_inlined_existence_dangling_cte`** — sibling
  "dangling CTE / sentinel from a cross-source membership/intersection" reports.
