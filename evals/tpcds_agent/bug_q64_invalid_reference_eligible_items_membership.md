# Bug: q64 `eligible_items` rowset — HAVING→membership rewrite scans the membership-key side but projects the *other* join-equivalent side → `INVALID_REFERENCE_BUG`

**Status:** FIXED 2026-06-28 — render-time grain-key-membership redirect (option (a),
output-side). When the normal render of a single-component-grain CTE's grain-key output
dead-ends on the collapsed-away twin (the `INVALID_REFERENCE_BUG` sentinel), borrow the
column of the CTE's grain-key semijoin **left operand** — by construction
(`_build_grain_key_membership`) that IS the grain key's materialized column, the same
logical key, renderable in the scanned CTE. Implemented as `_grain_key_membership_redirect`
in `trilogy/dialect/base.py`, called as a fallback in `render_concept_sql` only when the
result already contains the sentinel. Tightly gated: single-component CTE grain, the rendered
concept IS that grain key, and the membership is an existence-set semijoin (both operands
single same-grain `BuildConcept`s, not a literal value-list). Does NOT mask q38 (its guard
raises before render) or alter q75 (its grain key is materialized, no sentinel). Regression:
`tests/test_cross_rowset_having_membership_redirect.py`.
**Severity:** high — `trilogy run` surfaces `Unexpected error in query64.preql: Invalid reference string found in query` (internal `INVALID_REFERENCE_BUG` sentinel rendered as a SELECT column). The agent went 0/5 reps and thrashed for many iterations trying to avoid chaining rowsets through `in`.
**Area:** the HAVING→semijoin lowering `_rewrite_having_finer_dims_to_membership` /
`_build_grain_key_membership` (`trilogy/parsing/v2/select_finalize.py:1191-1307`) interacting with
output/projection source-map resolution that does **not** follow the inner-join equivalence collapse
(emission `trilogy/dialect/base.py:1177-1179`, `BASE_INVALID`, line 253).

## Symptom

The agent modeled the catalog "cumulative ext_list_price > 2× cumulative refund" leg as a rowset
(`eligible_items_cat`) that **inner-joins two earlier per-item aggregate rowsets** and puts the
two-aggregate comparison in **HAVING**, then projects one side's item id and selects it. Running it:

```
ValueError: Invalid reference string found in query: ...
SELECT
    INVALID_REFERENCE_BUG as "eligible_items_cat_eiid"
FROM
    "wakeful"
WHERE
    "wakeful"."_cat_refund_item_iid" in (select juicy."_virt_filter_iid_1386117820697920"
        from juicy where juicy."_virt_filter_iid_1386117820697920" is not null)
GROUP BY 1
LIMIT (10), this should never occur.
```

The supporting CTEs render fine: `abundant` (cat_sales agg), `wakeful` (cat_refund agg), `yummy`/
`thoughtful` (their projections), and `juicy` = `yummy INNER JOIN thoughtful WHERE
cat_sales.ext_list > 2*coalesce(refund,0)` producing the existence set `_virt_filter_iid_*`. Only the
final consuming SELECT is broken: it scans `wakeful` (the **refund / `cr`** side, where the membership
key resolves) and applies the semijoin correctly, but the **projected output** `eligible_items_cat.eiid`
— whose lineage bottoms out at `cs.item.text_id` (the **sales / `cs`** side) — has no source in
`wakeful` → sentinel.

Instrumenting `INVALID_REFERENCE_STRING` confirms the missing address is exactly **`cs.item.text_id`**.

## Minimal repro (smallest snippet still emitting the sentinel)

Model dir = the eval workspace (`raw.catalog_sales`, `raw.catalog_returns`; no data needed, fails at
`generate_sql`). Two per-item aggregate CTEs, a third CTE that inner-joins them with a HAVING
comparing the two aggregates and projects the **sales** side, then selects it:

```trilogy
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

with cat_sales_item  as select cs.item.text_id as iid, sum(cs.ext_list_price)        as s1;
with cat_refund_item as select cr.item.text_id as iid, sum(coalesce(cr.refunded_cash,0)) as s2;

with eligible_items_cat as
select cat_sales_item.iid as eiid
inner join cat_sales_item.iid = cat_refund_item.iid
having cat_sales_item.s1 > 2 * coalesce(cat_refund_item.s2, 0);

select eligible_items_cat.eiid limit 10;          -- INVALID_REFERENCE_BUG as eligible_items_cat_eiid
```

Command (`ws` = `evals/tpcds_agent/results/repeat_q64_20260628-160225_enriched/workspace`):

```python
from pathlib import Path
from trilogy import Environment, Dialects
env = Environment(working_path=ws)
ex  = Dialects.DUCK_DB.default_executor(environment=env)
ex.generate_sql(Path("repro64.preql").read_text())   # raises ValueError: Invalid reference string
```

### Trigger matrix (single key, two single-column agg rowsets, isolates the defect)

| variant | result |
|---|---|
| HAVING comparing two aggregates, project **sales** side (orig) | **SENTINEL** |
| HAVING `cat_sales_item.s1 > 100` (one-side agg), project sales side | **SENTINEL** |
| same comparison moved to **WHERE** instead of HAVING | CLEAN |
| inner join, **no** HAVING/WHERE at all | `UnresolvableQueryException` (q38's guard, expected) |
| HAVING two-agg, project **refund** side (`cat_refund_item.iid`) | CLEAN |

The two CLEAN/expected rows pin the defect precisely:
- It is the **HAVING→`_virt_filter` membership lowering** specifically (WHERE form resolves fine).
- It fails **only when the projected output is the side OTHER than the membership-key side**.
  Projecting the same side the final query scans (`cat_refund_item.iid` → `wakeful`) resolves; the
  join-equivalent twin on the non-scanned side (`cat_sales_item.iid` → `abundant`) does not.

## Root cause (file:line)

1. `eligible_items_cat`'s select grain is the item id, and its HAVING compares two aggregates
   (`cat_sales_item.s1`, `cat_refund_item.s2`) that are **outside the projection** (`eiid`). So
   `_rewrite_having_finer_dims_to_membership` (`select_finalize.py:1248`, `needs_membership` at 1270)
   fires and `_build_grain_key_membership` (1191) rewrites HAVING into a semijoin
   `grain_key in (filter grain_key where <predicate>)` — the `_virt_filter_iid_*` existence set
   (`juicy`). This is the same q75-family machinery.
2. The inner join `cat_sales_item.iid = cat_refund_item.iid` collapses the two item-id concepts
   (`cs.item.text_id`, `cr.item.text_id`) into one equivalence class. The membership **left key** and
   the final FROM are resolved through that pseudonym/equivalence closure onto **one** survivor —
   here `wakeful` / `cr._cat_refund_item_iid`.
3. The rowset/consumer **output** concept `eligible_items_cat.eiid` (BASIC alias → `cat_sales_item.iid`
   → `cs.item.text_id`) is **not** rewritten through that same closure. Its source_map still demands
   `cs.item.text_id`, which is absent from `wakeful`'s source_map.
4. `_render_concept_sql` → `safe_get_cte_value(...)` returns `None`, falls through to
   `INVALID_REFERENCE_STRING("Missing source reference to cs.item.text_id")`
   (`dialect/base.py:1177-1179`), i.e. bare `INVALID_REFERENCE_BUG`.

The defect is the **asymmetry**: membership-key / final-FROM resolution follows the inner-join
equivalence collapse, but **output-column resolution does not** — so the projected twin on the
non-scanned side is sourceless. (Canonical hand-authored `tests/modeling/tpc_ds_duckdb/query64.preql`
generates CLEAN SQL — it models a single conformed source and never builds this cross-CTE
HAVING-membership, so the asymmetry never arises.)

## Verdict: SHARED-vs-DISTINCT (q64 / q38 / q75)

**Related family, but three distinct landed defects; none of the existing fixes covers q64.** All three
emit the *same* sentinel from the *same* site (`base.py:1177`) because a membership/semijoin existence
rewrite leaves a concept absent from the consuming CTE's `source_map`. But the failing concept and the
fix surface differ:

- **q75** (FIXED) — sentinel on the membership **LEFT tuple** (an unprojected *grain key* never
  materialized). Fix materialized the grain key as a hidden output. In q64 the left key resolves
  fine, so this fix does nothing here.
- **q38** (FIXED) — sentinel on an **OUTPUT column** from the **pruned** side of a cross-rowset INNER
  collapse. Fix = a clean-error **guard** (`_validate_cross_rowset_inner_joins`, `rowset_node.py:94`)
  that fires only when an operand is *absent from the materialized addresses*. In q64 **both** operands
  materialize (`yummy` and `thoughtful` both render; `juicy` joins them), so `pruned` is empty and the
  guard correctly does **not** fire — verified: the *no-filter* repro variant DOES hit this guard, but
  the HAVING variant slips past it.
- **q64** (this, OPEN) — sentinel on an **OUTPUT column** that is the **join-equivalent twin** of the
  membership key on the **non-scanned** side. Closest in **root cause** to q38 ("output/projection
  resolution does not follow the inner-join equivalence collapse that the membership/where-operand
  resolution does"), but it manifests one layer up — in the HAVING-membership **consumer** select, not
  inside a pruned rowset body — so q38's guard never applies.

**Could they be fixed together?** Only by a single *general* mechanism, not by extending either point
fix: a pre-render source_map / pseudonym-closure pass that, before emitting the final CTE, redirects any
unresolvable output **or** left-tuple concept onto a join-equivalent column that *is* present in the
scanned CTE (option (a) that q38 explicitly rejected in favor of a guard), and otherwise raises a clean
`UnresolvableQueryException`. Such a pass would subsume q75, q38, and q64. As things stand the three are
**distinct** and q64 needs its own fix — most naturally the option-(a) output-side pseudonym redirect
that q38 declined.
