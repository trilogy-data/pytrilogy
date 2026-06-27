# Bug: shorthand reference to a nested rowset's passed-through output falsely reports "Ambiguous reference" (q64)

**Status:** FIXED 2026-06-27 — `_collapse_alias_matches` in `trilogy/parsing/v2/semantic_state.py`
drops a forward-ref (symbol-table-only) match that is an ordered subsequence of a real-concept match,
so the stale written-shorthand path no longer double-counts the one canonical output. Genuine
ambiguity (two distinct real outputs) is unaffected. Tests in `tests/test_rowset_output_shorthand.py`
(`test_nested_rowset_*`). The two "matching" addresses were the SAME single
output — a false ambiguity from the shorthand resolver double-counting one concept.
**Surfaced by:** TPC-DS q64 (run `20260627-164436`) — the agent's natural nested-rowset shape
(`base` → `y1999`/`y2000` → self-join) repeatedly hit this and couldn't get past it.
**Severity:** MEDIUM-HIGH — blocks the common "stage a rowset, re-slice it in child rowsets, then
join" pattern; the only workaround is spelling the full canonical path.

## Symptom

```
Syntax error: Ambiguous reference 'y1999.product_name':
  matches ['y1999.base.product_name', 'y1999.base.store_sales.item.product_name'].
  Qualify the full path to disambiguate.
```

But `y1999` projects **one** `product_name`. Parsing just `base` + `y1999` (no downstream), `y1999`'s
only product_name output is `y1999.base.store_sales.item.product_name` — there is **no**
`y1999.base.product_name` concept. The second "match" is synthesized by the shorthand resolver during
the downstream lookup: it treats the **shorthand path by which `y1999` referenced the column**
(`base.product_name`) and the **canonical resolved path** (`base.store_sales.item.product_name`) as two
separate candidates for the same output → false ambiguity.

## Minimal repro

```trilogy
import raw.store_sales as store_sales;

with base as
  select store_sales.item.product_name,            -- unaliased → output kept at full path: base.store_sales.item.product_name
         store_sales.item.id as item_id, count(store_sales.line_item) as cnt;

with y1999 as
  select base.product_name,                         -- shorthand for base.store_sales.item.product_name
         base.item_id, base.cnt;

select y1999.product_name, y1999.cnt limit 5;        -- ERROR: Ambiguous reference 'y1999.product_name'
```

## What works / doesn't (workarounds)

- `select y1999.base.store_sales.item.product_name` (full canonical path) → **OK**.
- Aliasing the base column (`store_sales.item.product_name as product_name`) → **still ambiguous**
  (the resolver still produces both the `base.product_name` and full-path candidates).
- Shorthand `y1999.product_name` → **ERROR** (the bug).

So the only reliable escape is the full path — which defeats the purpose of the shorthand resolver
([[project_rowset_output_shorthand_resolution]]) and is exactly what an agent won't reach for.

## Likely fix area

The shorthand resolver's candidate set (`ConceptLookup._resolve_rowset_suffix` /
`EnvironmentConceptDict._try_resolve_namespace_suffix`, semantic_state.py / environment.py) is
counting **one** rowset output under **two** addresses — the intermediate shorthand path
(`y1999.base.product_name`, by which `y1999` referenced the column) and the canonical resolved path
(`y1999.base.store_sales.item.product_name`). Dedup candidates that resolve to the **same underlying
concept** (same lineage / pseudonym / canonical address) before deciding ambiguity: a single output
reachable by two name forms must be ONE candidate, not two. The ambiguity check should only fire when
two **distinct** concepts share the suffix (the genuine `s.date.week_seq` vs `s.return_date.week_seq`
case), not when both names alias the same concept.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-164436/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql('''import raw.store_sales as store_sales;
with base as select store_sales.item.product_name, store_sales.item.id as item_id, count(store_sales.line_item) as cnt;
with y1999 as select base.product_name, base.item_id, base.cnt;
select y1999.product_name, y1999.cnt limit 5;''')   # Ambiguous reference 'y1999.product_name'
```
