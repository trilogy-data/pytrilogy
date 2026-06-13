# Handoff: cross-CTE aggregate dead-ends with unhelpful "No remaining priority concepts"

**Status:** OPEN (2026-06-13). Diagnosis + reproduction confirmed; fix is a
design call (see "Owner's thesis" / "Decision points"). Surfaced by the q31
agent leg.

## Symptom

A query that aggregates two **named CTE outputs** together, with no join/merge
relating them, dies with the raw-internals fallback error instead of the
existing human-readable disconnection error:

```
Resolution error: Cannot resolve query. No remaining priority concepts,
have attempted {'store_by_q.county', 'local._virt_filter_web_ext_sales_…'}
out of [local._virt_filter_web_ext_sales_…@Grain<store_by_q.county>,
        store_by_q.county@Grain<store_by_q.county,store_by_q.quarter>,
        web_by_q.web_ext_sales@Grain<web_by_q.county,web_by_q.quarter>]
with found {…}
```

The `_virt_filter_<hash>` aliases and `@Grain<…>` dumps are not actionable.

## Minimal reproduction

`tests/modeling/tpc_ds_duckdb` model (the q31 shape, trimmed):

```trilogy
import physical_sales as store_sales;
import web_sales as web_sales;

with store_by_q as
where store_sales.date.year = 2000
select store_sales.sale_address.county as county,
       store_sales.date.quarter as quarter,
       sum(store_sales.ext_sales_price) as store_ext_sales;

with web_by_q as
where web_sales.date.year = 2000
select web_sales.bill_address.county as county,
       web_sales.date.quarter as quarter,
       sum(web_sales.ext_sales_price) as web_ext_sales;

with county_data as
select store_by_q.county as county,                                  -- only join/group key
       sum(store_by_q.store_ext_sales ? store_by_q.quarter = 1) as store_q1,
       sum(web_by_q.web_ext_sales   ? web_by_q.quarter   = 1) as web_q1;  -- web has NO key relating it to store_by_q.county

select county_data.county, county_data.store_q1, county_data.web_q1;
```

`county_data` groups by `store_by_q.county` but pulls `web_by_q.web_ext_sales`.
`store_by_q.county` and `web_by_q.county` are **distinct addresses**, never
equated by a join or merge — so there is no key to fold `web_by_q` rows into
`store_by_q.county` groups. The query is genuinely unresolvable; the issue is the
*error*, and (per the owner's thesis) the *detection*.

## Root cause of the unhelpful message

`trilogy/core/processing/discovery_utility.py :: get_priority_concept` (terminal
raise ~L519-534) has the right error one branch up but it's gated:

```python
subgraphs = disconnected_components(environment, all_concepts)
if len(subgraphs) > 1:
    raise DisconnectedConceptsException(format_disconnected_subgraphs_error(subgraphs))
    # "...split into N disconnected subgraphs: {...}. Are you missing a join or merge statement?"
raise DisconnectedConceptsException("Cannot resolve query. No remaining priority concepts, ...")
```

For this query `disconnected_components(...)` returns **1 group**, so we fall
through to the raw dump. Verified at the raise point:

```
>>> 3 concepts, disconnected_components -> 1 group(s)
    group 0: ['local._virt_filter_web_ext_sales_…', 'store_by_q.county', 'web_by_q.web_ext_sales']
```

Why 1 group: `disconnected_components` → `_anchor_nodes(concept)`
(`discovery_utility.py` ~L546-560) anchors each concept to **base-model**
reference-graph nodes:

```python
nodes = [concept_to_node(concept), concept_to_node(concept.with_default_grain())]
for arg in concept.concept_arguments:           # <-- collapses CTE output to its base source
    if isinstance(arg, BuildConcept):
        nodes.append(concept_to_node(arg.with_default_grain()))
```

`store_by_q.county`'s own node isn't in the model graph, but its source arg
`store_sales.sale_address.county` is; `web_by_q.web_ext_sales` collapses to
`web_sales.ext_sales_price`. In a star schema those base nodes are weakly
connected through the shared `address` / `date` dimensions, so the two CTE
outputs land in **one component** → "connected" → false negative on disconnection.

Note the base concepts aren't even the same: `sale_address.county` vs
`bill_address.county` (different role-played addresses). They're connected only
by whole-schema weak reachability, not by anything that makes a merge resolvable.

## Owner's thesis (design direction)

> CTE outputs are **unique concepts** and should not be collapsed to their base
> nodes. Either there is a real weak connection between the two CTEs — in which
> case we should **inject the bridging node(s) to finish the merge** and resolve
> — or there isn't, in which case raise the helpful `DisconnectedConceptsException`.
> **Grain does not matter** — we can always group an arbitrary grain; **only
> joins matter.** So the connectivity question is purely: is there a join/merge
> path relating these CTE-output concepts?

Implication: the bug is the `_anchor_nodes` collapse. Treating CTE/derived
outputs as their own graph nodes makes `disconnected_components` accurate here —
`store_by_q.*` and `web_by_q.*` become separate components (no join/merge edge
between the two CTEs) → the real error fires. The grain mismatch
(`Grain<store_by_q.county>` vs `Grain<web_by_q.county, web_by_q.quarter>`) is a
red herring; do **not** build the fix around grain reconciliation.

## Decision points for the implementer

1. **Where do CTE-output concepts get their own nodes?** Today they're absent
   from the model reference graph and `_anchor_nodes` back-fills via source args.
   Option: add CTE/rowset output concepts as nodes with edges ONLY to whatever
   actually relates them (an explicit join/merge or a shared grouping key carried
   into the combining select), not to their base lineage. Then drop the source-arg
   fallback in `_anchor_nodes` (or make it not bridge across distinct CTEs).

2. **"Inject nodes to finish the merge" vs raise.** When two CTE outputs DO share
   a relatable key (e.g. both expose `county` and the combining select could align
   on it), should the planner auto-inject the shared key as a join target and
   resolve, rather than require the user to write it? For the repro the user only
   referenced `store_by_q.county` (not `web_by_q.county`) in `county_data`, so
   there is currently no shared key in scope — likely the correct behavior is
   **raise**. But `sale_address.county` vs `bill_address.county` are different
   roles, so even an explicit `web_by_q.county` alignment may need a `merge`.
   Decide whether weak-but-unjoined ⇒ raise (safest) or ⇒ auto-bridge (riskier;
   compare to the rejected auto-bridge in
   `project_scoped_join_property_enrichment` — do NOT auto-bridge into a FULL JOIN
   1=1).

3. **Fallback message** (cheap win regardless of 1/2): even if detection stays
   coarse, rewrite the terminal raise to drop `_virt_filter_<hash>` internals and
   name the unresolved real concepts + their unrelated sources, in the spirit of
   `format_disconnected_subgraphs_error`.

## Files / pointers

- `trilogy/core/processing/discovery_utility.py`
  - `get_priority_concept` terminal raise (~L519-534) — the two-branch error site
  - `disconnected_components` (~L563-603) — partitions by model-graph weak
    components; uses `_anchor_nodes`
  - `_anchor_nodes` (~L546-560) — **the collapse**; back-fills CTE outputs to
    base nodes via `concept_arguments`
  - `format_disconnected_subgraphs_error` (~L606-617) — the good message
- Related prior art: `handoff_q64_join_grain_resolution.md` (the *other* "No
  remaining priority concepts" cause — scoped-join derived key not sourceable;
  FIXED by mirroring `merge_concept`). Different root cause; same terminal raise.

## Reproduction / probe script

Wrap `get_priority_concept` to print `disconnected_components` group count at the
raise (confirmed 1 group):

```python
import trilogy.core.processing.discovery_utility as du
orig = du.get_priority_concept
def wrapped(all_concepts, attempted, found, partial, depth, environment=None):
    try:
        return orig(all_concepts, attempted, found, partial, depth, environment)
    except du.DisconnectedConceptsException:
        if environment is not None:
            subs = du.disconnected_components(environment, all_concepts)
            print(len(subs), [[c.address for c in g] for g in subs])
        raise
du.get_priority_concept = wrapped
# then parse_text(repro_query, Environment(working_path='tests/modeling/tpc_ds_duckdb'))
# and process_query(env, statements[-1])
```

## Acceptance criteria

- The repro raises `DisconnectedConceptsException` with a message naming the
  real unresolved concepts and suggesting a missing join/merge (no
  `_virt_filter_<hash>` / `@Grain<…>` dump).
- A genuinely-connected cross-CTE query (two CTEs explicitly joined/merged on a
  shared key) still resolves — no regression in `disconnected_components`
  treating real merges as disconnected.
- Full suite green: `ruff check . --fix`, `mypy trilogy`, `black .`, and the
  tpc_ds parse/perf sweep (`-m "not adventureworks_execution"`).
```
