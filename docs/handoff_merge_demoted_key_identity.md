# Handoff: a merge-demoted key has no stable identity across the plan

**Status**: OPEN (2026-09-12). Two *symptoms* are fixed on
`space_discovery_fixes` (PR #687); the identity instability behind them is not.
Found while fixing space_reporting's frozen `launch.parquet`.

## The shape

```
merge first_org into org.code;
```

`first_org` is derived (`split(agency, '/')[1]`), `org.code` is a datasource key.
The merge DEMOTES `first_org`: `environment.concepts` keeps a lineage-less ROOT
at that address, while the real lineage moves to `alias_origin_lookup`. This is
the gcat / space_reporting model shape, and it appears 16 times across
`tests/modeling/gcat/*.preql`.

## What is already fixed (do not re-chase)

1. `4e19c1f7e` - the canonical side could not be SOURCED when the model was
   imported under a namespace sorting before `local`. Equivalence class
   representatives are the lexicographic minimum, so the spelling that won
   decided whether the bridge carried the origin. See
   `docs/` commit message and `tests/discovery/test_merged_derived_key_namespace.py`.
2. `5b13681f7` - the demoted side could not be PROJECTED alongside an aggregate
   (`select first_org, count(id) by first_org` raised "Missing source reference
   to local.first_org" while the `org.code` spelling planned). Fixed in
   `_canonical_render_siblings` by treating same-address as identity. Guard:
   `tests/discovery/test_merged_axis_spelling_equivalence.py`.

Both are the same underlying thing seen from two layers.

## The underlying issue

One authored axis carries at least four distinct canonical identities:

| where | address | canonical_address | lineage |
| --- | --- | --- | --- |
| build env concept | `local.first_org` | `org.code` | no |
| build env concept | `org.code` | `org.code` | no |
| `alias_origin_lookup` | `local.first_org` | `local._virt_func_index_access_*` | YES |
| minted at projection | `local.first_org` | `local.first_org` | no |

The fourth is minted by `render_as` inside `sort_select_output_processed`
(`trilogy/core/processing/utility.py`, ~line 259), which runs from
`optimize_ctes`:

```python
def render_as(target, oc: BuildConcept) -> BuildConcept:
    if target.address not in cte.source_map and oc.address in cte.source_map:
        cte.source_map[target.address] = list(cte.source_map[oc.address])
    return BuildConcept(
        name=target.name,
        canonical_name=target.name,   # self-canonical
        pseudonyms={oc.address},      # ONE delegation hop
        ...                           # lineage deliberately not carried
    )
```

That is a deliberate label-only concept: it exists to put the written name on
the projection and delegates the actual column to `oc` through a single
pseudonym hop. It is fine when `oc` is itself renderable.

It broke here because the hop lands on `org.code`, which is NOT directly
renderable in that CTE (its `source_map` entry is empty); the only object that
can render the value is the origin at the SAME address, `local.first_org`, which
the single hop never reaches. `5b13681f7` works by finding that same-address
origin, so it repairs the chain from the far end rather than making the
delegation transitive.

## Evidence gathered (so you do not redo it)

- Duplicate-address CTE outputs are NORMAL and load-bearing: **404 occurrences
  across 169 corpus queries / 599 CTEs**, and every one is benign (same
  canonical, same lineage presence). Zero divergent cases in
  tpc_ds / tpc_h / gcat / thelook / faa. The divergent variant only appears when
  a query projects the demoted side, which no corpus query does.
- `with_grain` is NOT the culprit; it carries `canonical_name` through
  correctly (`build.py`, `BuildConcept.with_grain`).
- The demoted concept's pseudonym set is self-referential in the build env
  (`local.first_org` pseudonyms `{'local.first_org'}`), which is why
  `render_concept_sql`'s pseudonym fallback is a no-op for it while working
  fine for `org.code` (whose pseudonym correctly points across). Source:
  `build.py` ~3455, `base_pseudonyms = {lookup_address}` where `lookup_address`
  resolved back to the same address.

## Latent hazard worth a look

`_resolve_output_target` (`utility.py` ~214) builds

```python
mapping = {x.address: x for x in cte.output_columns}
```

a dict keyed by address, so duplicate-address outputs collapse LAST WINS. With
404 duplicate-address outputs in the corpus this is currently harmless (the
twins are equivalent there), but it silently picks one of two objects that are
not guaranteed equivalent. Worth confirming whether last-wins is intended.

## Reproduction

`tests/discovery/test_merged_axis_spelling_equivalence.py` already builds the
model. The bare shape:

```
# orgs.preql
key code string;
property code.label string;
datasource organizations (code: code, label: label) grain (code)
query '''select 'NASA' as code, 'NASA' as label''';

# launch.preql
import orgs as org;
key id string;
property id.agency string;
property id.first_org <- split(agency, '/')[1];
merge first_org into org.code;
datasource launches (id: id, agency: agency) grain (id)
query '''select 'L1' as id, 'NASA/ESA' as agency''';
```

Probe the identities with `environment.materialize_for_select()` and compare
`concepts['local.first_org']`, `concepts['org.code']` and
`alias_origin_lookup['local.first_org']`.

## Where to start

The question to settle first is whether the demoted concept SHOULD keep a
self-referential pseudonym set. If its pseudonym were `org.code` (pointing
across, mirroring what `org.code` already does), the existing pseudonym
fallback in `render_concept_sql` would have resolved it and `5b13681f7` would
not have been needed.

**Gate any change there carefully.** Pseudonyms feed join resolution, the
network `_equivalence_map` (`network_build.py`, keyed on `canonical_address`)
and coverage checks, so expect a far wider corpus diff than either landed fix.
Run the corpus A/B (render `query*.preql` + `adhoc*.preql` under
`tests/modeling/tpc_ds_duckdb` and `tests/modeling/tpc_h`, diff per query),
`python -m local_scripts.fuzzer`, and the full modeling suite. Both landed fixes
were 146/146 byte-identical; anything touching pseudonyms probably will not be,
so budget time to read the diffs rather than assuming a regression.

## Why it matters

`_equivalence_map` keys classes on `concept.canonical_address`. An axis with
four canonical spellings means the class you get depends on which object the
request happened to carry, which is exactly the failure mode of `4e19c1f7e`.
Nothing in the corpus reaches it today and both known symptoms are fixed, so
this is not urgent; it is a correctness landmine rather than a live fire.
