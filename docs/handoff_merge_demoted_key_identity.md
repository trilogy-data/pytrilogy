# A merge-demoted key's identity across the plan

**Status**: RESOLVED (2026-09-12, PR #687). Kept as the record of what the
identities are and why the projection now renames through an alias.

## The shape

```
merge first_org into org.code;
```

`first_org` is derived (`split(agency, '/')[1]`), `org.code` is a datasource
key. The merge DEMOTES `first_org`: `environment.concepts` resolves that
address to the `org.code` build concept, while the real lineage moves to
`alias_origin_lookup['local.first_org']`. This is the gcat / space_reporting
model shape, and it appears 16 times across `tests/modeling/gcat/*.preql`.

## The identities, and which one is authoritative

| where | address | canonical_address | lineage | role |
| --- | --- | --- | --- | --- |
| `concepts['local.first_org']` (is `concepts['org.code']`) | `org.code` | `org.code` | no | what a reference to either spelling MEANS |
| `alias_origin_lookup['local.first_org']` | `local.first_org` | `local._virt_func_index_access_*` | YES | the only object that can COMPUTE the value |
| projection rename (`render_as`, `processing/utility.py`) | `local.first_org` | `local._virt_func_alias_*` | ALIAS | the written name over the column the plan produced |

The first two are by design: substitution gives the axis one meaning, the
origin keeps it sourceable. `BuildEnvironment.merge_origins` is the one lookup
for the second (discovery bridging, the concept graph's bare-key swap and the
non-BASIC-origin gate all read it).

The third was the instability. `render_as` used to mint a lineage-less,
self-canonical label that delegated to the produced column through a single
pseudonym hop. When the produced column was `org.code` and the final CTE was
folded into a scan where `org.code` has no source of its own (its value is the
origin's split, computed inline), the one hop landed on a column that could not
render, and the origin at the label's own address was unreachable
(`render_concept_sql` skips same-address pseudonym twins). Symptom:
`select first_org, count(id) by first_org` failed with "Missing source
reference to local.first_org" while the `org.code` spelling planned.

## The fix

`render_as` now mints the same ALIAS rename `select org.code as first_org`
would build: lineage `ALIAS(oc)`, derivation BASIC, canonical from the lineage
hash. Consequences, all of which fall out of existing machinery:

- It renders through `oc`, and through whatever `oc` renders through, so the
  origin is reached the same way it is for the `org.code` spelling.
- `CollapseSingleParent` and `InlineDatasource` already recognise an ALIAS
  rename (`rename_reference`) and pin it to the parent column it consumed
  (`rebind_rename_to_consumed`); after the fold the rename's lineage IS
  `ALIAS(<origin object>)`.
- The same-address special case added to `_canonical_render_siblings` is gone.

Guards: `tests/discovery/test_merged_axis_spelling_equivalence.py` (every
spelling of the axis agrees on rows; the `by` spelling leaves SQL
byte-identical) and `tests/discovery/test_merged_derived_key_namespace.py`
(the origin is bridged whatever namespace spells the key).

## Evidence kept from the investigation

- Duplicate-address CTE outputs are normal and load-bearing: 404 across 169
  corpus queries / 599 CTEs, all benign. The divergent variant only appears when
  a query projects the demoted side, which no corpus query does.
- `_resolve_output_target` keys `cte.output_columns` by address, last wins.
  Harmless today (the twins are equivalent); noted, not changed.
- tpc_ds + tpc_h + thelook + tpc_ds aggregates: 172/172 byte-identical before
  and after the rename change. Fuzzer 238/238.
