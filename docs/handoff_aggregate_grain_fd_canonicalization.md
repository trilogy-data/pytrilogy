# Handoff: an abstract aggregate's identity depends on how its grain is *spelled*

Status: open, not started. Found auditing PR #695; written up with PR #696 (2026-09-18).

Pinned today as a strict xfail in `tests/discovery/test_merged_key_grain_reads_bound_derived.py::test_summary_reads_at_fd_equivalent_grain[beside_surviving_key_spelling]` (landed with #695).

## What happens

```
merge org.state_code into state.code;          # a property of org.code, respelled as a KEY
auto launch_count <- count(id);
datasource org_summary (Code: org.code, N: launch_count) grain (org.code) ...

select org.code, org.state_code, launch_count;  # reads org_summary
select org.code, state.code,     launch_count;  # recomputes from base
```

Same query, same rows, two plans. An abstract aggregate has no grain of its own: the Factory resolves it at the select grain (`__build_concept` -> `_abstract_resolution_grain` -> `get_select_grain_and_keys`), and that grain becomes the `by` list of its lineage. The canonical name is a hash of that lineage (`generate_concept_name`), so **the grain is part of the aggregate's identity**:

| query grain | canonical | matches `org_summary.N`? |
|---|---|---|
| `Grain<org.code>` | `_virt_agg_count_1670...` | yes (same hash as an explicit `count(id) by org.code`) |
| `Grain<org.code, state.code>` | `_virt_agg_count_7356...` | no |

`org.code -> state.code`, so both grains have exactly the same groups, but the second spelling is a different concept as far as every canonical-keyed lookup is concerned. #695's `_scan_rows_at_grain` FD gate never gets consulted, because the candidate check before it (`canonical_address in materialized_canonical_concepts`) already failed. The `org.state_code` spelling happens to pin at `Grain<org.code>` (observed; I have not traced why the merged-away address drops out of the pin), which is the only reason it reads the summary.

This is wider than the summary-table case. Anything keyed on aggregate canonicals sees FD-equivalent grains as distinct. Materialized-root matching is the confirmed case; prebuilt-aggregate reuse and sibling-aggregate / CTE dedupe key on the same canonicals and are worth checking. (Additive rollup is not affected: it matches on lineage signature, not canonical.) It cannot produce wrong rows (the plans are each correct); it produces missed reuse, redundant group-bys, and -- where the recompute path has no source -- an avoidable `NoDatasourceException`.

## Proposal: canonicalize an aggregate's grain to its FD-minimal key set

When pinning an abstract aggregate, reduce the resolution grain to a minimal set of components whose FD closure covers the rest: `{org.code, state.code}` -> `{org.code}`. Two spellings of one grouping then hash to one concept.

There is precedent and machinery already:
- `v4_helper/functional_dependency.minimize_build_grain` (declared keys/grains + pseudonyms; the engine `check_if_group_required` and #695's gate use).
- `concept_graph._aggregate_input_grain` already returns `minimize_build_grain(environment, input_grain)` for the aggregate *input* side. This would do the same for the *output* side.

## Why it is its own PR

1. **Seam.** The pin happens in the author-side Factory, before a `BuildEnvironment` exists; `minimize_build_grain` takes a `BuildEnvironment`. The options are (a) minimize in the Factory off `environment.domain_graph` -- but that graph's equivalence classes include authored scoped-join equalities, which hold only on matched rows and already caused a LEFT->FULL flip when used for group elision; or (b) keep the pinned `by` for rendering and minimize only the grain that feeds the canonical hash. (b) is smaller but means identity and rendered GROUP BY diverge, which every consumer of `lineage.by` has to tolerate.
2. **Which minimal set.** Minimal covers are not unique (`{a, b}` with `a <-> b`). The choice has to be deterministic and stable across statements and across the datasource-column build vs the query build, or the two sides hash differently again. `minimize_build_grain` iterates `sorted()` addresses, which is deterministic but namespace-spelling-dependent.
3. **Partial keys.** The declared-FD engine is `~`-blind; `DomainGraph.determines(population=)` is not. A `~state.code` binding proves `org.code -> state.code` only on the rows where it is present. For row multiplicity that is harmless (#695's gate relies on it); for *dropping a GROUP BY column from identity* it needs an argument, since a NULL-extended key is still one value per `org.code`.
4. **Blast radius.** Canonical names feed the model fingerprint / refresh system and the build cache key. Every aggregate selected beside an FD-determined column gets a new canonical, so expect zquery log churn across tpc_ds / thelook even where the SQL is equivalent, and a fingerprint migration question for persisted state.
5. **The GROUP BY itself.** Once identity is minimal, the rendered `GROUP BY org.code, state.code` is still needed to *project* `state.code`; that is the select's concern, not the aggregate's, and is where the "three seams" FD-closure work (`check_if_group_required`, `has_condition_key_outside_grain`, `_lineage_pinned_grain`) already lives.

## Suggested acceptance tests for that PR

- Flip the strict xfail above.
- `select k, prop_of_k_respelled_as_key, agg` and `select k, agg` produce the same canonical for `agg`.
- Two spellings in one statement (`count(id) by org.code` beside an abstract `count(id)` at `Grain<org.code, state.code>`) collapse to one aggregate CTE.
- A `~`-bound determined key: rows unchanged vs main on a fixture with extension rows.
- Blob-hash A/B of `zquery*.log` to separate "canonical renamed" churn from real SQL change.
