# Review brief: flat-WHERE aggregate gate silently dropped both atoms

**What landed**: 2 planner fixes + 3 regression tests, in this repo (`pytrilogy_two`),
on branch `cleanup-remove-v3-discovery`. Originating bug report:
`C:\Users\ethan\coding_projects\pytrilogy\HANDOFF_v4_where_agg_dropped_atoms.md`
(filed against the other checkout; fix kept local here).

**Diff**:

| file | +/- | what |
|---|---|---|
| `trilogy/core/processing/v4_node_generators/root.py` | +42/-1 | new `_with_condition_source_join_keys` (L76), called at L303 |
| `trilogy/core/processing/v4_helper/condition_placement.py` | +50/-43 | `_nested_scope_swallows_atom` rewritten (L188), call site restructured (L1093–1112) |
| `tests/test_where_select_dual_scope.py` | +32 | `GATE_SCHEMA` + 3 tests |

`trilogy/core/processing/v4_helper/group_rules.py` also shows a 1-line diff
(`any(True for _ in successors)` → `out_degree`). **That is not part of this change** —
it was already in the shared working tree. Ignore it.

---

## 1. The problem

```
key id int; property id.cat string; property id.val int;
datasource d (id: id, cat: cat, val: val) grain (id) query '''
select 1 as id, 'a' as cat, 5 as val
union all select 2, 'a', 8
union all select 3, 'b', 3
union all select 4, 'b', 2''';
```

`where val < 8 and sum(val) by cat > 10 select id order by id asc;` returned
**all four rows**. Expected `[(1,)]`: per the flat-WHERE dual-scope contract the gate
sees the *pristine* population (`{a: 13, b: 5}`), so only cat=a clears it, and of a's
rows only id=1 has `val < 8`.

The SQL contained neither atom applied to the output stream:

```sql
WITH quizzical as (SELECT cat, id, val FROM d),          -- no `val < 8` anywhere
     highfalutin as (SELECT cat FROM quizzical GROUP BY 1 HAVING sum(val) > 10)
SELECT quizzical.id
FROM highfalutin LEFT OUTER JOIN quizzical on 1=1        -- gate joined on NOTHING
```

### The handoff's framing was one bug; it is two

The report suggested using `select id, cat` as a "working analog" for A/B diffing.
**That analog is itself silently wrong** — it returns `[(1,'a'),(2,'a')]` on `main`.
The two defects are independent and each has a query that isolates it:

| # | isolating query | before | expected | defect |
|---|---|---|---|---|
| 1 | `where sum(val) by cat > 10 select id` | 4 rows | `[(1,),(2,)]` | unkeyed rejoin (`on 1=1`) |
| 2 | `where val < 8 and sum(val) by cat > 10 select id, cat` | `[(1,'a'),(2,'a')]` | `[(1,'a')]` | `val < 8` hosted in the d1 condition scope |
| both | `where val < 8 and sum(val) by cat > 10 select id` | 4 rows | `[(1,)]` | |

All three are now tests. **Reviewer check**: revert either fix alone and confirm the
matching row above fails — they should not mask each other.

---

## 2. Defect 1 — unkeyed condition-source rejoin

**Where**: `v4_node_generators/root.py`, `gen_root`'s fallback branch (the path taken
when `plan_source` cannot source the outputs *with* the conditions, because the gate
isn't a datasource column).

That branch re-sources the gate as a standalone feeder via
`_resolve_root_condition_sources` and merges it back onto the row scan with
`condition_on_merge=True`. `MergeNode` infers join keys from **shared outputs**. Node
tree before the fix:

```
MergeNode out=[id, val] cond=(val < 8 and _virt_agg_wscope > 10)
  SelectNode out=[id, val]                      <- no `cat`
  GroupNode  out=[_virt_agg_wscope, cat]        <- gate, keyed by cat
```

Nothing shared → `on 1=1` → the gate cannot filter. With `cat` a mandatory output the
same plan joins on `cat` correctly, which is why the bug only shows when the `by` key
is absent from the select.

`resolve_condition_sources` (the *generic* fork, `condition_sources.py` L56-67) already
has a sibling guard for this exact hazard — it un-hides feeder grain keys the consumer
also carries, with the comment "hidden outputs are invisible to downstream join
inference — the merge back onto `node` degrades to a cartesian". The ROOT fork's
docstring claims it needs no analogue "because demanding those keys as mandatory
outputs stops them being hidden in the first place". **That reasoning covers only the
feeder side.** `_condition_source_search_outputs` does widen the *feeder* search to the
gate's grain; nothing widened the *consumer*.

**Fix**: `_with_condition_source_join_keys` widens `fallback_outputs` by the grain
components of the gate(s) before planning the scan. The existing
`hidden = fallback_outputs - outputs` line then hides them, so this adds join columns
only, never projected ones.

Points a reviewer should push on:

- **Gating.** It fires only under `_condition_source_uses_aggregate_contract(row_args)`
  — i.e. every unproduced condition row arg is an AGGREGATE. This deliberately mirrors
  the `aggregate_only` gate in `_resolve_root_condition_sources` that decides whether
  the feeder search widens to grain at all. If those two ever diverge, the widening
  stops matching the feeder's actual key set. Worth asserting they stay coupled.
- **`produced` is computed pre-plan** (from `fallback_outputs`) whereas
  `_resolve_root_condition_sources` computes it post-plan (`node.usable_outputs`), which
  can be a superset. Worst case we demand a key that turns out unnecessary; it is hidden
  either way. I judged this benign — confirm you agree.
- **`by *` gates.** Grain is the abstract all-rows marker, contributing no components,
  so a global gate stays a genuine cross join. Covered indirectly by
  `test_single_row_global_aggregate_stays_shared`; there is no *new* test pinning it.
  Arguably worth one.
- **Failure mode when the key is unjoinable.** If `plan_source` cannot produce outputs +
  gate grain together it returns `None` and `gen_root` returns `None` → unresolvable
  query. I deliberately did **not** add a retry-without-keys fallback: retrying would
  restore the silent cartesian, and the house rule is opt-in behaviour fails loudly.
  Full suite found no query that trips this, but it is the main regression surface.

---

## 3. Defect 2 — row atom hosted inside the d1 condition scope

**Where**: `v4_helper/condition_placement.py`.

Placement dump for the repro (before):

```
buckets:
  grp:root:root:∅                          ROOT      / ROOT     primary=[cat,id,val]
  grp:[@condition]aggregate:d1:local.cat…  AGGREGATE / D1       primary=[_virt_agg]  grain=[cat]
  grp:root:root_d1:∅                       ROOT      / ROOT_D1  primary=[cat,id,val]
edges: d1-agg -> root, root_d1 -> d1-agg
placements:
  local.val < 8   -> ('grp:[@condition]aggregate:d1:…',)   upstream_most
```

The `@condition` d1 aggregate is a lineage **ancestor** of the main ROOT (ROOT consumes
the gate value), so `_upstream_most` elects it over ROOT, which could host the atom just
as well. Two things then go wrong at once:

- the atom filters the **gate's own population** — `sum(val)` would be computed over
  `val < 8` rows, exactly the cross-filtering the dual-scope split forbids
  (`_inheritable_atoms` in `root.py` documents the same contract);
- the main row stream **never receives it**, because a d1 group feeds only the condition
  side channel — the WHERE silently vanishes.

Note `_candidate_groups` already excludes `ROOT_D1` groups but not `D1`, and
`_uncovered_exposing_output_contributor`'s docstring asserts "Condition-phase (d1)
groups are population scope and never receive row atoms" — which was not true.

### The rule

`_nested_scope_swallows_atom` already encoded the right idea but required **two**
conditions:

1. a candidate FILTER scope's own condition *implies* the atom, and
2. no nested candidate exposes the atom's row inputs in its GRAIN.

(1) is a redundancy proof via `condition_implies`, and it almost never fires — it did not
fire for the repro (an AGGREGATE scope, no filter at all) and it does not fire for q04
(`condition_implies` can't prove `year = 2001` ⟹ `year in (2001, 2002)`).

I dropped (1). **(2) alone is the real rule**: a d1 scope's value is read back through a
join on its GRAIN, so an atom within that grain selects which groups survive and
propagates outward; an atom over anything else only shifts each group's value — which is
simultaneously invisible to the outer rows *and* the population narrowing the contract
forbids. Both wrong, in different directions.

### The load-bearing detail — this is what to scrutinise

My first attempt applied the test to the **candidate pool** (filter nested gids out of
`restricted`, then `_choose_groups`). That is what the old code did, and it **broke
TPC-DS q04** with `Missing source reference to sales.sale_date.year`.

Why: q04's `sales.sale_date.year in (first_year, second_year)` is legitimately elected at
**both** ROOT and its d1 filter scope:

```
CAND  year in (first,second) -> [root, basic:d*:billing_customer.sk, agg:d1, filter:d1]
PLACE year in (first,second) -> ('grp:root:root:∅', 'grp:[@condition]filter:d1:…')   # baseline
```

Removing the nested gids re-runs `_upstream_most` over a *different* pool, which no
longer elects ROOT either — it lands on the customer-grain BASIC group, which cannot
source `sale_date.year`. So the "fix" moved the atom to a third group that was never a
sensible host.

Final shape:

```python
chosen_groups = _choose_groups(restricted, lineage_ancestors_graph, main_lineage)
if _nested_scope_swallows_atom(row_inputs, chosen_groups, restricted, nested_ids, buckets):
    outer_hosts = [gid for gid in restricted if gid not in nested_ids]
    if outer_hosts:
        chosen_groups = _choose_groups(outer_hosts, lineage_ancestors_graph, main_lineage)
```

Two invariants, both deliberate:

- **Judged on the ELECTED hosts, not the pool.** Returns `False` unless *every* chosen
  host is nested. A nested host elected alongside an outer one already reaches the outer
  rows through that outer copy, so there is nothing to rescue (q04).
- **Grain exposure asked of the whole nested SCOPE** (`restricted`), not the elected host
  alone. The FILTER sits at a finer grain than the AGGREGATE that carries the value out;
  asking only the elected FILTER regressed
  `test_atom_the_scope_keys_its_value_by_stays_in_the_scope` (q30/q81: chosen
  `('filter',)` grain `{id}`, but the scope's aggregate is grained `{region}` and that is
  what re-enters the outer plan).

Those two unit tests in `tests/core/processing/test_v4_condition_placement.py`
(`..._a_filter_scope_cannot_propagate_lands_outside_the_scope` = tpch q02 →
`('root',)`; `..._the_scope_keys_its_value_by_stays_in_the_scope` = q30/q81 →
`('filter',)`) are the tightest guard on this rule and both still pass unchanged.

Points a reviewer should push on:

- The rule is **strictly wider than before** (condition 1 removed). The compensating
  narrowing is the all-elected-hosts-nested gate. Convince yourself those two changes
  don't leave a gap in either direction — specifically: is there a shape where the atom
  is elected *only* at nested hosts, the scope's grain does not cover it, and hosting it
  there is nonetheless correct? I could not construct one, and the corpus found none.
- `if outer_hosts:` (rather than the old `outer_hosts or restricted`) is now a no-op
  guard — if there are no outer hosts we keep the already-computed `chosen_groups`.
  Same semantics as before, just expressed without recomputing. Verify.
- `condition_implies` / `BuildFilterItem` imports were dropped from the module; confirm
  no other caller wanted them (ruff says no).

---

## 4. Verification

- **New tests** — `tests/test_where_select_dual_scope.py`: `GATE_SCHEMA` plus
  `test_aggregate_gate_by_key_absent_from_select` → `[(1,)]`,
  `..._by_key_present_in_select` → `[(1,'a')]`, `..._alone_by_key_absent_from_select`
  → `[(1,),(2,)]`. All assert **rows**, not shapes.
- **Final SQL for the repro** — `val < 8` on the output stream,
  `INNER JOIN … on highfalutin.cat = quizzical.cat`, and `HAVING sum(val) > 10` computed
  over the *unfiltered* `quizzical` scan (population semantics preserved).
- **Full suite** `-m "not adventureworks_execution"`: **7449 passed, 121 skipped,
  1 xpassed, 0 failed** (21m17s). Includes all 164 TPC-DS, TPC-H, `tests/join_matrix`,
  `tests/core/processing`, `tests/optimization`.
- ruff (`--select E,F,I`), black, mypy clean on the two changed files; `mypy trilogy`
  clean across 345 files.

**Not applicable in this tree**: the handoff's acceptance item 2 also asks for the staged
`where … then where …` spelling to assert `[]`. `then where` does not parse here
(`InvalidSyntaxException`) — it lives on the other checkout's `hierarchical_where`
branch. Whoever lands that feature should add the staged assertion, since it rides the
same placement machinery (`PlacementReason.STAGE_PRECONDITION` → ROOT_D1 feeders) and
the fix above deliberately does not touch stage delivery.

## 5. Suggested review order

1. Reproduce the three table rows in §1 against the current tree (they pass), then revert
   each fix in turn and confirm only its own row fails.
2. Read `_nested_scope_swallows_atom` (L188) against its two unit tests — that is where a
   subtle mistake would hide.
3. Sanity-check the `_with_condition_source_join_keys` gating against
   `_resolve_root_condition_sources`' `aggregate_only` branch; they must agree.
4. If you want more assurance than the suites give, a per-query generated-SQL diff over
   the TPC-DS/TPC-H corpus would show the blast radius of the widened placement rule in
   size terms (the suites prove rows, not bytes).
