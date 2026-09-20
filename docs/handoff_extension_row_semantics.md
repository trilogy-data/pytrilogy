# Handoff: responsive aggregates, `~` family pairing, key hosting, and the extension-row NULL rule

Session of 2026-09-19. Two PRs are open; one semantic rule is decided and only half implemented. Start at "Pick up here".

## State

| PR | Branch | What | Status when written |
|---|---|---|---|
| #699 | `two-count-shorthand-and-two-key-span` | bug fixes, no plan changes | pushed `39ba1630d`; CI re-running (was 7/7 green before the last push) |
| #700 | `host-keys-on-row-preserving-aggregates` | key hosting on; contains #699 by merge | pushed `03941c498`; CI re-running (was 7/7 green before the merge) |

Merge #699 first. #700 is based on `main` only because the workflow triggers for `main`-based PRs (`pull_request: branches: [main]`), so until #699 lands its diff shows #699's commits too. Retargeting a PR's base does not fire the workflow; close and reopen does.

Local verification of the final #699 code: `pytest tests --ignore=tests/modeling` 8622 passed, `tests/modeling` 438 passed, zero generated SQL moved. #700 adds exactly one plan change: thelook q19 (6 joins to 5, one `order_items` re-read fewer, `test_nineteen` row-checks it). The merged #700 branch got focused tests plus the thelook battery locally, not a full run; CI is its full run.

## What landed, and the reasoning worth keeping

### 1. A bare aggregate function is a responsive aggregate (#699)

`select supplier_id.count, nation_id.count;` was a keyless cross join from 0.3.316 (`(18, 18)` for `(6, 3)`) and has raised the keyless-join guard since 0.3.330. The repro lives at `trilogy-public-models/local_examples/repro_two_count_shorthand.py`.

The model, as the owner states it: an empty `by` is **responsive**, it takes the statement's grain. A grand total is `by *`, the ALL_ROWS concept. A bare aggregate `Function` lineage (what the shorthand and `function_to_concept` build) and an empty-`by` `AggregateWrapper` (what the parser builds for `auto x <- count(a)`) are two spellings of that one thing.

`Concept.get_select_grain_and_keys` is where the statement grain is known, and it runs at build. It resolved the bare spelling only `if grain.components`, so a grainless select left it unresolved, and ~40 planner seams read an aggregate off `BuildAggregateWrapper`. The fix drops that condition. One render rule had to follow: `CTE.group_concepts` kept a constant aggregate out of the GROUP BY with `isinstance(lineage, BuildFunction)`; it asks `is_aggregate` now (validation's `grain_check <- sum(1)` has purpose CONSTANT and would otherwise land in the GROUP BY).

Two wrong turns, recorded so they are not repeated:

- The first fix patched the producers (`functions.try_create_auto_derived`, which is the live one; `environment_helpers.generate_key_concepts` is not on the parse path). It worked and was reverted: it fixed two producers, not the rule.
- A blanket build-phase wrap broke 17 tests, and I read that as "the two shapes mean different things at build". They do not. The failures were the `BuildFunction` type check above. When a canonicalization breaks something, find the seam that keys on the old shape before concluding the shapes differ.

### 2. Two `~` extension families never cross-pair (#699), three layers

Natural repro: `_COMPOSITE` in `tests/engine/test_duckdb_partial_key_assembly.py`. `lines` at `(order_id, line_no)` binds `~product_id`, `orders` binds `~user_id`; `select order_id, line_no, product_id, user_id, state, sum(qty)` invented `(None, None, 30, 3, 'TX', None)`.

- **Bucketing**, `group_graph._keep_extension_families_together`. The dim peel clusters each dimension under its finest determining grain key, so the families sourced apart and `elect_extent_owners` had no joint owner. Clusters reaching a demanded span through another key merge; a cluster keyed by the span itself is a plain dimension read and stays apart. `_assemble_final_node`'s "a ROOT carrying a merge key joins on that key alone" shortcut now needs those keys to determine the ROOT's outputs.
- **Join typing**, `join_resolution._span_padded_addresses`. This was the real premise: `get_modifiers` paired two nullable keys null-safely because "both sides extended" was taken to mean shared provenance. It is not a value-NULL ambiguity (`nulls_are_values` already handles that); both NULLs are known padding, for different members. Padding is attributed per `~` span, transitively through lookups chained off a padded key; disjoint spans join FULL on plain `=`.
- **Key-pair reduction**, `reduce_concept_pairs`. A null-safe pair no longer serves as an FD determinant or triggers the grain-only restriction: NULL matching NULL says nothing about dependents.

Both bucketing and typing are needed. With bucketing off, ownership makes one side extent-free and INNER, and no typing restores rows nobody padded.

### 3. Key hosting is on (#700)

#698 shipped row-preserving aggregate hosting as values-only because a hosted KEY made the grouping grain two-keyed and triggered the pairing above. With section 2 in, `concept_graph._host_outputs_on_row_preserving_aggregates` hosts any covered output. `DomainGraph.covers` still gates it.

## Pick up here

### A. Confirm CI, merge

`gh pr checks 699` and `gh pr checks 700`. `jq` is not installed in this Git Bash; parse the tab-separated output with awk. If #699 is green, merge it, then #700 (its diff collapses to the hosting commits once #699 is in).

### B. The owed half of the extension-row rule

**Decided by the owner, 2026-09-19:** on a `~` extension row, a derived concept over an *absent* entity is NULL. An extension row carries its own dimension's attributes and NULL for everything outside that key's closure; a `CASE ... ELSE` over an order that does not exist is outside it.

Today two spellings disagree (forked model in the same test file, `big <- case when amount > 55 then 'BIG' else 'SMALL' end`):

```
select order_id, product_id, user_id, big, sum(qty);   -- NULL     CASE runs on the orders scan, then LEFT joins
select order_id, product_id, user_id, big;             -- 'SMALL'  CASE runs over the padded row, ELSE fires
```

The first is right. Pinned:

- strict xfail `test_status_on_extension_rows_is_null_without_an_aggregate` is the target;
- `test_composite_grain_families_with_by_span_aggregate` still pins `'LATER'` on its extension rows and must flip to NULL with the fix;
- `test_forked_with_status` and `test_forked_full_column_set` were already re-pinned `'LATER'` to NULL in #700.

Scope: only non-null-propagating BASICs matter (`propagates_argument_nulls` in `join_resolution` is the existing predicate); `amount + 1` is already NULL on padding.

Two candidate designs, neither started:

1. **Evaluate before the span join.** Plan such a BASIC on its entity's own rows and join the result, which is what the aggregate spelling already does through the dim peel. Least new machinery; the question is whether group formation can do it without an aggregate forcing the peel.
2. **Presence guard at render.** `CASE WHEN <entity key> IS NOT NULL THEN <expr> END`. The guard must be on the absent entity's KEY, not on the argument: guarding on `amount` conflates absence with a `?` value NULL on a real order. That means the key has to be carried to the node as a hidden output, and an expression over two entities (`order_status` reads `amount` and a by-user aggregate) needs every absent one guarded.

Start by dumping the group graph for the two spellings (patch `concept_strategies_v4.build_group_graph`; set `PYTHONIOENCODING=utf-8`, group ids contain `∅`) and see where `big` is bucketed in each.

### C. Small follow-ups

- trilogy-public-models: drop the two tpc_h skips in `tests/test_examples.py` once a release carries #699.
- `docs/handoff_aggregate_grain_fd_canonicalization.md` covers sections 2 and 3 in depth and the NULL decision; it does not mention the shorthand.

## Process notes from this session

- The full non-modeling suite took 33 to 35 minutes and ~10 GB RSS on this machine today. That is slow, not a runaway: check the `-v` log advances before killing anything.
- Never run two pytest processes at once here. Run modeling last and read `git status` for generated-SQL moves; timing artifacts always churn and are committed by design.
- A `-p` toggle plugin on `PYTHONPATH` bisects mechanisms without editing code. One that patches a name *before* another module does `from x import name` disables that importer too.
- Restore generated or experimental files with `git show HEAD:path > path`, never `checkout --` / `reset` / `stash` in this shared tree.
