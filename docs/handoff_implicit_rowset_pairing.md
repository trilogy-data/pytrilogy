# Handoff: implicit rowset pairing — the census, and what changed

## Closed out 2026-09-30: the pairing is declared

Owner's ruling on the census below: the "genuinely implicit" corpus is small,
so make it explicit — a rowset's outputs pair with a concept outside the
rowset only through a declared relation (`subset join rs.key = key`), or by
projecting the concept inside the rowset and reading it through the handle.
Keyless joins (a scalar rowset beside rows) stay fine. Shape C's premise was
never "the filter applies unaliased"; it is "the filter applies", and it does
under the declaration.

| item | outcome |
|---|---|
| The gate | `query_processor._raise_if_disconnected` runs with `island_rowsets=True`. Shapes A, B, C and bug 2 are `DisconnectedConceptsException` |
| Scalar over a handle under a WHERE (`sum(rs.amt) where cat = 'a'`) | the single-row output is no longer skipped as crossjoinable: it anchors on the multi-row handle it reads (`_rowset_handles_read_by_scalars`) and is not an EXISTS gate |
| The helpful error | `rowset_relation_hints`: for a rowset read in one subgraph, every output whose body content lands in another subgraph's component becomes `subset join <handle> = <content>`; keys that determine the other side rank first; a sibling rowset wrapping the same body concept gets `subset join theirs = ours`, listed last |
| Shape C declared, keyless join (planner bug) | FIXED. `output_rowset_base_keys` and the new `_rowset_base_grain` count a boundary grain key a relation has already spelled at its base; the substitution had made `resolve_rowset_content_address` a no-op there, so the condition root never rendered the key |
| Shape C declared, scalar: `sum(rs.amt) subset join rs.oid = oid where cat = 'a'` returned 6.00 (filter dropped) | FIXED. The group-internal pre-merge now carries `preexisting_conditions` from its parents' hosted atoms, as the FINAL merge already did, so `tighten_join_for_filtered_branch` sees the filtered scan as the population instead of FULL-joining it to the subset-declared boundary. Two limits, each found by a wrong-rows test: only ROOT parents (a grouping parent applies the atom to its INPUT rows; claiming it on its output let the FINAL skip its gate, `test_where_select_dual_scope`), and only NULL-REJECTING atoms (an `is null` atom is satisfied by padding; threading it dropped a `~?` guest row, `test_duckdb_rowset_null_group_rejoin`) |
| TVF-union ORDER BY carry | the union column `_carry_order_by_concepts` hides for a rowset handle is left out of the gate's set (`_carried_union_columns`): it renders at the union node the handle wraps and is not an authored request |
| `connected_equivalent_suggestions` | a rowset handle is no longer a "separately-imported copy" twin (`user_id` vs `even.user_id`) |

### The declared contract, as the tests now pin it

Under `subset join rs.key = key` the key is ONE axis read from the superset
side (docs/subset_union_join_design.md): `select user_id, even.user_id,
even.sale_price` shows `(1, 1, None)` for a user `even` filtered out, not
`(1, None, None)`, and `count(rs.oid)` counts the padded rows. The
intersection is a non-key value: `count(rs.amt)`, or `rs.amt is not null`.

Tests rewritten to the declared form: `test_duckdb_rowset_aggregate_filter_leak.py`
(plus `test_undeclared_pairing_names_the_join`, which pins the hint),
`tests/complex/test_rowset.py` (the four alias-collision tests, `subset join
buyers_a.id = id and buyers_b.id = id`), `tests/modeling/test_complex.py::
test_rowset_with_addition` (plus an error guard), `test_v4_root_partition.py`
(the xfail is now a real error assertion;
`test_bare_key_beside_a_filtered_rowset_keeps_every_key` declares the join),
`tests/engine/test_duckdb.py::test_rowset_join`, and three
`local_scripts/v4_evals/cases` (`rowset_alias_collision`,
`rowset_outer_addition`, `rowset_rank_join_fanout`). Two rowsets over one base
with no bare key (`select rs_a.grp_key, rs_a.total, rs_b.total`) are held to
the same rule: `subset join rs_b.grp_key = rs_a.grp_key`.
`trilogy/ai/constants.py` tells the agent the join is required.

TPC-DS q44's outer `where ss.store.sk = 1` paired a base concept with the two
rank rowsets only through their bodies, which already carry it; the
restatement is dropped. Rows are identical; the rowset pair now renders the
declared `subset join descending.rnk_d = ascending.rnk_a` as LEFT from
`ascending` with a coalesced key where the outer gate's proof used to make it
INNER (same CTEs, +306 chars, rebaselined).

### Found and left

- `_final_merge_grain` still answers two questions with one set (below,
  unchanged). The implicit machinery — the boundary exposing base keys,
  `resolve_rowset_content_address` in three passes — is still there and now
  runs only under a declaration or for scalar shapes; a follow-up could fold
  it into the authored-relation path.
- The nested body gate (`nested_select.py`) and the existence-argument gate
  still pass `island_rowsets=False`: a rowset body reading another rowset
  beside base concepts is not yet held to the rule.
- `auto join <cte_name>` (bind a rowset's outputs as a subset of their
  licensed parents) is the owner's suggested follow-up.

OWNER NOTEs (as received):


Given the smaller corpus of "genuinely implicit" let's clean up the test assertions and make it all explicit. This will require fixing buggy discovery in the C. case.

Keyless joins are obviously fine - a scalar aggregate can be joined (rowset or not) with no licensed relation.

"| **C. a base property filtering rowset rows**: `select rs.oid, rs.amt where cat = 'a'` (`test_duckdb_rowset_aggregate_filter_leak.py`, whose premise is that this MUST hold), `select cur_period.wk ... where year = ...` | 6 | base property ↔ projected handle |" is misinterpreted - the premise is that the filter applies, not that the filter should be applied unaliased. 

A bonus would be a helpful error on disconnected joins when there _is_ a potential implicit join suggesting the join syntax.

We could further consider an 'auto join <cte_name>' which would implicitly bind the concepts as a subset of licensed parents, but perhaps leave that as a followup.


ORIGINAL DOC:

Follows `docs/handoff_region_domain_kinds.md` ("Found and left"). The owner's
question was whether a rowset handle should pair with a base concept at all
without a declared join, and how much of the suite relies on it. This records
the census, the two fixes taken, the one left, and the design finding.

## Status

| item | outcome |
|---|---|
| Wrong rows: `select r.user_id, r.state, sum(r.sale_price)` (user 1 twice) | FIXED. `_rowset_join_key_addresses` reads the handle's own grain, not its `keys` |
| Keyless join: `select r.user_id, state, sum(r.sale_price)` | LEFT, loud (`UnresolvableQueryException`). The declared spelling works, rows verified; see below |
| Wrong rows, new, branch-only: `select user_id, even.user_id, even.sale_price` under `~` dropped users 1 and 3 | FIXED. A boundary partial on a handle's content is partial on the handle for host election |
| Suite | 9 failures before; the run after the fixes is in the PR |

Guards: `tests/core/processing/test_v4_root_partition.py` (the last four
tests) and `test_v4_group_behaviors.py::test_final_contributor_contract_uses_rowset_lineage_join_key`.

## The two bugs, as found

**Bug 1** was the FK-path `keys` artifact. `order_items` binding `~user_id` at
line grain stamps `user_id.keys == {line_id}` on the environment concept, and
a rowset output snapshots that (`rowset_semantics.py`, deliberately). Every
other reader of a rowset output's axis treats the output like any concept; the
one that did not was `_final_merge_grain`'s rowset branch, which walked
`concept.keys` to get from a handle back to the base key. Right for a
property (`r.state.keys == {r.user_id}`), wrong for a KEY: it volunteered
`r.line_id` into the merge grain, and the FINAL dedup read the line-grain
merge as already at the output grain. The fix takes the handle's own grain and
unwraps it through selected concepts' lineage as before, so an unselected key
does not change what the axis says. Nothing else moved: `test_rowset_with_addition`
is SQL-identical.

The owner's larger point stands and is not done: `_final_merge_grain`
answers two questions with one set. The result's row grain is
`from_concepts(outputs)` with handles as ordinary concepts; the join axis a
ROOT sibling needs is that grain unwrapped to base addresses, because a root
scan can never emit a handle. Splitting the two would remove the rowset branch
from `_final_merge_grain` entirely, but touches the authored-relation and
mixed root/rowset cases too.

**Bug 2** is a different question: an outer-statement `users` scan (for the
bare `state`) beside an aggregate over the boundary (emitting `r.user_id`).
Nothing but the handle's content licenses `r.user_id = local.user_id`. The
group graph's capability pass promises the boundary renders its grain keys
under their base address; the boundary generator refuses to expose a base key
an exposed handle already covers (`rowset.py`, "a key an EXPOSED handle
already covers is not re-exposed"). Making the generator honour the demand
fixed the statement and broke 8 tests: `test_rowset_with_addition`, both
`subset` rows of `test_scoped_derived_rowset_join_matrix` (the exact hazard
the generator's comment names: the second name for one value outranks an
authored derived-key join), tpc-ds q14/q54/q64, and a semi-join pushdown test.
Reverted. The declared spelling plans and returns the right rows today:

```
select r.user_id, state, sum(r.sale_price) as revenue
subset join r.user_id = user_id;
```

## The census

`search_concepts` was patched read-only over the suite (9,450 tests) to record
every depth-0 statement whose outputs or WHERE args mix a concept reaching a
rowset handle with a non-constant concept that does not, with no scoped join
declared. 74 hits.

| kind | count | pairing |
|---|---|---|
| a rowset handle that is a global scalar beside row concepts (`_subquery_*.mx`, `overall.overall_avg`, tpc-ds q14's `avg_sales.average_sales`, q23's `max_total.cmax`, q44's threshold) | ~50 | none; a keyless scalar merge |
| **A. outer addition**: `rowset even_orders <- select order_id, store_id where ...; select order_id, even_orders.order_id, even_orders.store_id` expecting `(1, None, None)` | 2 | base key ↔ handle |
| **B. two rowsets over one base beside the bare key**: `select id, buyers_a.cust_id, buyers_b.cust_id`; `per_txn.tcust` beside `txn_cust` | 6 | base key ↔ two handles |
| **C. a base property filtering rowset rows**: `select rs.oid, rs.amt where cat = 'a'` (`test_duckdb_rowset_aggregate_filter_leak.py`, whose premise is that this MUST hold), `select cur_period.wk ... where year = ...` | 6 | base property ↔ projected handle |
| the error path: `single_rowset_islanded_property_clean_error` expects a clean `DisconnectedConceptsException` when the base property is NOT reachable | 1 | — |

The tpc-ds and tpc-h corpora mix rowsets with scalars only. Every genuine key
pairing in the suite is one rule in one direction: base concept ↔ rowset
handle, licensed by the handle's content, and it is confined to unit and
engine tests.

## Can they be declared instead?

Tried on the guard model, boundary fix reverted:

| shape | declared spelling | result |
|---|---|---|
| bug 2 | `subset join r.user_id = user_id` | right rows, both models |
| bug 2 | `union join r.user_id = user_id` | right rows; FULL, user 3 present without `~` |
| A | `subset join even.user_id = user_id` | plans, but `(1, 1, None)` not `(1, None, None)`: the declared relation coalesces the two keys into one axis, so the handle no longer reads NULL where its body filtered the row |
| B | `subset join a.user_id = user_id and b.user_id = user_id` | right rows; a RIGHT + FULL with `coalesce` where the implicit form is two INNERs |
| C | `select rs.user_id, rs.line_id, rs.sale_price subset join rs.user_id = user_id where state = 'ca'` | **keyless join, a planner bug in the authored path** |
| C | the same without the handle projected | `DisconnectedConceptsException` |

So "just add joins" is not a drop-in: shape A's contract changes under a
declared relation (the implicit form keeps the handle's own NULL; the declared
one unifies the axis), and shape C does not plan declared at all.

## The design finding

The implicit form already IS "the boundary declares a subset relation on its
output keys to their base concepts": shape A's expected rows are exactly a
`left join` from the base side. It is just implemented as lineage special
cases in three places — `_final_merge_grain` (the axis), the boundary
generator (which base key to expose), and join resolution
(`resolve_rowset_content_address`, "one boundary rule") — rather than as one
relation the authored machinery consumes. Two ways to make it one thing:

1. **The boundary mints the relation.** At rowset creation, each grain-key
   output whose content is a base key registers `handle ⊑ base` the way a
   scoped `subset join` does. Bug 2 falls out; shapes A/B/C keep working IF
   the axis unification is made to keep the subset side's own value (shape A's
   NULL) — today it coalesces. Shape C's authored-path bug has to be fixed
   first either way.
2. **Require the declaration.** Shapes A, B, C and bug 2 become errors without
   a `subset join`; ~14 tests and the filter-leak file's premise change. The
   owner's view: mixing implicit and explicit is the weird part; "create a
   rowset, then join it to other info on NOT all natural keys" cannot be said
   today.

Neither was taken here. The branch's job was a believed-good state.

## The branch-only regression

`select user_id, even.user_id, even.sale_price` over a filtered rowset,
under `~user_id`, returned `[(2, 2, 3.0)]`. Bisected to `967298c40`
("Keyspace phase 7 (WIP): the region contract on nodes"). Plan-time typing
was `users RIGHT OUTER boundary`: with `host_grain == {even.user_id}` the
host election (`join_resolution`, the SideFacts builder) excluded a side's
`partial_concepts` by address, and the boundary's stamp is on the content
(`local.user_id`) while the grain is spelled as the handle, so the filtered
boundary read as binding the key completely and out-hosted the complete users
scan. Now a handle whose content the side marks partial is partial for
hosting; the pair types FULL and the optimizer narrows it to LEFT from
`users`. Main was right (no host election there).

## Tools

- `local_scripts/sql_ab` as before; the census probe was a pytest plugin
  patching `query_processor.search_concepts_v4` (the name is bound at import,
  so patching `concept_strategies_v4` alone misses the top-level call).
- `tests/core/processing/test_v4_root_partition.py::_rows` runs a statement
  on the guard model with and without the `~`.
