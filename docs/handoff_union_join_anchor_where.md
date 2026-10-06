# Handoff: anchor-side WHERE over a `union join` to a rowset (2026-10-06)

Found while auditing `handoff_release_followups.md` before merging PR #702. The
regression on this branch is fixed. Everything below is OLDER than this branch
(origin/main `44d8513a8` behaves the same) unless noted.

## Start here

Branch `extension-row-null-semantics` (PR #702). Pushed head `afeb51fbd` was green
locally; `bf409bb54` (followups doc) is committed but not pushed. Uncommitted in
the working tree: regenerated benchmark artifacts (commit them with the merge) and
nothing else once the prototype is reverted; the prototype lives in
`docs/handoff_rowset_relation_prototype.patch`.

Order of work:
1. Root cause 4 + the guard: make a pre-aggregation WHERE on ROOT columns a row
   input of the aggregates it precedes; keep `_check_final_atoms_precede_aggregates`
   strict. B and the anchor-only-keys count test must pass through the plan.
2. Root cause 1 without the two general edits: the licensed-only handle must not
   change partitioning, region domains or extent ownership. Clear the 12 failures.
3. The older twin bug (`on 1=1` condition group), then anchor-only NULL rows vs the
   oracle.
4. Delete the merge-site patches root cause 1 makes dead; SQL A/B the corpus
   (`local_scripts/sql_ab`) to prove no plan grows.
5. The duplicate-row, D, binder-error and `~`-shared-key items here and in
   `handoff_release_followups.md` items 7-10.

Verify with the full local suite (`-m "not adventureworks_execution and not
clickhouse_server and not cloud_live and not bigquery_execution"`), then push and
read CI.

## Repro fixture

`_COMPOSITE_UNION_JOIN_STDDEV_FIXTURE` (tests/engine/test_duckdb_rowset.py) plus
two rows: a 2001 sale with no return, and a return with no sale.

```python
FIX = _COMPOSITE_UNION_JOIN_STDDEV_FIXTURE.replace(
    "select 3 as i, 102 as t, 2 as s, 2 as d, 13 as q",
    "select 3 as i, 102 as t, 2 as s, 2 as d, 13 as q union all\n"
    "select 4 as i, 103 as t, 1 as s, 1 as d, 17 as q",
).replace(
    "select 1 as ri, 101 as rt, 2 as rd, 3 as rq",
    "select 1 as ri, 101 as rt, 2 as rd, 3 as rq union all\n"
    "select 5 as ri, 104 as rt, 1 as rd, 4 as rq",
)
J = "union join ticket = r_filtered.r_ticket"
```

Tickets: 100, 101 (sale + return, 2001), 102 (sale only, 2000), 103 (sale only,
2001), 104 (return only). `year` is a sale-side attribute: (item, ticket) ->
date_id -> year.

## Fixed on this branch

**C. FULL join re-admitted rows the WHERE removed** (regressed in `395bbcacf`).
`where year = 2001 select ticket, r_filtered.r_ticket, r_filtered.return_quantity`
returned return-only ticket 104. The sales group emits `ticket` relabelled as
`r_filtered.r_ticket`. Once placement read a group's emitted columns,
`_group_in_active_relation` counted that alias as an owned key, saw both sides of
the relation in one scan, and pushed the WHERE below the FULL join. A plain group
no longer owns a rowset handle (`_is_rowset_handle` in condition_placement.py).
Test: `test_union_join_anchor_where_drops_rowset_only_keys`.

## Open: wrong answers

**A. Rowset-only outputs raise.** `where year = 2001 select r_filtered.return_quantity $J`
-> `DisconnectedConceptsException: WHERE input(s) ['local.year'] cannot be related`.
Same for `select r_filtered.r_ticket`, and `where state = 'CA' select r_filtered.ritem`.

**A2. The same shape with a column on the fact drops the WHERE silently.**
`where store_id = 1 select r_filtered.return_quantity $J` and
`where quantity = 5 ...` return 2, 3, 4: no filter at all.

**B. The aggregate form drops the WHERE silently.**
`where year = 2001 select count(r_filtered.return_quantity) as c $J` -> 3 (should be 2).

### Oracle

The same query over the base concepts (no rowset) plans correctly today; the rowset
form must match it. `select return_quantity union join ticket = r_ticket` with:
no WHERE -> 2, 3, 4; `where store_id = 1` -> 2, NULL; `where year = 2001` count -> 2.
A NULL row is a sale-only ticket the anchor WHERE keeps (103 under `year = 2001`).

### Owner direction (do not deviate)

- No semijoin rewrite (`R in (A ? atom)`). That is a later optimization. Normal
  discovery must source the WHERE's columns through the intermediate table
  (`dates` -> `store_sales` -> `ticket`) and join to the rowset on the declared key.
- No new rowset special-casing. A rowset is an island (its body is planned apart),
  but a declared join on its handle must enter join discovery like any key.
- Conditions are hoisted before aggregates: source the rows, filter, then aggregate.
  A FINAL-placed WHERE above an aggregate it should filter is a bug, never a
  fallback.

### Root cause

1. Join discovery (`network_build.py` / `network_search.py`) sees only physical
   datasources; a rowset output is a leaf group. Without a rowset, `ticket` and
   `r_ticket` collapse into one key and sourcing finds the join path itself. With a
   rowset, nothing tells the WHERE's ROOT group to carry `ticket`: it sources
   `store_id` from `stores` or `year` from `dates` alone, FINAL has no shared key
   and cross-joins (A2), or the disconnected guard raises (A). The concept graph's
   RELATION-edge block (concept_graph.py, `relation_members` loop) adds edges only
   for computed/aliased members and explicitly skips rowset handles.
2. Merge-site patches cover individual shapes instead: `_rowset_relation_keys`
   (aggregate row parents), the rowset-namespace block in `_final_merge_grain`,
   `_group_final_grain_contribution`, `_keyed_by_output_rowset_base` /
   `FINAL_ROWSET_BASE_KEY`. These should become dead once (1) is fixed.
3. Nothing enforces "WHERE before aggregate". About 10 branches in
   `plan_condition_placements` route an atom straight to FINAL without asking
   `decided_at_output_grain` (projection.py), which only `region_reads` uses.
4. The count's `aggregate_input_grain` is only the rowset grain
   (`r_filtered.r_ticket, r_filtered.ritem`); the rowset satisfies it alone. A
   WHERE on a ROOT column gets no edge to the aggregates it must precede (a
   rowset-side WHERE gets `[@condition]` CONSTRAINT edges; a ROOT one only works
   in the non-rowset form because it shares the aggregate's input scan). So the
   anchor scan is never a row parent of the count.

### Prototype (`docs/handoff_rowset_relation_prototype.patch`, not committed)

Apply with `git apply docs/handoff_rowset_relation_prototype.patch`. Contents:

- concept_graph.py `_add_rowset_handle_relations`: for each statement join whose
  side is a handle of a demanded rowset (found by walking lineage leaves), add the
  ROOT mate (`ticket`) and the handle as nodes and a RELATION edge mate -> handle.
  Only when the statement reads beside the rowset. A handle added only for the edge
  takes the grain of the rowset outputs the statement reads, not the join key's
  (`rs2.cid` would otherwise bring `customer_id` into the rowset group's grain).
- group_rules.py `_relation_side_partitions`: split the ROOT scan only when two
  partitions hold relation endpoints (any RELATION edge used to split `ticket` from
  `year`).
- extent_ownership.py: a group's input-contract `preserve_keys` count as exposing a
  span, so the aggregate whose merge makes cat's padding owns it.
- condition_placement.py `_check_final_atoms_precede_aggregates`: the guard from
  root cause 3. Keep it STRICT. It raises on
  `test_union_join_rowset_keeps_anchor_only_keys[ticket, count(...)-where year = 2001]`,
  which passes today only by filtering after the count; fix the plan, not the guard.

Results: A and A2 shapes return rows for union AND subset joins, all 13
`tests/engine/test_rowset_declared_join_pairing.py` pass. B now raises (guard)
instead of returning 3. A LINEAGE edge mate -> aggregate (tried, not in the patch)
made B return 2 and per-ticket counts (100,1),(101,1),(103,0), but it is a graft:
the principled fix is root cause 4 (the WHERE's ROOT args constrain the aggregates
they precede, so the anchor scan becomes a row parent).

Open against the prototype: anchor-only NULL rows are missing versus the oracle
(`store_id = 1` gives 2, oracle 2, NULL), and 12 suite tests fail, mostly outside
rowsets, which points at the group_rules and extent_ownership edits rather than the
edge:
`test_duckdb.py::test_recursive_enrichment` (the computed-key RELATION case the
partition split exists for), `test_unnest_partial_stamp.py` x2,
`test_v4_root_partition.py` padded-key-stream x5, `join_matrix/test_multi_probe_coalescing.py`,
`optimization/test_semi_join_pushdown.py::test_probe_reaches_the_scan_below_a_projected_join_target`,
`modeling/hackernews` adhoc02/adhoc03. Likely direction: mark a licensed-only handle
node (a ConceptAttrs flag) so partitioning, region domains and extent ownership
ignore it, and drop the two general edits.

The prototype also moves TPC-DS q64's plan (`zquery64.log`, about 40 fewer
lines). Rows were not compared: A/B q64's rows against main before keeping it.

**Older twin bug the prototype exposes** (fails on HEAD with no prototype):
`with r as select customer_id, status; select customer_id, r.customer_id, r.status subset join r.customer_id = customer_id where status is null;`
returns [] on `CUSTOMERS_DERIVED` and [(3, 3, None)] on `CUSTOMERS_MATERIALIZED`
(tests/helpers/models.py). The WHERE's derived `status` group (order grain) reaches
FINAL without `customer_id` and joins `on 1=1`. With the prototype the handle is a
member implicitly, so `test_rowset_pairs_on_the_declared_join_only[... where status is null; expected3]`
hits it.

### Probe harnesses

`uj_where.py` (fixture above plus `show(query)` printing sorted rows or the error,
`$J` substituted), and a spy on `build_concept_graph` / `plan_condition_placements`
printing nodes, edges, buckets and group-graph edges. `local_scripts/plan_debugger/trace_query.py --model m.preql --text "..."`
records every phase but writes nothing when planning raises.

**Duplicate rows on an axis-only projection.**
`where year = 2001 select ticket, r_filtered.r_ticket $J` returns (100, 100)
twice. Both relation sides are projected, so `axis_only_projection`
(group_graph.py) sets `deduplicate_to_grain=False` and FINAL is built with
`whole_grain=True`. The hidden `year` column carries sale-line grain, so ticket
100's two lines both survive. The fan-out that rule protects is the sides' own
rows; a column carried only for the WHERE should not add rows. Without the WHERE
the result is correct.

**D. Count over an anchor-only key is NULL, not 0, under the WHERE.**
`where year = 2001 select ticket, count(r_filtered.return_quantity) as c $J`
gives (103, None). Without the WHERE it is (103, 0). main is worse: it returns
one row.

**Binder error: membership beside an anchor output.**
`where r_filtered.r_ticket in (ticket ? year = 2001) select r_filtered.r_ticket, ticket $J`
-> DuckDB `Values list "juicy" does not have a column named "ticket"`.

## Open: plan cost (no wrong rows)

**The union axis is kept under a WHERE that already makes it one-sided.**
`where year = 2001 select ticket, r_filtered.return_quantity $J` (and the
`state = 'CA'` and composite `and item = r_filtered.ritem` variants) plans
5 joins and ~2.6k chars of SQL. The same query without the union axis merge plans
3 joins and ~1.8k chars, with identical rows. `year` is sale-side, so every
return-only axis row has NULL year and the WHERE drops it: the FULL axis reduces
to the anchor's tickets, and a RIGHT join onto the anchor is enough.

The axis merge is the `preserve_parents` MergeNode on `r_filtered.r_ticket` (two
GroupNode parents, one per side). `_merges_coalesced_sides` correctly stops it
from being folded away. The fix belongs earlier: when a WHERE atom null-rejects
every row of one side (it reads only the other side's columns and is not an
`is null` test), build the relation as a directional pairing onto the surviving
side instead of a coalesced axis. The plan-time join typing pass already reasons
about null-rejection (`null_rejected(conditions)` in partial_bridging.py and the
5(a) typing rules); reuse that.

Validation: the probe harness compared current vs no-veto plans for 886 union join
queries; the SQL-only cases (18) are exactly this shape. A fix should shrink them
and leave the 32 row-changing cases (no WHERE) untouched.

## Open: `~` facts sharing a key

Found by the item 4 probe. When two `~` facts both bind the same key (sales and
returns both binding `date_id`), the rows of a query with no measures depend on
which fact the plan reads. `where week = 1 select order_id, customer` returns sale
orders under one plan and return orders under the other. The heal only changes
which plan is chosen. Decide whether the row universe is the union of both facts'
rows.
