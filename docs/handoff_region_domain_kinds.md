# Handoff: every demanded region has a domain; carried keys are read off their fields

Follows `docs/handoff_root_partition_open_items.md` (status section at its
top). Two things the owner questioned there were taken on, and the rest of
that list was pushed as far as the measurements allowed.

| question | answer |
|---|---|
| "a demanded region has no domain"? | It has one. `decide_region_domains` returns a `RegionDomain` for every region the statement asks rows of, and `DomainKind` says where the rows come from |
| are the spans duplicative of the carried keys? | Yes. `carried_keys` was four writers into one list; it is a property now, read off `grain_components`, `anchor_keys` and `carried_spans` |

## What changed

| before | now |
|---|---|
| `RegionDomain(region, bucket)`, only for a region given a bucket | `RegionDomain(region, kind, label, carried, bucket, note)`, one per demanded region with rows of its own |
| `_keep_extension_families_together(assignment, keyspace, environment)` | `(assignment, domains)`: it reads the domains of its scope and nothing else |
| `GroupBucket.carried_keys: list`, `GroupAttrs.carried_keys`, `GroupAttrs.members` stored | properties. `carried_spans` (written by `carry_region_spans`) and `anchor_keys` (written by the entity split) are the stored fields |
| `_carry_grain_keys` | gone: a grouping bucket's grain is its `grain_components` |
| trace step "grain keys carried" | "carried-only row streams split" |
| `split_carried_only_row_streams` finds the domains by `extent_spans` | it is handed `RootPartition.domains` |
| `_can_merge_nested_signatures` asks `gid.startswith("grp:root")` | asks the derivation of the concept behind the group, so a labelled scope (`grp:[r]root:…`) answers the same way |
| the plan trace shows buckets | the "region domains added" step also lists each domain, its kind and what it carries (`BucketsStep.domains`, a table in the viewer) |

`DomainKind`, with the count over the suite (313 demanded region and scope
pairs in 208 tests, measured before the change):

| kind | rows come from | count |
|---|---|---|
| `OWN` | a ROOT bucket of its own, `…:extent:<spans>` | 270 |
| `BOUNDARY` | the rowset boundary whose body padded the region; nothing has to be evaluated on the solid rows | 15 |
| `ROW_STREAM` | the scope's row stream: it sources nothing absent on the region (gcat's composite-key region) | 8 |
| `PADDED` | not modelled, the padded plan stands in: a materialized rollup (8), a WHERE atom no host can apply to a domain (3, tpc-h q22's shape) | 11 |
| `RELATION` | an authored coalescing relation unions the key (`union join ocust = cid`) | 2 |

Seven more pairs are a demanded span whose region has no rows of its own (it
is kept by a completion). Such a region is rows of the larger source and gets
no domain.

## How it was measured

`local_scripts/sql_ab`, base `66a54c618`:

| oracle | result |
|---|---|
| corpus SQL, 206 statements | SUITE_CORPUS |
| suite SQL, about 9,680 tests | SUITE_RESULT |

The corpus compile is not enough on its own. Pointing `_members_of` at the
output set moved nothing in the corpus and broke two tests and grew a third
plan in the suite (below).

## The open items of the last handoff

| item | outcome |
|---|---|
| 1. `_keep_extension_families_together` reads the keyspace | It reads domains. The merge branch IS reachable, the suite just never reached it: two families peeled off two keys (`brand` off `item_id`, `region` off `order_id`) are rekeyed to `dim:item_id\|order_id`. The plain reading ("un-peel whatever a domain carries") gives the same rows and up to two more CTEs and three more joins there, so the rule keeps its merge. Guarded by `test_families_peeled_off_two_keys_source_as_one_cluster` |
| 2. Stage 3 reads `carried_keys` | `_condition_twins`, `_consumer_reads` and the accumulated-atom columns read the output set. `_members_of` does not: see below. The FINAL candidate sort and `elect_extent_owners.rank` read `members`. `_anchor_scalars_to_dim_peel_key` reads `anchor_keys`, and the capability pass reads `dim_keys` and `carried_spans` |
| 3. the entity split is a sourcing decision | untouched |
| 4. composite keys use d0 grains only | Decided: d0 only is right. From every depth, tpc-h q20 with its keys selected (`select part.id, part.supplier.id, part.supplier.name` under q20's WHERE) goes from 2 CTEs and 5 joins to 6 and 8, rows the same. The peel reaches FINAL beside the row stream the WHERE filters and both are built. The split says so where it picks the grains |
| 5. buckets the partition skips | `COMPONENT` buckets untouched. The carried-only split and the signature check are in the table above |
| 6. where the regraft root is decided | untouched |

### `_members_of` is not the output set

The last handoff listed it among four readers that "look like they want the
output set". With it reading `output_concepts`:

| test | what happened |
|---|---|
| `tests/engine/test_duckdb_subset_join_pivot_axis.py` (two tests) | a keyless join, raised by `join_resolution` |
| `tests/complex/test_rowset.py::test_basic_expression_over_rowset_output_keeps_scoped_join_keys` | 4 CTEs to 5 |

It has four call sites (the accumulated-atom columns, `_filter_intrinsic_pushdown_safe`'s
`supplied`, and two on a ROOT's re-source at FINAL). Which of them needs the
members was not separated; the docstring records the failing test.

## Two planner bugs fixed on the way

Both are on `main` (`329c662f3`) and both come from the entity split. Model:
customers (`bal`, `name`), orders complete on `customer_id`.

| statement | before | cause |
|---|---|---|
| `where count(order_id) by customer_id > 1 and bal > 150 select customer_id, count(order_id) as n` | `WHERE input(s) ['local.bal'] cannot be related to the query outputs` | `bal` was peeled onto `dim:customer_id` alone. No output reads that scan, so it is outside the main lineage and placement refuses the atom. With `name` selected beside it the same statement planned |
| `where bal > avg(bal) by * select customer_id, count(order_id) as n` | `Missing source reference to local.bal` | `bal` feeds a condition-phase aggregate, so the condition scan holds it, and placement counts what a condition scan holds as readable by the host. The split had peeled it off the row stream the host reads |

Two rules in `_split_root_dimension_clusters`:

- a cluster with no output among its members is not peeled;
- a root a condition stage reads is not peeled. This is the partition's own
  sentence, "a private scan of its roots, beside the row stream that keeps
  them", which the split was breaking.

Neither moves a corpus plan. Guards: `test_filter_argument_fd_on_the_grouping_key`.

## Found and left

Each is on `main` too. Model for the first two: `_MODEL` of
`tests/core/processing/test_v4_root_partition.py`, with or without the `~`.

1. **Wrong rows, silently.**
   `with r as select user_id, state, line_id, sale_price; select r.user_id, r.state, sum(r.sale_price) as revenue;`
   returns user 1 twice. With one of the two dimensions selected it is right.
   The FINAL contract asks for grain `r.user_id` and the merge sits at
   `(local.user_id, r.line_id)`, but `check_if_group_required` answers no:
   `r.state` is keyed at the rowset's row grain, so the target grain built
   from the outputs covers the merge grain. The aggregate's own grain is
   `r.user_id`, so the two disagree about what `r.state` is a function of.
2. **A plan error.**
   `with r as select user_id, line_id, sale_price; select r.user_id, state, sum(r.sale_price) as revenue;`
   raises the keyless-join guard: a rowset handle beside a base attribute of
   the same entity.
3. **A lead, rows unverified.** With the nested-signature guard off (I had it
   off by accident for one corpus run) tpc-ds q36 loses a CTE, 3 to 2.

## Open items

1. **The election still ranks.** `elect_extent_owners` gives a span to its
   `OWN` domain and ranks the exposing groups for every other kind. The kinds
   are known at the partition now, so each could name its owner there:
   `ROW_STREAM` and `BOUNDARY` have an obvious one. `PADDED` and the regions
   only a join of two facts witnesses (tpc-ds q64) would still rank.
2. **`PADDED`.** Owner's direction on the rollup: a materialized rollup
   probably needs concepts of its own, which would then have a domain of their
   own, and it is marginal enough to take separately. "Materialized rollup"
   here is a summary table holding an aggregate at a coarser grain (thelook
   `sales_agg`), not GROUPING SETS. The other `PADDED` reason, a WHERE atom no
   host can apply to a domain, is tpc-h q22's shape, rows-verified on the
   padded plan (`tests/engine/test_scalar_aggregate_over_region.py`).
3. **The entity split as a sourcing decision**, tpc-ds q98, as the last
   handoff left it.
4. **`COMPONENT` buckets** get no domain and no split. Still nothing in the
   suite says whether that is a rule.
5. **The regraft root** is still decided on the group graph.
6. **`anchor_keys` is set on some peels and `dim_keys` on all.** A peel carries
   its key only when it holds an argument of a projected scalar. Making every
   peel carry it is a behavior change that was not tried.

## Tools

- `local_scripts/sql_ab` as before. Run the suite capture in a detached
  worktree at a commit, never in the shared tree.
- `local_scripts/plan_debugger/trace_query.py <file>`: the "region domains
  added" step lists the domains.
- Guards: `tests/core/processing/test_v4_root_partition.py`.
