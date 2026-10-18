# Handoff: computing a dimension-keyed derivation on the dimension's scan

Branch `discovery-fold-bridge-colocate` fixes the species-bucket fanout
(`fanout_repro_pr747.preql`) as correctness bugs in the fold, the bridge and the
partial stamps. Co-locating the bucket on the dimension scan was prototyped on
the same branch, measured, and taken out again: the planner side worked, but the
optimizer undid the benefit and the two corpus queries it touched (tpc-ds q62,
q99) only grew. This records what was built, what it measured, and what is left
so the feature can land on evidence.

## The shape

`fit` is a species-grain `auto` over two enrichment columns; the statement counts
trees by `fit`. Today `fit` is computed inline on the tree rows after the join to
`enrichment`. Computed on the enrichment scan instead, the CASE runs once per
species and the trees join a two-column projection on `species`, a key of that
side. tpc-ds q62/q99 (`substring(warehouse.name)`, `lower(call_center.name)` as
grouping keys) are the same shape.

The mechanism already exists: `_synthetic_dimension_regraft_parent`
(`group_graph.py`) builds a `BASIC_INPUT` root bucket keyed by the derivation's
grain. Its gate requires the key to be a grouping grain of some aggregate, so a
FINAL merge column exists. In the repro the aggregate groups by `fit`, not
`species`, and the gate never fires.

## Planner side, prototyped and working

Relaxed gate: the original condition OR all of

- `_readers_pair_on`: every lineage reader of the BASIC has another ROOT row
  parent whose outputs include the key, so the dimension scan pairs with the
  stream on its own key;
- `_key_is_stored`: the key and every merge origin
  (`environment.merge_origins`) are ROOT. A key the stream derives (a window or
  aggregate merged into the dimension key) is NULL where nothing matches; the
  inline CASE over that row's NULL attributes takes its ELSE branch, a value
  computed on the dimension has no row to take. `tests/discovery/
  test_merge_derived_bridge.py[window]` is the lock; `environment.concepts.get`
  returns the canonical substitute, so the derivation has to be read off
  `alias_origin_lookup`;
- `_is_row_key`: the key does not determine every column the stream reads
  (environment FD, `build_fd_determines`; the concept-graph FD misses FK chains
  such as the line's part to the part's type), or the "dimension" is the fact
  table again (`sale_price - cost` at the order item, 14 corpus queries);
- `_stream_reads_only_for`: no other reader of the stream, no output and no
  WHERE argument needs the inputs (`reserved = output_addresses |
  condition_arg_addresses`, threaded from `build_group_graph` because condition
  atoms are injected after the regraft); otherwise the stream carries them anyway
  and the dimension read is a second read (`select fit, sum(zone_min)`);
- `_dimension_table_binds`: a datasource with grain within the key binds every
  input; a key with no table of its own (q99's two date keys) could only be
  scanned by grouping the fact rows.

Plumbing that the bucket needs and that is still on the branch (zero corpus
footprint on its own): `anchor_keys` covers `BASIC_INPUT` (`models.py`), a dim
peel always emits its anchor key (`_group_inputs`), and a BASIC keeps its
parents' anchor keys in `needed` (`strategy_builder._build_group`). Without
these the dimension scan dropped its key and the keyless-join guard fired.

## Why it was taken out

With the gate on, q62 planned as: fact stream (3 joins, WHERE), a warehouse
projection computing `w_substr`, an aggregate joining the two. The optimizer then

1. folded the projection into the warehouse scan (`collapse_single_parent`) and
   inlined that scan into the aggregate (`inline_datasource`), so the substring
   ran once per joined sale row, exactly as on main;
2. left the fact stream as its own CTE, since the aggregate now had two parents
   and `collapse_single_parent` folds only a joinless child with one parent.

Net: 2016 -> 2543 chars, same work. Not an improvement.

## Optimizer side, what is left

1. `inline_datasource`: do not fold a scan that computes a scalar (the fold
   plan renders a BASIC that is not a rename) into a consumer whose grain is
   not within the scan's. Prototyped: ten lines in `optimize`, corpus footprint
   zero without co-location, and with it the warehouse projection stays a
   five-row CTE and q62 is 2707 chars with the substring at warehouse grain.
2. A flatten for a row-preserving join CTE with one consumer that joins it as
   its base: hoist its datasources, joins and WHERE into the consumer. Not
   prototyped. `CTE.inline_parent_datasource` is the single-datasource version
   of this; the join CTE version has to carry `parent.joins`, append
   `parent.condition`, and repoint `source_map` entries for every datasource the
   parent read. Safe only when the consumer joins the parent INNER or as base
   (a WHERE hoisted out of the right side of a LEFT join drops padded rows).
   The alternative is at the planner: `_pre_merge_parents` could absorb a plain
   MergeNode parent's parents and joins into the merge it builds, which keeps
   the shape question out of the optimizer. This is the remaining 700 chars of
   q62 and the whole of main's single-select plan.

Evidence bar for landing: q62 and q99 at or under their main sizes with the
scalar on the dimension side, corpus otherwise byte-identical, fuzzer 260/260,
and the repro's three-CTE plan unchanged in rows and join count.

## Still open in the same family

A scalar determined by the aggregate's grouping key (`case fit when ...` beside
`count(tree_id)`) is read off the species or tree rows and the FINAL merge joins
it on `fit`, then regroups. Row-correct; `find_fanout_joins` flags the join.
