# **P0** q30 scoring hang: keyless FINAL merge cross-joins two group parents (5.7B-row fan-out)

## Symptom

Offline scoring of `evals/tpcds_agent/results/20260813-125008_ingest/workspace/query30.preql`
timed out after 180s. The run's own agent hit the identical hang live:
`agent_log.q30.jsonl` event 32/33 shows its `trilogy file write answer_2802535988.preql --run`
dying with `trilogy error: subprocess timed out after 600s.` (2026-08-13T13:48 -> 13:58),
after which the agent burned its remaining budget on rowset workarounds (probe7 then hit
"query could not be planned; this is a bug") and the session was killed at the 900s wall.

Not a planner loop and not a slow reference: `generate_sql` returns in 0.34s and the
reference SQL runs in 0.05s. The generated SQL contains two `INNER JOIN ... on 1=1`
cross products in the final SELECT and does not finish in 120s.

## Reproducible-on

`2435237d4` (branch `more-benchmarking`, post-rebase onto main)

## Timing table

| Step | Time | Notes |
|---|---|---|
| engine setup (`make_scoring_engine`) | 2.05s | workspace + tpcds extension |
| `generate_sql(query30.preql)` | **0.34s** | 1 statement, 8,591 chars; rules out (A) |
| execute generated SQL (DuckDB, db copy) | **>120s, killed** | agent-side run also killed at 600s |
| reference `tests/modeling/tpc_ds_duckdb/query30.sql` (same db copy) | **0.05s** | 100 rows; rules out (C) |

## Minimal repro / EXPLAIN excerpt

Repro: score the candidate file with the standard harness (kill it externally):

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
eng = scoring.make_scoring_engine(db_copy, workspace, 'tpcds')
sql = eng.generate_sql(open(workspace / 'query30.preql').read())[-1]  # 0.34s
# duckdb.connect(db_copy, read_only=True).execute(sql)  # hangs
```

Final SELECT of the generated SQL (CTE names from the run):

```sql
FROM "abhorrent"                            -- GA customer detail, 5,266 rows
  INNER JOIN "juicy" on ... customer_sk ... -- qualifying totals, 3,403 rows
  INNER JOIN "late"  on 1=1                 -- distinct GA (address_sk, state), 2,446 rows
  INNER JOIN "macho" on 1=1                 -- distinct last_review_date date_sk, 367 rows
GROUP BY 1..13, ...
```

`abhorrent JOIN juicy` matches 6,373 rows; the two keyless joins multiply that to
6,373 x 2,446 x 367 = **5,720,927,386 rows** feeding a 14-column HASH_GROUP_BY
(11 of them strings). DuckDB EXPLAIN shows two stacked `CROSS_PRODUCT` operators
with an estimated ~2.03B rows into the group-by. Besides being a hang, the result
would also be wrong: the final projection reads `macho`'s date_sk, pairing every
customer with all 367 distinct review dates.

`late` and `macho` are group-by rewrites of the same GA-filtered customer source as
`abhorrent`; `abhorrent`'s CTE even still renders both bridge columns
(`..._customer_address_address_sk`, `..._last_review_date_date_sk`) but they are
node-level hidden, so join inference cannot pair on them.

## Root cause file:line

Causal chain, innermost defect first:

1. `trilogy/core/processing/v4_helper/group_graph.py:1401` `_final_merge_grain`
   returns an EMPTY grain for this query shape: the `sum(...) by (customer_sk, state)`
   vs `1.2 * avg(...) by state` comparison lives entirely in the WHERE scope, so no
   FINAL predecessor carries a GROUPING derivation and no mandatory output advertises
   a grouping grain. `final_merge_grain` at `strategy_builder.py:3282` unions the
   contributor contracts to the empty set (verified by instrumentation:
   `_wrap_for_grain` receives `merge_grain_components=frozenset()`).

2. `trilogy/core/processing/v4_helper/strategy_builder.py:2546` `_wrap_for_grain`
   (called from `_assemble_final_node` at :3371 with that empty grain): BOTH of its
   anti-cross-join guards are gated on `merge_grain_components` being non-empty:
   the orthogonal-key early return at :2582 and the FK-hop collapse at :2635. With
   the empty grain each needed concept buckets to its natural self-grain, and the
   GroupNode at :2669 emits `late` (grain {address_sk}) and `macho` (grain {date_sk})
   WITHOUT the spine key `customer_sk`. The FK-hop guard's own collapse condition
   provably held here (`last_review_date.date_sk` has `keys={customer_sk}`, and the
   parent's usable outputs include `customer_sk`), so a non-empty merge grain would
   have kept the join key. This is exactly the failure mode the :2630 comment
   ("loses its join key and degrades to ON 1=1") documents, unreachable because of
   the empty-grain gate.

3. `trilogy/core/processing/join_resolution.py:693` `get_node_joins` builds its
   ds/concept pairing graph only from VISIBLE outputs (hidden concepts skipped at
   :740-741), so the still-rendered-but-hidden bridge columns on `abhorrent` cannot
   rescue the join, and `resolve_join_order_v2` silently emits `keys={}` joins for
   the disconnected parents, rendered as `on 1=1` by `trilogy/dialect/common.py:329`.
   There is no "keyless non-constant join at FINAL" error path.

## Verdict

**(B) framework bug, P0.** `generate_sql` is fast; the planner emits SQL whose FINAL
merge cross-joins two keyless group parents, producing a 5.7B-row intermediate that
DuckDB cannot finish in minutes (agent run killed at 600s, offline scoring at 180s,
local probe at 120s). The reference query is 0.05s, so scoring itself is fine. Fix
belongs in the planner (empty `final_merge_grain` neutralizing `_wrap_for_grain`'s
join-key preservation), with a secondary hardening candidate in join resolution
(refuse or bridge a keyless FINAL join between non-constant parents).

## RESOLVED 2026-08-15

Fixed as a category ("FINAL contributor loses its join axis -> keyless merge"),
not as a point patch. The candidate now plans in 0.2s and executes in 0.12s
(clean two-way join on `customer_sk`; the `late`/`macho` cross-join parents are
gone entirely).

Planner changes (all in the passes that IDENTIFY required keys — the contract
and demand layers — not in late re-injection):

1. `group_graph._group_final_grain_contribution` + new `_lineage_pinned_grain`:
   a non-grouping FINAL contributor whose outputs ride a fixed-grain barrier
   (BASIC rename/derivation of an aggregate OR of a rowset member) advertises
   that barrier's grain as its projection grain, so the assembly-side merge
   grain can no longer collapse to empty. Authored scoped-join relation
   members are excluded (their authored keys stay the axis; q59 fan-out).
2. `strategy_builder._wrap_for_grain`: the FK-hop collapse now resolves its
   axis in priority order (supplied merge grain, else the parent's own grain,
   else the concept's own keys, each intersected with what the parent can
   supply) and asks the concept-map FD closure `build_fd_determines` whether
   the concept is determined by it. Two prior gaps: it demanded the parent
   carry the WHOLE merge axis (q30), and it inferred FD via
   `BuildGrain.from_concepts`, which folds the property hierarchy only, so an
   enum declared `key city` and bound by a `grain (tree_id)` source never
   folded even though it carries `keys={tree_id}` (boston_multi_enum).
3. `group_graph._compute_concept_sets`: sibling-grain comparison and grain-key
   capability now resolve rowset-namespaced grain keys (`rs_a.grp_key` ==
   `local.grp_key`), and a rowset boundary's base grain keys count as
   exposable capability.
4. `v4_node_generators/rowset.py`: aggregate rowsets expose their grain key
   when the inner producer actually renders it (per-key `in produced` gate
   replaces the blanket aggregate exclusion).

All four are demand/contract-layer fixes: the passes that decide which keys a
node must expose now identify them correctly. Nothing re-injects a key
downstream of node construction.

### Category guard

`join_resolution._raise_if_keyless_row_bearing_join` raises
`UnresolvableQueryException` whenever a keyless join lands between two
row-bearing sources **that share a PROJECTABLE join axis**: an address one
side actually emits, reachable from the other side's outputs through the FD
closure over `keys`, pseudonyms and rowset content, after canonicalization.
That is precisely "the axis existed, the planner could have joined on it, and
it dropped it."

The projectable qualifier is what makes the check sound. A join key must be a
column the source really renders, so the axis comes from output concepts, not
from grain components. Hidden outputs DO count (they are rendered, just
masked, which is exactly how q30 lost its key); a grain component the source
never emits does NOT (a merge at grain `{name, value}` projecting only
`{value, dim}` has no `name` column to pair on).

Hard failure by design, not a warning: shipping the cartesian silently is
wrong results plus an unbounded fan-out. If it ever fires, the fix belongs
upstream in the demand/contract passes that lost the axis, never in relaxing
the check.

These stay legal: row-independent sides (no grain; an authored literal
fan-out; all-single-row outputs, the existing
`utility.calculate_graph_relevance` rule; or a KEYLESS global-aggregate
scalar), and axis-DISJOINT row-bearing sides, since an aggregate selected
without its grouping key is an authored cross join. A KEYED metric with a
self-grain is a per-group aggregate whose grain is mislabelled, so it stays
checked.

Precision: **10 of the 141 corpus queries legitimately render `ON 1=1`, and
the guard fires on none of them**; it fires on all three real bugs when run
against the pre-fix planner.

An earlier revision of this section claimed the full suite had zero triggers.
That was wrong: it was written after roughly two thirds of the suite. The
root-level `tests/test_*.py` and `tests/persistence` chunk held two more
triggers, both genuine pre-existing bugs, both since fixed (see "Two further
latent bugs" below). The suite is clean now, verified across every chunk.

One trap in that chunk, recorded so it is not re-diagnosed: `tests/cli`
reports 57 failures when the chunk runs as a whole, and none of them are the
guard. They are a pre-existing collection-time `conftest.py` import
interaction, reproducible at HEAD and visible with `-k test_cloud` (where only
cloud tests execute and still fail). `tests/cli` passes alone and in small
combinations. Split it out when bisecting planner work.

### What the guard caught

THREE more live wrong-results bugs of the same category, all previously green
because no test validated rows:

1. `rowset_alias_collision`: 9-row cartesian instead of 3.
2. The distinct-aggregates rowset collision: wrong totals paired with wrong
   keys.
3. `boston_multi_enum` (tests/modeling/geography): `city` and `usbos_source`
   both carry `keys={tree_id}`, each got deduped to its own grain (dropping
   `tree_id`), and the merge paired every tree with every enum value. Its test
   asserted only that two table names appear in the SQL, so a data-corrupting
   cartesian passed CI.

All three are fixed by the same principle. The lesson: `test_v4_parity_cases`
checks PLANNING STATUS only, and several modeling tests assert on SQL text
rather than rows, so a query that plans into a cartesian and returns garbage
stays green. Any future fix in this area needs row-asserting tests.

### Two further latent bugs (found by the root-level chunk)

Both pre-existing at HEAD — verified by running a worktree holding HEAD's
planner with only the guard added, where both fire with identical node
identifiers.

4. **Dead condition-source contributor.** For a ROOT contributor, filter-only
   WHERE args are appended to `group_concepts` so the fresh scan can source and
   apply the condition. That same list was then handed to `_wrap_for_grain`,
   which buckets by natural grain: `wr.date_dim.date` bucketed to `{date_sk}`
   and became a second `GroupNode` projecting nothing any consumer reads and
   sharing no key with the real projection, so the merge cross-joined it. Rows
   stayed correct only because a `GROUP BY` above collapsed the fan-out — an
   incorrect plan that happened to be neutralized. Fix: pass
   `group_concepts` minus the filter-only additions to `_wrap_for_grain`; the
   condition is already applied inside the node.

5. **Key-dropping PERSIST through `_cross_component_source`.** That fallback
   exists for the `sum(samt) + sum(wamt)` shape: components related only
   through a derived expression's lineage, with no key relationship, each
   collapsing to a scalar, where a cross product genuinely is the answer. It
   was gated on `_lineage_connected` alone. `PERSIST split_only FROM SELECT
   generic.split` materializes a table with no `scalar` column even though
   `split` carries `keys={scalar}`, so `select split, scalar` cross-joined two
   ROW-BEARING components. Fix: refuse the cross product when one component's
   addresses fall in another's `build_fd_closure` — a key relationship makes
   the join mandatory. Confirmed wrong results: 4 rows became 8 with two scalar
   rows. `tests/persistence/test_complex` missed it because its fixture holds
   exactly one scalar row, where a cartesian and a join agree.

### Tests

`tests/core/processing/test_v4_grouping_alias_merge_grain.py` (distilled q30,
three shapes, row-asserted), `tests/core/processing/test_join_keyless_guard.py`
(guard matrix: shared-axis and FD-key and keyed-metric shapes raise;
axis-disjoint, unprojected-grain, empty-grain, single-row, keyless-scalar and
rollup-padded shapes stay legal),
`tests/complex/test_rowset.py::test_rowset_alias_collision_rows` /
`::test_rowset_alias_collision_distinct_aggregate_rows` (row-asserted),
`tests/persistence/test_persist_lossy_source.py` (two-scalar-row fixture, the
one the original persistence test lacked),
`test_union_arm_subset_join_full_grain.py::test_arm_filter_arg_is_not_a_merge_contributor`.

Corpus A/B vs a clean HEAD worktree: 4 tpc-ds queries changed plan (q59, q65,
q79, q98 — join keys preserved / redundant dedups dropped); all pass the
executing tpc_ds suite (169 passed) and tpc_h (30 passed).

Note the corpus is **not** a sufficient gate for this area. It stayed
byte-identical across every fix added after the first pass, including the two
regressions below, because no corpus query exercises rowset or scoped-join
shapes. Root-level `tests/test_*.py` is what caught them.

### Two regressions these fixes caused, and their fixes

Both were caught by the root-level chunk, not the corpus.

- `rowset.py` grain-key exposure re-exposed a key that an already-exposed
  handle covers, publishing a second name for the same value. Two sibling
  rowsets over one base then appeared to share a join axis, which silently
  outranked an authored scoped join on a derived key
  (`agg.period + 53 = fut.period`) and re-typed a subset `LEFT` to `FULL`.
  Fix: skip a key already covered by an exposed handle's content.
- `_compute_concept_sets` rowset-grain resolution volunteered ROLLUP grouping
  keys as a join axis, dropping every subtotal row. Fix: exclude
  `_rollup_padded_addresses(environment)`. The trap: at demand time the rowset
  members are still STANDARD aggregates, so
  `wrapper.grouping.nulls_grouping_keys` is `False`; the spec lives on
  `concept.lineage.rowset.select.grouping.mode` (narrow with `isinstance`
  against `SelectLineage` — `UnionSelectLineage` has no `grouping`).

### Acceptance

`scoring.score_query(engine, workspace, 30)` now returns in **0.39s** (was: killed
at the 180s timeout; the agent's own run died at 600s). The P0 hang is closed.

## FOLLOW-UP (separate defect, pre-existing): NULL-extended customer rows

The candidate now scores in under a second but still grades `fail`, for a cause
unrelated to the merge grain. The customer-side CTE is byte-identical before and
after this fix except for two dropped columns — same joins, same WHERE:

```sql
FROM "date_dim" ...
  FULL JOIN "customer" on d_date_sk = c_last_review_date_sk
  RIGHT OUTER JOIN "customer_address" on c_current_addr_sk = ca_address_sk
WHERE ca_state = 'GA'
```

`RIGHT OUTER JOIN customer_address` preserves every GA address, including
addresses that are no customer's `c_current_addr_sk`. Those rows carry a NULL
customer, survive the GA filter, and then pair with the aggregate's own NULL-key
row through the FINAL `is not distinct from` join. Result: **6,220 of 6,373
output rows have a NULL `customer_id`**, so the `nulls first` ORDER BY fills the
LIMIT 100 with all-NULL rows.

The navigation here is `customer -> customer.customer_address.state` — a property
hop that should be CUSTOMER-preserving. Preserving toward the address side is the
defect. This belongs to the preserving-join/nullability family (cf. the open q78
`is_returned` INNER-collapse record), not to the keyless-merge category fixed
above, and it has a much wider potential blast radius — worth its own A/B.
