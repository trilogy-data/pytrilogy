# Open optimization items

Two independent items, both found by tracing
`tests/modeling/thelook_duckdb/adhoc04.preql`. Neither changes adhoc04's
rendered SQL. Each removes work the planner or optimizer does that it
shouldn't need to. They can be implemented separately.

## Status: both done (2026-09-29)

**Item 1.** `_region_contract_join` escalates to FULL only when the stream
joined against the holder holds another family (`_holds_another_family`): the
side being added when the holder is already joined, everything joined when
the holder is the one being added. `get_join_type` takes the joined set from
`resolve_join_order_v2`. adhoc04 plans `left outer` / `full` and the upgrade
pass no longer fires on it. SQL moved in two places, thelook query19 and
query21: a join typed LEFT at plan time renders its key off the preserved
side, so `coalesce(users.id, fact.user_id)` became `users.id`. Rows match the
reference SQL. Guards: `tests/core/processing/test_v4_region_join_plan_time_typing.py`,
`test_region_join_escalates_only_over_another_familys_rows`.

**Item 2.** The fold is decided without the folded group's node, but not
before Stage 3: whether a ROOT parent groups is source planning's call
(`datasource_nodes.py`), and a graph-only prediction got 30 of ~350 fold
decisions wrong over the planner suites. `_aggregate_inlines` reads the graph
and the group's *parents'* nodes, which are built by the group's turn. The
consumer uses it to re-root (`_inline_aggregate_inputs`); the build loop uses
it to take a group every reader inlines out of the graph, the contracts and
the extent routing before building it (`_inlined_by_every_reader`,
`_fold_into_readers`). Measured against the old node-reading test over the
planner suites: 343 folds, 0 disagreements. No SQL moved.

Census over tpc_h, tpc_ds_duckdb and thelook_duckdb: 107 dead of 851 built
before, 75 of 819 after. Of the 44 dead groups the fold explained, 32 are no
longer built. The 12 that remain:

| count | why it is still built |
|---|---|
| 7 | condition-phase (`d1`) group: its node is what vetoes its `d*` twin's fold (`co_materialized`). Left unbuilt, the twin folds too and q44/q64 re-plan (q44 came out 250 chars shorter, rows right): a follow-up, not a planning-only change. |
| 4 | a BASIC read by a FILTER that is itself folded (tpc_h q14, tpc_ds q50/q62/q99): at the BASIC's turn its reader is not an aggregate, and the FILTER's own fold needs the BASIC's node. |
| 1 | tpc_ds q72: the FINAL reads outputs the group exposes and its readers do not carry. |

A reader that finds a folded input unreadable raises
(`_raise_if_inlined_input_is_unreadable`).

---

# Item 1: plan-time FULL on a region join the planner already knows is LEFT

### Problem

`tests/modeling/thelook_duckdb/adhoc04.preql` renders a FINAL of the form

```
users LEFT JOIN <fact spine> ON user.id  FULL JOIN products ON product.id
```

The SQL is correct, but the LEFT is **not** chosen by the planner. Before
optimization the plan is FULL/FULL. `UpgradeOuterFromKeySetEquivalence`
(`trilogy/core/optimizations/value_set_join_upgrade.py`) then narrows the
user join. Its log line:

```
[Optimization][UpgradeOuterFromKeySetEquivalence] uneven: full → left outer on declared-subset full-match between abundant and cooperative
```

The planner had everything it needed to type this LEFT itself. The goal is
to type it correctly at plan time, so the optimizer's upgrade pass has
nothing to do here.

### Root cause

`_region_contract_join` in `trilogy/core/processing/join_resolution.py`
(around line 541) decides both FINAL joins. Traced facts:

| pair (left x right) | holder | families in merge | result |
|---|---|---|---|
| `users` x fact spine, on `user.id` | users (left) | 2 (`{user.id}`, `{product.id}`) | FULL |
| fact spine x `products`, on `product.id` | products (right) | 2 | FULL |

The FULL comes from this branch:

```python
families = {
    region for region in facts.region_partition if region & facts.held_spans
}
if (
    _unpaired_value_nulls(keys, holder.held_spans, feeder, holder)
    or len(families) > 1
):
    return JoinType.FULL, False
```

The comment says "the rows already joined carry its extension rows (NULL on
this key): only FULL keeps them". But `len(families) > 1` counts every
family in the **merge**, not the ones **already joined** when this pair is
typed. It treats every region join as though another family's padding were
already present.

`get_join_order` (same file, around line 1042) calls
`get_join_type(left_candidate, right, ...)` with `left_candidate` taken
from `eligible_left`, the accumulated side, and `right` the side being
added. So:

- **Holder on the accumulated (left) side** (pair 1: users is the first
  source; the fact joins onto it). LEFT preserves the whole accumulator,
  including any other family's padded rows already in it. The feeder's
  unmatched rows are members nobody referenced. **LEFT is sufficient
  whatever the family count.** `_unpaired_value_nulls` still vetoes as it
  does today.
- **Holder is the side being added (right)** (pair 2: products joins onto
  users+fact). The accumulator carries the user family's extension rows,
  which are NULL on `product.id`. RIGHT would drop them, so **FULL is
  needed, but only if another family's holder is already in the
  accumulator.** Otherwise RIGHT is sufficient.

### Fix (narrow)

Make the `families > 1` veto depend on join order:

1. If the holder is `left`, never escalate on family count.
2. If the holder is `right`, escalate to FULL only when a holder of a
   *different* region is already in `eligible_left`.

This needs the joined set, which `get_join_type` doesn't currently receive.
One option is to pass `eligible_left` (or the set of regions already joined)
into `get_join_type` / `_region_contract_join`. Another is a field on
`JoinFacts` that the caller updates as it goes. Choose whichever is
smaller. Don't add a separate post-pass.

Keep `get_join_type`'s docstring accurate: region-contract typing is already
directional at plan time, so this doesn't conflict with its
"row-preserving by default, narrowing pass restores" note. It removes an
over-broad escalation.

### Out of scope

- The second FINAL join (products) must stay FULL.
- Don't touch `UpgradeOuterFromKeySetEquivalence`. It stays as the general
  narrowing pass; this just stops handing it a join we already knew.
- Don't make the join-order change of reusing the `products` read from the
  cost-carrying peel. That was discussed and dropped.

### Reproduce

Trace (the viewer shows "CTEs before optimization" with `uneven` FULL/FULL):

```
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql --rows --setup tests.modeling.thelook_duckdb.db_build:seed
```

Per-rule typing trace (script `jt.py` below, run from the repo root). This script wraps `get_join_type` and the rule
helpers, and prints each `user.id`/`product.id` pair with the side facts:

```
.venv/Scripts/python.exe <path-to>/jt.py <out.trace.json>
```

`jt.py`:

```python
import sys, runpy
sys.path.insert(0, ".")
import trilogy.core.processing.join_resolution as jr
from trilogy.core.enums import JoinType
names = ["_region_contract_join", "_extent_free_join", "_partial_domain_join", "_nullable_join"]
for n in names:
    orig = getattr(jr, n)
    def wrap(*a, _o=orig, _n=n, **k):
        r = _o(*a, **k)
        keys = a[2]
        if any("user.id" in x or "product.id" in x for x in keys):
            print(f"[jt] {_n} {a[0]} x {a[1]} keys={sorted(keys)} -> {r}")
        return r
    setattr(jr, n, wrap)
orig_g = jr.get_join_type
def g(l, r, keys, facts):
    out = orig_g(l, r, keys, facts)
    if any("user.id" in x or "product.id" in x for x in keys):
        f = facts
        print(f"[jt] get_join_type {l} x {r} keys={sorted(keys)} -> {out}")
        for side in (l, r):
            s = f.side(side)
            print(f"      {side}: partials={sorted(s.partials)} complete={sorted(s.complete_spans)} nullables={sorted(s.nullables)} grain={sorted(s.grain)}")
        print(f"      extent_free={sorted(f.extent_free_keys)} demanded={sorted(f.demanded_domains)} held={sorted(f.held_spans)}")
    return out
jr.get_join_type = g
sys.argv = ["trace_query.py", "tests/modeling/thelook_duckdb/adhoc04.preql", "--rows", "--setup", "tests.modeling.thelook_duckdb.db_build:seed", "--out", sys.argv[1], "--no-html"]
runpy.run_path("local_scripts/plan_debugger/trace_query.py", run_name="__main__")
```

### Acceptance

- adhoc04 pre-optimization `uneven` joins are `left outer` (users x fact) and
  `full` (x products). The `UpgradeOuterFromKeySetEquivalence` line for
  `uneven` no longer appears.
- Final SQL for adhoc04 is expected to be **byte-identical** (the optimizer
  already produced this shape). An unmoved `zquery<N>.log` after a modeling
  run is a valid SQL A/B.
- Add a focused test asserting the plan-time (pre-optimization) join type,
  for example by building the CTEs without optimization, or with that one
  rule disabled. The rendered SQL alone can't show the fix.
- A/B the planner suites (thelook_duckdb, tpc_ds_duckdb, tests/engine,
  join_matrix). Any SQL diff needs a rows check: a plan that now types LEFT
  where it previously relied on FULL to keep padded rows is a wrong-rows
  risk, not a cosmetic change.
- `ruff check . --fix`, `mypy trilogy`, `black .`

---

# Item 2: decide aggregate-input folding on the group graph, before building

### Problem

In adhoc04, `grp:basic:d*:local.id:sig:1940c2ff9524` (the BASIC computing
`item_margin <- sale_price - product.cost`) is built and then never read.
Its only consumer, `grp:aggregate:d0:local.id|order.id:input:local.id`,
folds it: the aggregate attaches to the BASIC's parent (the
`root:root:dim:local.id` peel) and computes `item_margin` inline. The
aggregate's MergeNode outputs `local.item_margin` directly off the peel. The
rendered SQL is what we want. The BASIC's build is wasted planning work.

```
.venv/Scripts/python.exe local_scripts/plan_debugger/dead_groups.py --detail tests/modeling/thelook_duckdb/adhoc04.preql
# thelook_duckdb/adhoc04.preql p0(d0) grp:basic:d*:local.id:sig:1940c2ff9524 basic consumed
```

This is the "inlined" class from the dead-group census: 81 of the 168
dead groups, and the largest class. adhoc04 is a small, clean instance.

### Root cause

Stage 3 builds groups in topological order (`_topological_order`,
`trilogy/core/processing/v4_helper/strategy_builder.py`, around line 2631).
The fold is decided later, while the **consumer** is being built:
`_parent_nodes_for`, the `Derivation.AGGREGATE` block around lines 749-830.
It loops over the parent candidates and replaces a "row-preserving input"
parent with that parent's own parents when `_group_renderable_from` says the
parent can be computed from them.

The fold test combines graph-level checks with checks on the parent's
**built node**. The built node is the only reason the parent has to be
built before the fold can be decided.

### Fix

Decide the fold on the group graph, before Stage 3. Then remove foldable
groups: rewire each consumer to the folded group's parents, and never build
the folded group. Keep the current topological build order. Don't turn the
build into a top-down/on-demand build as part of this; that was considered
and set aside.

Here is each condition in the current test, and its graph-level equivalent:

| current check | reads | graph-level equivalent |
|---|---|---|
| `attrs[pgid].derivation in ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS` and not ROOT | attrs | already graph-level |
| `not _provider_feeds_other_grouping(...)` | graph + attrs | already graph-level |
| `primary_members ⊆ _aggregate_row_preserving_input_addresses(...)` | attrs + env | already graph-level |
| `not (pgid_outputs & nonstandard_key_addresses)` | attrs | already graph-level |
| `not _group_filter_has_existence(...)` | attrs + env | already graph-level |
| `node.conditions is None` | **built node** | the group's condition placements (`attrs[pgid].condition_atoms` / the placements from `_inject_conditions`), which are decided before building |
| `not node.force_group` | **built node** | follows from the group's attrs/derivation. Confirm which attrs drive `force_group` on a BASIC's node. |
| `not node.existence_concepts` | **built node** | the group's EXISTENCE-kind in-edges (`edge_kind(..., EdgeKind.EXISTENCE)`) |
| `not _contains_shape_barrier(node)` | **built node** (GroupNode/WindowNode or `force_group` anywhere in the subtree) | follows from the derivations of the parent groups. Per the owner, this is easy to determine from the parent's node type. |
| `not co_materialized` | **`built` dict**: another depth-1 group whose *built* outputs overlap | overlap of `primary_members` / computed output sets between depth-1 groups on the graph |
| `_group_renderable_from(attrs, env, pgid, available)`, with `available` = outputs of the **built** grandparent nodes | **built nodes** | outputs of the grandparent groups from the concept sets computed at "FINAL added, concept sets computed" (group_graph phase) |

Notes:

- The fold is **iterative** (the `while True` loop): a folded group's
  parents can themselves be folded. A graph-level version must reach the
  same fixed point, for example by rewriting until nothing changes.
- `co_materialized` and `available` are the two checks where the built node
  may differ from the graph's claim (re-sourcing, slicing, regraft). If one
  can't be matched exactly, keep that single check at build time as a veto,
  and still skip the build whenever the graph-level test succeeds. A veto
  that fires after the group was skipped means the prediction was wrong. It
  must fail loudly, not silently build the group then. See the "no silent
  skip in a delivery pass" feedback.
- The fold happens inside the aggregate's build today, so removing the group
  from the graph also changes what later passes see:
  - FINAL contributor contracts (the BASIC appears as a contributor in
    adhoc04's FINAL contract)
  - extent ownership
  - existence wiring
  - `_provider_feeds_other_grouping` for other consumers

  Make sure a removed group is gone from all of them, not just unbuilt.
- The viewer and `dead_groups.py` should show the group as never built, or
  removed, rather than built but not in FINAL.

### Out of scope

- A top-down/on-demand build order (a possible follow-up once the census
  shows what remains dead).
- The other dead-group classes (existence lineage, unread roots, unbuilt).
- Any change to the rendered SQL. This item is planning work only.

### Acceptance

- adhoc04: the BASIC group is never built. `dead_groups.py --detail` no
  longer lists it. Rendered SQL is byte-identical.
- Corpus census before and after, over the planner corpora (tpc_h,
  tpc_ds_duckdb, thelook_duckdb):
  ```
  .venv/Scripts/python.exe local_scripts/plan_debugger/dead_groups.py "tests/modeling/tpc_h/*.preql" "tests/modeling/tpc_ds_duckdb/query*.preql" "tests/modeling/thelook_duckdb/*.preql"
  ```
  Report how much of the "consumed"/inlined bucket goes away, and explain
  anything that remains.
- **SQL must not move anywhere.** This is a pure planning-work change, so
  any `zquery<N>.log` diff or other SQL diff is a bug in the prediction.
  Unmoved zquery logs after a modeling run are the A/B.
- Planner suites (thelook_duckdb, tpc_ds_duckdb, tests/engine, join_matrix)
  pass. Planning-time timing logs are expected to move, ideally down.
- `ruff check . --fix`, `mypy trilogy`, `black .`

---

## Environment notes (both items)

- Another session may be running suites in the main tree (benchmark
  artifacts were being modified during this investigation). Work in a
  worktree, and don't run concurrent pytest runs against the same tree.
