# Handoff: graph-time extent ownership (never build the dead extent branch)

## OPEN 2026-08-21, design work for a fresh agent

Framing, agreed with the user: this is a LOGICAL PLAN DEFECT. The planner
emits a plan that, read as a whole, contains a join whose rows the same
plan provably discards. That should never survive logical planning. It
survives today because no planner layer ever reads the whole plan: extent
routing is an EMERGENT side effect of bottom-up build order, projection
accidents, and a final-assembly election that runs over already-built
nodes. The first layer that holds both the producer and its consumers is
the CTE rule loop, so the whole-plan analysis currently lives there.

The field-report residual stitch was therefore fixed in the optimizer
(PR #653, docs/handoff_field_report_residual_stitch.md): a
consumer-invisibility proof narrows the padded FULL to LEFT, then
`PruneInvisibleOuterJoins` deletes the now-dead join and its subtree. That
fix is correct, stays as a safety net, and is NOT the end state. This
handoff is the fundamental relocation: make the logical planner OWN the
extent-routing decision, so the dead subtree is never manufactured.

## Why no node can catch it locally, precisely

Do not misread the prune's trigger. "No rendered reference + right side
unique on join keys" makes a LEFT join dead, but the planner builds the join
as FULL, and a FULL join with zero projected columns is NOT dead: its
right-padding rows carry the union extent the `?`/`~` contract demands of a
self-contained branch. The join only becomes LEFT (and therefore dead) via a
consumer-side proof: the final assembly consumes the metric branch as the
right side of a plain-equality LEFT join on `item_id` from the full-extent
anchor (`concerned`), so padded rows are output-invisible.

That consumption mode is decided AFTER the branch builds. The build order
today is: group graph -> build every group bottom-up -> final-assembly cover
(`_cover_groups_for_mandatory` in strategy_builder.py) elects contributors
and span owners FROM THE BUILT NODES -> join inference renders the final
merge. The election depends on what each built branch happens to expose
after projection narrowing (`_project_basic_aggregate_inputs` drops
`user_id` from the field-report metric branch, which routes the licensed
span to the dim bucket). So the killing fact is derived downstream of the
thing it should have killed. The optimizer is simply the first layer that
holds both ends today.

Hard evidence that no branch-local shortcut exists (all reproduced
2026-08-21, do not re-derive):

- The field report and the forked model
  (tests/engine/test_duckdb_partial_key_assembly.py, `_FORKED`) render the
  BYTE-SAME merge shape mid-plan: a dim-peel span joined FULL with
  null-safe pairs on `(order_id, product_id)`, `product_id` a licensed `~`
  key, zero mandatory concepts contributed. In the forked plans the final
  assembly consumes that merge via `INNER ... item_id IS NOT DISTINCT FROM`
  and projects `item_id` FROM it: the padded rows are load-bearing. In the
  field report it consumes via plain-equality LEFT: the padded rows are
  dead.
- A naive plan-time drop (remove an aggregate parent whose outputs are
  address-covered by a sibling) was prototyped via monkeypatching
  `_parent_nodes_for`: the field report renders clean and row-validates
  940/940, but `test_forked_with_state` breaks with a BINDER error (the
  downstream election assumed the contributor existed). Address subsumption
  is not extent subsumption, and divergence between a predicted election
  and the actual one is exactly this binder-error class.
- Both aggregates carry the same bucket label (`grp:aggregate:d0:local.item_id`),
  so no bucket-shape discriminator exists either.

## Where the dead branch comes from (traced)

In the field report, `_split_root_dimension_clusters` (group_graph.py) peels
`order_id`/`product_id` (FD-determined by `item_id`) into a dim cluster
`grp:root:root:∅:dim:local.item_id`. Group planning then hands that cluster
to the aggregate-input merge as a parent WITHOUT its entity key, so
`get_node_joins` can only stitch it member-to-member on the padded dim keys
(the `cheerful` CTE: `products LEFT (distinct order_id, product_id from
items)`, joined FULL null-safe). The peel design intends dim buckets to join
the FINAL merge on their entity key; entering an aggregate-input merge
keyless is the smell. Trace tooling: monkeypatch `_parent_nodes_for` to
print `(gid, needed, parents)` and `MergeNode.__init__` to print parent
output signatures.

## The design: elect extent ownership on the flow graph, propagate down

Invert the dependency. Instead of branches building self-contained extent
and the election discovering afterwards which copy is consumed:

1. At graph time (concept_graph/group_graph, where buckets, licensed keys,
   FDs, and the statement grain are already known), elect ONE owner per
   licensed `~` span: the bucket whose rows deliver that key's extension
   members to the output (normally the dim span bucket).
2. Annotate every other bucket as extent-free for that span. Parent
   selection for those buckets then never demands the dim-peel span
   contributor; the `cheerful`-class subtree is never built.
3. The final-assembly cover CONSUMES the annotation instead of re-deriving
   ownership from built outputs. One source of truth is the load-bearing
   requirement: every fallback path (generator skips, satisfiability
   pruning, `satisfiable_outputs`) must re-enter the decision, never
   silently elect differently, or you get the binder-error class above.
4. Join inference at merges inside extent-free branches then has no padded
   contributor to stitch, and the final merge joins extent-free branches on
   solid keys with plain equality by construction.

The prize is larger than the field report: the forked reunion stitches
exist ONLY because today's election lets the same extension member be
manufactured in two branches, which then must reunite null-safely. Under
single ownership a member is manufactured exactly once, and the reunion
machinery (`family_anchored` in `_gate_nullable_by_host`, the
padding-provenance matrix, `PruneInvisibleOuterJoins` itself) should decay
toward dead code. Audit that by firing counts across the corpus, then
simplify.

## The constraint that makes ownership per-OUTPUT, not per-key

`test_forked_with_status` pins `order_status = 'LATER'` on both extension
rows. `order_status` is a fact-grain CASE
(`amount = user_first_amount ? 'FIRST' : 'LATER'`): computed IN the
row-bearing branch it evaluates on manufactured rows (NULL = NULL is not
true, so ELSE fires); read across a plain-equality LEFT join from a span
owner it would arrive NULL. So a fact-grain computed attribute demanded on
extension rows forces those rows through the branch that computes it, or
forces the expression to be re-evaluated post-join. The ownership election
must do this delivery analysis per demanded output (lineage and derivation
are on the graph). Expect this to be the hard part of the design.

## Constraints, all must stay green

- tests/engine/test_duckdb_partial_fk_field_report.py with the
  whole-statement assert (never regress it back).
- tests/engine/test_duckdb_partial_key_assembly.py: every row pin,
  especially `test_forked_with_status` / `test_forked_full_column_set`.
  These pin ROWS, not SQL shape, so a plan that routes extent differently
  passes as long as delivery is handled.
- tests/engine/test_multi_fact_nullable_fk_extent.py (the `?` contract,
  q98 shape test included), tests/core/processing/test_join_padding_provenance.py,
  tests/join_matrix, tests/generators/test_utility.py, tests/optimization.
- Fuzzer `.venv/Scripts/python.exe -m local_scripts.fuzzer` (228 cases,
  `padding_provenance` family especially).
- Corpus A/B against the CURRENT tree, three legs in ONE process with a
  no-op control (scratchpad harness pattern in
  docs/handoff_field_report_residual_stitch.md); row-validate every changed
  query via the modeling suites. A real footprint is EXPECTED here: this
  change should delete extent branches and stitches corpus-wide.
- Never run two pytest processes at once, and run nothing else that writes
  the repo while the modeling suites run (timing logs collide).

## Success criteria

- The field-report plan never contains the `cheerful`/`highfalutin`
  subtree at any layer (verify at node-build time, not just rendered SQL).
- Forked rows unchanged, including `order_status` on extension rows.
- Firing counts for `family_anchored` keeps and `PruneInvisibleOuterJoins`
  drop to (near) zero across the corpus; then simplify or retire them in a
  follow-up, keeping the prune rule as a safety net until the counts prove
  it dead.
