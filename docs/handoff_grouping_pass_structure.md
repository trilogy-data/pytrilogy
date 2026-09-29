# Handoff: how the grouping passes partition the root

Research notes from a discussion of the plan debugger's grouping steps, traced
on `tests/modeling/thelook_duckdb/adhoc04.preql`. No code changes yet. Each
item is a question to settle, and possibly a targeted change, before any
restructuring. All line numbers are `trilogy/core/processing/v4_helper/`
unless noted.

## Status: restructured (2026-09-29)

The passes are one pass, `root_partition.partition_root_demand`, in the order
existence, region, entity, condition. What is left is in
`docs/handoff_root_partition_open_items.md`. Everything after this section is
the original handoff and describes the code as it was.

| item | outcome |
|---|---|
| one pass with explicit reasons | done. Every ROOT group carries a `RootReason`; the passes ask it instead of a discriminator or `dim_keys` |
| B, domains before the split | done. `_drop_unread_peels_the_domain_took` is gone: a cluster the domain holds whole and nothing but FINAL reads joins the domain (`_domain_holding`). `_keep_extension_families_together` stays, see the open items |
| existence prune | runs first, on the row stream, instead of last on whatever the other passes made |
| A, the key criterion | audited. The split is a real optimization: off, 18 corpus plans get worse and one better (tpc-ds q98). Keys of every depth are right (tpc-ds q11). A rule for q98's shape was tried and taken back out, see the open items |
| A, materialized concepts | not reproduced as a split problem |
| C, `secondary_members` | renamed `carried_keys`, with the writers named on the field. Condition placement reads the demand pass's outputs instead |
| D, the regraft root | `grp:root:root:∅:basic_input:<key>`, reason `BASIC_INPUT`, built as a bucket like any other. Still created: the aggregate reads it once the BASIC folds |

One claim in the original text did not hold: "An aggregate at `k` that feeds
only a condition never puts `k` on the FINAL join." It does. The condition
group merges into FINAL on its grain; q11 is exactly this and needs the peel.

One dependency between the passes was not in the text. The domain pass's
materialized-aggregate veto relied on the split having run: the summary's own
columns keyed by the span had been peeled out of the bucket it tested.
Decided on the whole demand, the veto skips an aggregate the region carries
(`test_materialized_rollup_matches_its_base` caught it).

SQL moved in three tests, all from condition placement reading the output
set: two plans lose a WHERE an inner join already implies, one pushes a
filter to a second scan. No plan gained a CTE or a join. Everything else is
identical once CTE names are normalized, over 202 corpus statements and the
statements the test suite compiles.

The dead-group census is unchanged, 63 of 804 built: the peels the old
cleanup deleted were never built either.

Guards: `tests/core/processing/test_v4_root_partition.py`.

---

## Background: the passes today

`build_group_graph` (`group_graph.py`, around line 3290) runs, in order:

1. `_assign_groups`: one bucket per (derivation, depth, grain). All root
   columns go to one shared ROOT bucket, `grp:root:root:∅`. Traced as
   "buckets assigned".
2. `_split_root_dimension_clusters` (`group_graph.py:983`): peels entity-FD
   clusters of the shared root into `grp:root:root:dim:<key>`. Traced as
   "root dimension clusters split".
3. `add_region_domain_buckets` (`region_domains.py:239`), then
   `_drop_unread_peels_the_domain_took` (`group_graph.py:935`): one ROOT
   bucket per live extension region, `…:extent:<span>`, holding the region's
   rows. Traced as "region domain buckets added".
4. The following, traced together as "d1 roots and secondary members
   attached":
   - `split_carried_only_row_streams`
   - `_add_d1_root_buckets` (private `root_d1` per condition stage)
   - `carry_spans_to_condition_scans`
   - `_prune_existence_exclusive_roots`
   - `_attach_secondary_members`
5. Later, in the group-graph phase ("FINAL added, concept sets computed,
   sources regrafted"): `_synthetic_dimension_regraft_parent`
   (`group_graph.py:3712`) can mint **another** `grp:root:root:dim:<key>`
   for a BASIC whose inputs are FD on its grain. adhoc04's
   `root:root:dim:local.id` comes from here, for `item_margin`, and not from
   step 2.

## Observation: these are all one decision

Steps 2, 3, 4 (d1 roots and existence pruning) and 5 all answer the same
question: **which root columns are sourced together, and for which
reader?** They partition the root demand by different criteria:

| pass | partitions root demand by |
|---|---|
| dim split (2) | entity FD key that is also a downstream grouping key |
| region domain (3) | extension region (padded rows vs solid rows) |
| d1 roots / existence prune (4) | consumer phase: condition stage, semijoin set |
| synthetic regraft (5) | a single BASIC's FD-on-grain inputs |

They run at three different points, and each has cleanup that exists because
an earlier pass got it slightly wrong:

- `_drop_unread_peels_the_domain_took` undoes step 2 peels that step 3
  subsumed.
- `_keep_extension_families_together` (`group_graph.py:886`) makes step 2
  read the keyspace, which is step 3's concern.
- The step 5 regraft root shares the `dim:` name with step 2 peels but has
  different semantics.

The long-term question is whether this should be **one "partition the root
by reader" pass** with explicit reasons (entity, region, condition stage,
existence set, BASIC input) rather than four passes and their cleanup. The
items below are the narrower questions to answer first.

## Item A: the dim split's key criterion is statement-wide and source-blind

`_grouping_keys` (`group_graph.py:777`) is the union of `grain_components`
over **every** grouping bucket, whatever its depth (d0, d1, d*) and whatever
it feeds. The split's justification (docstring) is "the FINAL merge already
produces that key as a join column". Grouping by the key somewhere is only a
proxy for that:

- An aggregate at `k` that feeds only a condition (d1) or a further rollup
  never puts `k` on the FINAL join, yet `k` still qualifies.
- The composite-key path uses only d0 grains. The single-key path doesn't
  filter by depth. Check whether that asymmetry is deliberate.

**Materialized concepts.** `materialized_roots` are ROOT leaves in the
concept graph (`concept_graph.py:220-260`, around 1341), so the split sees
them as ordinary members. The split decides FD against the environment but
never looks at which datasource binds what. When one summary table
materializes both the aggregate at `k` and the dimension columns, the split
can still send those columns to a separate dimension scan, adding a join the
source didn't need. This needs a repro. A thelook or tpc-ds query over a
`sales_agg`-style materialized source would be a good place to start
(`tests/modeling/thelook_duckdb/sales_agg.preql` exists).

**Location.** Choosing between the fact scan joined to the dimension and a
standalone dimension scan joined on the key is a *sourcing* decision. It's
made in grouping, before any datasource is known, and guarded by about eight
situational carve-outs:

- projected scalar args
- pre-aggregate filter args
- post-aggregate args
- finer-filter grains
- extension families
- the row key (`_peels_a_cluster`)
- grouping-dimension members
- the d0-grain FD check for filter-only args

Each carve-out is a case where the peel was wrong for rows or shape.

**Questions:**
1. Would the source network search (`source_planning.py`) make this choice
   better? It knows which tables bind the columns.
2. Is the split's benefit (avoiding re-rooting on the fact and deduplicating
   back to entity grain) still real now that FINAL typing and the
   grain-matched collapse exist?
3. Measure it: turn the split off (early return) and A/B the modeling
   corpora for SQL and rows. A wrong-rows diff shows what it protects. A
   pure-shape diff shows what it only optimizes.

## Item B: region domains before the dim split?

Region domains are probably the most common reason a root splits. Running
them **after** the dim split is why the cleanup
(`_drop_unread_peels_the_domain_took`) and the keyspace read inside the split
(`_keep_extension_families_together`) exist.

Two coupling facts to check before reordering:

- The domain pass reads peels. It takes a region member that any ROOT peel
  holds (`region_domains.py`, around 298-300: "a dim peel keyed by the span
  is the region's own rows already", "`brand` under `dim:item_id`"). Before
  the split, those members are still in the shared root, so the domain would
  need to take them from there.
- A peel **with a reader** stays even when the domain took its members. It's
  that reader's solid-side provider (`tier` under `tier_amount`, INNER on the
  fact), which the padded domain isn't. After reordering, the split must
  still create a solid-side peel when a non-FINAL reader exists, and skip it
  when only FINAL reads the members the domain now holds.

**Question:** with domains first, does
`_drop_unread_peels_the_domain_took` become dead code, and does
`_keep_extension_families_together` simplify? Verify with a full planner-suite
A/B of SQL and rows. See the class C dim-peel history (unread span-keyed peel
dropped, read peel kept).

## Item C: `secondary_members` means several things

The `_attach_secondary_members` docstring says secondary members are grain
components of GROUP-BY / PARTITION-BY buckets, "for visualization and the
condition-placement pass", and that BASIC groups get nothing, because
passthrough is derived later in `_compute_concept_sets`.

In fact the field is **written** in four places, each with a different
meaning:

| writer | adds |
|---|---|
| `_attach_secondary_members` (`group_graph.py:554`) | a grouping bucket's grain keys |
| dim split (`group_graph.py`, around 1176) | the peel's entity key |
| `region_domains.py`, around 377 and 413 | a region span a side or bucket carries |
| bucket merge (`group_graph.py`, around 3821 and 3831) | the union of the merged buckets' |

And it's **read** well beyond visualization:

- condition placement (`condition_placement.py` 495, 510, 518, 605, 1021)
- extent ownership (`extent_ownership.py:91`)
- concept-set capability (`group_graph.py` 2532, 2875, 2924, 2935)
- the strategy builder (`strategy_builder.py` 200, 705, 916, 2678, 3131)

Meanwhile the authoritative "what this group carries" set is
`output_concepts`, computed later by `_compute_concept_sets` (the demand
pass). That includes hidden passthrough columns, which never appear in
`secondary_members`. So there are two sources of truth for carried columns:
an early, partial one that most consumers read, and a later, complete one.

**Questions:**
1. Condition placement runs *after* concept sets are computed (the trace
   shows "condition placements" after "FINAL added, concept sets
   computed"). Could its readers use `output_concepts` instead?
2. Which readers genuinely need "carried but not computed here, known
   before the demand pass"? Condition placement's `final_exposable` and
   extent ownership look like candidates. Would those be better as a named
   field, for example `carried_keys`, than as a catch-all?
3. At minimum, fix the `_attach_secondary_members` docstring. Its claims
   about purpose and scope are out of date.

## Item D: the synthetic regraft root (step 5) and the aggregate fold

`_synthetic_dimension_regraft_parent` creates `root:root:dim:local.id` for
adhoc04's BASIC `item_margin`. The aggregate then folds that BASIC away (see
`docs/handoff_open_optimization_items.md`, item 2) and reads the regraft
root directly. If the fold is decided on the graph (item 2 there), decide
together whether a regraft root minted for a BASIC that will be folded
should be created at all, or attached to the consumer directly. Also
consider giving it a name distinct from step 2 peels, such as
`grp:root:root:basic_input:<key>`, so the viewer doesn't present two
different mechanisms as one.

## Tools

- Viewer: `local_scripts/plan_debugger/trace_query.py <file> --rows
  [--setup ...]`. The grouping steps diff each bucket against the previous
  pass.
- `local_scripts/plan_debugger/dead_groups.py` for census effects of any
  change.
- A/B any change by SQL (`zquery<N>.log` unmoved) **and** rows. Every change
  here touches which rows reach FINAL.
