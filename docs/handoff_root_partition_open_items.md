# Handoff: the root partition, open items

## Status: picked up (2026-09-29)

What was done with this list is in `docs/handoff_region_domain_kinds.md`.
Everything after this section is the original handoff and describes the code
as it was.

| item | outcome |
|---|---|
| 1. `_keep_extension_families_together` | reads region domains. Every demanded region has one, with a `DomainKind`. The merge branch is reachable and guarded |
| 2. `carried_keys` | a property over `grain_components`, `anchor_keys` and `carried_spans`. `_members_of` stays on the members: the output set broke two tests |
| 3. the entity split as a sourcing decision | untouched |
| 4. composite keys, d0 only | d0 only is right, with a statement that says so |
| 5. buckets the partition skips | the carried-only split is handed the domains, the signature check asks the derivation. `COMPONENT` untouched |
| 6. the regraft root | untouched |

Two claims below did not hold. "The merge branch ... never fired" is true of
the suite and not of the rule: two families peeled off two keys reach it.
`_members_of` is not one of the readers that want the output set.

---

Follows `docs/handoff_grouping_pass_structure.md` (status section at its top).
The four root passes and their cleanup are now one pass,
`root_partition.partition_root_demand`
(`trilogy/core/processing/v4_helper/root_partition.py`), in one order:

| decision | reads | makes |
|---|---|---|
| existence | the condition stages' roots | the row stream without the roots that only define a semijoin set |
| region | the whole root demand, the keyspace | `…:extent:<span>`, reason `REGION` |
| entity | the region domains | `…:dim:<key>`, reason `ENTITY` |
| condition | the condition stages' roots | `grp:root:root_d1:…`, reason `CONDITION` |

Every ROOT group carries a `RootReason` (`models.py`). The regraft's root is
`…:basic_input:<key>`, reason `BASIC_INPUT`, still decided on the group graph.

Older docs (`keyspace_phase_plan.md`, the dim-peel and existence handoffs)
name the code as it was:

| then | now |
|---|---|
| `add_region_domain_buckets` | `region_domains.decide_region_domains` |
| `carry_spans_to_condition_scans`, the span loop of the domain pass | `region_domains.carry_region_spans` |
| `_drop_unread_peels_the_domain_took` | `root_partition._domain_holding`, inside the split |
| `_add_d1_root_buckets` | `root_partition._condition_scans` |
| `_attach_secondary_members`, `secondary_members` | `_carry_grain_keys`, `carried_keys` |
| `grp:root:root:dim:<key>` (the regraft's) | `grp:root:root:∅:basic_input:<key>` |
| trace steps "root dimension clusters split", "region domain buckets added", "d1 roots and secondary members attached" | "existence-only roots taken off the row stream", "region domains added", "entity clusters peeled", "condition scans added, spans carried", "grain keys carried" |

What follows is what was looked at and left, in the order I would take it.

## How every claim here was measured

`local_scripts/sql_ab` (README there), against a detached worktree at
`d1dc8a876`:

- **Corpus SQL** (20s): every SELECT of `tests/modeling/**`,
  `tests/engine/**`, `tests/complex/**`, `tests/discovery/**` and the plan
  debugger examples, 202 statements.
- **Suite SQL** (20 min): every statement any test compiles, per test id,
  about 8,500, compared per test as a set.

Both normalize CTE names by order of appearance. The README lists the tests
that move between two runs of the same code.

## 1. `_keep_extension_families_together` still reads the keyspace

With the domains decided first, the rule's un-peel has a simpler reading: a
member a region domain carries is peeled onto that region's span or not at
all. I ran that reading in shadow beside the real rule over the whole suite
(it computes, logs a disagreement, and lets the real rule act):

| firings | agree | disagree |
|---|---|---|
| 16 | 14 | 2 |

All 14 are the un-peel (`user_id`, and `state` with it, peeled onto
`order_id` beside a `{user_id}` domain). The merge branch, where the carrying
clusters are rekeyed by the union of their keys and stay peeled, never fired
in the suite or the corpus.

The 2 disagreements are
`tests/optimization/test_full_join_lowering.py::test_nullable_plain_equality_key_is_refused_with_remediation`
and `::test_suggested_null_reject_actually_unblocks_lowering`: a demanded span
(`ocust`) under an authored coalescing relation. Such a region gets no domain
by design, so the domain reading has nothing to ask, and the keyspace reading
still un-peels `cname` from `cid`.

So the rule stays as it is. To simplify it, two things need deciding:

1. Whether the merge branch is reachable. Nothing covers it. If it is dead,
   the rule is an un-peel and nothing else.
2. What to ask when a demanded region has no domain. "Carried by a domain, or
   a member of a demanded span whose region has none" covers all 16 firings,
   but the second half is the keyspace read again.

The tpc-h adhoc07 plan is the corpus guard for the un-peel: without it the
plan joins `orders` a second time at FINAL (5 joins to 6).

## 2. Stage 3 still reads `carried_keys`

Condition placement now reads what the demand pass has a group emit
(`GroupBucket.carried`, written back by `_compute_concept_sets`). The early
list differed from the output set in 299 of 1,062 corpus buckets. No corpus
SQL moved; three suite plans did, because a group that emits a column as a
hidden pass-through can now host an atom over it:

| test | what moved |
|---|---|
| `test_duckdb_partial_key_assembly::test_forked_with_state` and `…_and_brand` | `user_id is not null` is hosted on the `dim:order_id` peel too, and the WHERE beside the inner join on `user_id` is gone. 41 characters shorter |
| `tests/modeling/geography::test_exact_match_merge_preserves_subgraph_filters` | `species = 'Oak'` reaches the enrichment scan as well as the aggregate's. 47 characters longer, same CTEs and joins |

Rows are asserted in the first two and pass. The third asserts the filter on
the aggregate branch, which is still there.

Five readers in `strategy_builder.py` and one in `extent_ownership.py` still
read the early list:

| reader | asks |
|---|---|
| `_members_of` (two FINAL call sites) | what a group holds |
| `_select_addresses` | the fallback when the demand pass left no outputs |
| `_condition_twins` | which condition groups carry a member |
| `_consumer_reads` | what a group reads off its parents |
| FINAL candidate sort | whether a group owns an address or passes it on |
| `elect_extent_owners.rank` | the same, for a span |

The last two ask about ownership, which the output set cannot answer: every
exposing group emits the span. They are the real users of a named
`carried_keys`. The first four look like they want the output set. Each is a
one-line change; A/B them one at a time, since `_consumer_reads` decides
whether a filtered parent may drop a column.

`_anchor_scalars_to_dim_peel_key` finds a peel's key as
`carried_keys - region spans`. The peel only carries its key when the cluster
holds a projected scalar argument. `dim_keys` says the same thing without the
subtraction, but it is set on every peel, so switching is a behavior change:
the anchor would fire for peels it skips today.

## 3. The entity split is still a sourcing decision made on FD

With the split off entirely, 18 corpus plans get worse (up to 9 more joins,
tpc-ds q30) and one gets better, tpc-ds q98. So the split earns its place,
and q98 shows what it costs: the row stream reads `item` anyway, for the
`category` filter and the `class` grouping key, and the peel reads it again
at FINAL for `id`, `desc` and `current_price`. Left on the row stream the
plan is 3 CTEs and 3 joins instead of 4 and 4, rows matching the reference.

I tried that as a rule of the split and took it back out (`2a6e85a5a`,
`652d82d66`, reverted in `11b01e26d`). The rule: a cluster stays on the row
stream when a member the bucket keeps is bound by one table alone and that
table binds the whole cluster. Over the suite it moved four plans:

| plan | CTEs | joins |
|---|---|---|
| tpc-ds q98 | 4 to 3 | 4 to 3 |
| `test_multi_fact_nullable_fk_extent::test_value_nullable_attribute_does_not_degrade_solid_key_joins` (q98 again) | 4 to 3 | 4 to 3 |
| `tests/modeling/mobs::test_mobs_query_discovery` | 1 to 3 | 2 to 2 |
| `test_persist_projection_matrix[sibling_aggregates_wide]` | 8 to 9 | 4 to 5 |

In the two that got larger the cluster does not ride the aggregate as a
GROUP BY passenger the way q98's does. It reaches FINAL off the scan in a CTE
of its own. In mobs the aggregate hosts a filter over a derived value
(`log_length_bin`); in the persist case the evidence was `item_count`, a
measure input that is also a `group(...) by` key. A bound on each spared that
plan and nothing else, which is the carve-out pattern the original handoff
describes. Rows were right in all four; this is shape only.

What decides the outcome is whether the consumer can carry the cluster
through its GROUP BY, and grouping does not know that. Two places do:

- **Source planning**, which is the original question 1. It sees the tables.
- **The optimizer**, after the plan exists: a FINAL join to a dimension table
  on its key, where a CTE below already joined the same table on the same
  key, can be folded into that CTE. That is general, and it cannot make a
  plan larger.

The eight carve-outs listed in the original handoff are untouched.

**Materialized concepts.** Not reproduced as a split problem. With a summary
table binding `name`, `tier` and a materialized `total` at `customer_id`
beside the base tables, `select customer_id, name, tier, total, max(amount)`
reads the summary once and joins the aggregate to it. A plain
`select customer_id, name, tier, total` has no grouping bucket, so the split
does not run. One thing seen on the way and not followed:
`select tier, sum(total) as tier_total, count(order_id) as n` recomputes
`total` from `orders` instead of reading the summary's column.

## 4. Composite keys use d0 grains only; single keys use every depth

Single keys must: tpc-ds q11 has only a condition-phase aggregate by
`customer.sk`, and restricting single keys to d0 costs it a CTE and two
joins. The split's docstring now says why.

Letting composite grains come from every depth too moves no corpus plan, so
nothing says which is right. A statement with a condition-phase aggregate at
a two-key grain and outputs FD on that grain would.

## 5. Buckets the partition skips

`RootReason.COMPONENT` buckets (several disjoint row streams in one scope)
get no region domain and no entity split, as before: both passes used to skip
any bucket with a discriminator. Nothing in the suite says whether that is a
rule or an accident of how the check was written.

`split_carried_only_row_streams` splits BASIC buckets by region. It runs
after the partition and reads the domains, so it is ordered correctly, but it
is a fifth partition that lives outside the pass.

`_can_merge_nested_signatures` (`group_rules.py`) still asks a group id
whether it starts with `grp:root`. A stop-signature holds ids, not buckets,
so asking the reason needs the bucket map passed in.

## 6. Where the regraft root is decided

`_synthetic_dimension_regraft_parent` needs a BASIC's `input_concepts`, which
the demand pass computes, so it cannot move into the partition as it stands.
The root it makes is read by the aggregate after the BASIC folds
(`_aggregate_inlines`), so it is needed whether or not the BASIC is built.
