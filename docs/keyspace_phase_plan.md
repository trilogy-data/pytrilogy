# Project plan: a keyspace phase in discovery

Written 2026-09-20, from scratch, for a fresh session. It proposes one new analysis in v4 discovery and a strangler migration onto it. Nothing here is built. Background: `docs/handoff_extension_row_semantics.md` (the rule this serves, and a prototype that was backed out), `docs/extent_ownership.md`, `docs/domain_graph_design.md`.

## The problem

Every statement has a **row universe**: the kinds of row it can return, and which entities exist on each kind. Customers and orders, not every customer has an order, `select customer_id, status`: two kinds of row. One has a customer AND an order. One has a customer and no order.

Nothing in discovery computes that. Instead about a dozen places each re-derive the fragment they need, from stamps (`Modifier.PARTIAL`, `partial_concepts`, `nullable_concepts`) and from the shape of whatever was already built:

| where | the fragment it answers |
|---|---|
| `partial_bridging.heal_pinned_partials` (before discovery, rewrites datasources) | does this WHERE empty a kind of row? |
| `join_resolution.licensed_extension_spans`, `extent_ownership.spans_demanded_by` / `span_members` | which extra kinds of row does the statement ask for? |
| `extent_ownership.elect_extent_owners` (END of `build_group_graph`, after groups exist) | which group produces them? |
| `group_graph._keep_extension_families_together` (inside the dim peel) | two kinds of extension row must not pair |
| `MergeNode._extent_free_partials`, `join_resolution.extension_padded_addresses` / `_span_padded_addresses`, `get_join_type` | which NULLs here are absence, and whose? |
| `strategy_builder._cover_groups_for_mandatory` | which group holds a column's value on which rows? |
| `strategy_builder._fold_passthrough_parents` / `_drop_ancestor_parents` / `_fold_covered_contributors` | can this contributor go? (asked of COLUMNS; a contributor of ROWS is invisible to it) |
| `condition_placement._preserved_final_branch`, `_uncovered_exposing_output_contributor` | will a join re-admit rows this WHERE removed? |
| `optimizations/predicate_pushdown`, `join_upgrade`, `null_safe_join` | is this side null-extended, and is that NULL a value? |

They disagree at the edges, and every disagreement is a wrong answer or a lost filter. The derived-concept key-domain rule (a derived concept is NULL where its key's entity is absent) could not be implemented in one place because there is no place: the prototype needed five new predicates and three `keep=` parameters, each restating "which rows, which entities" locally, and each suite failure was one more component with a different implicit answer.

Both design docs already say the same thing from the other side: `domain_graph_design.md` and `v4_network_discovery_design.md` each have a "nullability is not modeled" section. `DomainGraph` relates VALUE SETS of concepts across the model. What is missing is its statement-level counterpart.

## The idea

A **keyspace**: computed once per plan, before grouping, from the requested keys.

- A **region** is one kind of row: a set of entity keys that are PRESENT on it. Customers/orders gives `{customer, order}` and `{customer}`.
- The statement's universe is a set of regions. They are disjoint by construction (the Venn diagram's cells, not its circles).
- A concept is **defined** on a region when every one of its FD-minimal keys is present there; elsewhere it is ABSENT, which renders as NULL but is not a NULL value.
- An aggregate is evaluated OVER regions (that is why `count(order_id) by customer_id` is 0, not NULL, on `{customer}`); a row-stream derivation is evaluated ON the regions where it is defined.

Shapes it has to get right, all pinned by tests today:

| model fact | regions |
|---|---|
| `orders.customer_id: ~customer_id`, customer key demanded | `{customer, order}`, `{customer}` |
| same, customer key NOT demanded (join axis only) | `{order}` (what `spans_demanded_by` decides today) |
| items bind `~user_id` and `~product_id` | `{item, order, user, product}`, `{user}`, `{product}`. There is no `{user, product}` cell: "families never cross-pair" is this, stated once |
| `returns` binds `~order_id, ~item_id` as its OWN grain, `lines` binds both complete | one region `{order, item}`. `_ret_order` is a NULL VALUE there, `is_returned` evaluates to false (tpc_ds q94) |
| `orders.customer_id: ?customer_id` | one region; the NULL key is a value on a real row |
| `by rollup (k)` | subtotal rows are a grouping product, not a region |
| WHERE null-rejects a concept absent on a region | that region is empty for this statement (pin-heal, as a query) |

The region test, as far as the prototype got it: an extension row of a `~` key comes from a source binding that key completely, and carries whatever that source binds beside it. So an entity is present on the region iff some such source binds its key. "The span does not FD-determine it" is NOT the test (it breaks q94).

## Where it sits

`concept_strategies_v4._build_from_graph` is already a three-stage pipeline: `build_concept_graph` -> `build_group_graph` -> `build_strategy_node`. The keyspace is a stage between the first two, handed to the other two and to FINAL assembly. `_build_from_graph` recurses (rowset bodies, WHERE-phase sub-plans), so a keyspace is per PLAN, not per statement: a sub-plan's requested keys include its grain, which is exactly what made a statement-level gate fire inside tpc_h q20's sub-plan in the prototype.

Inputs: requested keys (output grain, aggregate grains, WHERE row args), `DomainGraph` (bindings with `~`, FD edges, subset/equal/incomparable), the WHERE.

What it owns: regions, presence, and the questions in the table above, as queries.

What it does NOT own: join rendering, source search, aggregate argument inlining (a stage-3 rendering choice: an aggregate re-derives its BASIC arguments from the root instead of reading the BASIC's group, which is why padding must never happen inside one), and the optimizer. Those consume its answers.

## Migration (strangler; SQL stays byte-identical until phase 4)

Every eager structural change to root grouping has broken something (see the memory notes on the dim peel, the filter-only bridge, the merged unnest bridge). So the first three phases change no plan.

0. **Read discovery first.** `source_planning.py`, `network_search.py`, `network_build.py`, `network_obligations.py` were not read when this was written. The plan assumes the source search consumes partiality through `extent_free_spans` and stamps only; confirm or correct that before designing the data structure.
1. **Read-only analysis.** Build the keyspace, attach it to `BuildInfo`, render it in the debug trace. No consumer. Unit-test it directly against the shapes table above: this is the first time those rules have a spec that is not a SQL string.
2. **Agreement assertions.** Under a test-only flag, each scattered site asserts its answer equals the keyspace's: demanded spans, the elected owner, pin-heal's healed set, `_extent_free_partials`. Run the full suite. Each disagreement is either a keyspace bug or a latent planner bug; triage them before moving anything. This phase is the real test of the idea and it is cheap to abandon.
3. **Move the questions, keep the answers.** Replace each site's derivation with a keyspace query, one site per commit, `zquery<N>.log` blob-hash A/B proving no SQL moved. Pin-heal stops rewriting datasources. The election stops running last.
4. **Place derivations by region.** A row-stream derivation is computed on a stream covering only regions where it is defined, unless it is NULL-propagating there anyway. Regions declare their join keys up front, so `_compute_concept_sets` demands them as hidden columns. This is where the key-domain rule lands, for every shape at once; it is what the prototype could only do behind gates (one span, span key projected, deliverable WHERE).
5. **WHERE by region.** An atom is evaluated on the regions where its inputs are defined; a region it null-rejects is empty; a null-accepting atom is evaluated after the regions are united. Replaces `_preserved_final_branch` and its relatives.
6. **Rows versus columns in the builder.** A contributor carries a region contract; the three folds check it instead of each growing a `keep=`.
7. Delete what is left: the padding-provenance matrix, `_keep_extension_families_together`, the span plumbing on `BuildEnvironment`.

## Status

Phases 0-2 are built (2026-09-20, same branch). Everything below this heading is a record of what they found; the sections above are the original plan, unedited.

### Phase 0: what the source search actually consumes

The plan's assumption was half right. The network search (`network_model`, `network_build`, `network_search`, `network_obligations`, `network_coalescing`, `network_topology`) reads NO span, NO `extent_free_spans`, NO `DomainGraph`, NO `NULLABLE`:

- `network_build._candidate` reads `datasource.partial_concepts` once and turns it into one `BindingStrength.FULL | PARTIAL` bit per binding. That bit is the whole vocabulary (it is also the entire Rust FFI surface for partiality). `non_partial_for` + `condition_implies` (`ConditionFit.IMPLIED_EXACT`) can promote a partial binding to full per request, and `network_coalescing.downgrade_axis_bindings` MINTS partiality for coalescing axes with no stamp behind it.
- Spans reach the source planner only ambiently: `source_planning` builds `MergeNode`s without passing `extent_free_spans`, and `MergeNode.__init__` defaults it to `environment.extent_free_spans`, which `build_strategy_node` sets per group. That side channel is what a phase 3 has to inventory.
- `plan_source` runs in stage 3, per group, after grouping. A pre-grouping keyspace is upstream of it and does not have to serve it.
- The search re-derives row-POPULATION facts (`_row_complete`, `_grain_classes`, `_drop_dominated_arms`) but never entity presence. The stronger partiality reasoning is in the legacy single-scan path (`select_helpers/source_scoring.py`, reached through `_direct_source`) and `select_node_v2.scan_stamps`, the only `NULLABLE` reader.
- There is no model-level index of address -> {complete binders, partial binders}; `SourceNetwork.full_binders` is per request and already query-conditional. The keyspace builds its own, cached per environment (`keyspace._model_facts`).

### Phase 1: the keyspace (`v4_helper/keyspace.py`, `models.Region` / `models.Keyspace`)

Built between `build_concept_graph` and `build_group_graph`, attached to `BuildInfo.keyspace`, logged when a plan has more than one region. No consumer. `tests/core/processing/test_keyspace.py` pins every row of the shapes table.

How regions are computed, since the plan left it open:

- **Entities.** A KEY is its own entity (its `keys` are an FK-path artifact: `customer_id` bound on `orders` lists `order_id`). Anything else resolves through its keys recursively, because an aggregate's keys are its `by`, which can be a property (`count(order_id) by status` is keyed on the order) or a ROLLUP grouping marker.
- **Regions are generated by sources.** A datasource row carries the keys it binds plus what a keyed lookup from them reaches (whole grain only, never through a source whose own grain is bound `~`). Each carried key remembers the `~` bindings on its path, its CAUSE; empty means the rows span the key's domain.
- A source whose carried keys cover a proper subset of the entities witnesses a smaller region. It survives only when every source carrying MORE reaches its identifying keys through a `~`; those keys are the region's `spans`. That one rule gives: the demanded/not-demanded split (a join-axis-only key is not an entity, so its source projects to nothing), q94 (`lines` and `returns` carry the same entities, so one region), `?` (complete binding, one region), and "families never cross-pair" (no source carries exactly `{user, product}`).
- An entity no source relates to a region's rows is cross-joined onto them, so it is present there.
- `emptied_by`: a WHERE null-rejecting a concept that is absent on a region empties it. This follows the RULE, so a derived concept counts (`status = 'delivered'`); pin-heal only trusts bound columns.

Not modelled: a lookup that may miss (a source whose grain is bound `~` and that binds a further key makes that entity OPTIONAL on the row: the same bug class from the other side), `complete where` partitions, and regions only a join of two facts can witness (the base region stands in).

### Phase 2: the audit (`v4_helper/keyspace_audit.py`)

Inert unless `TRILOGY_KEYSPACE_AUDIT=<file>`; then each plan appends a JSON line per disagreement. Three checks: `demand` (the election's `demanded_extension_spans` against `Keyspace.output_demanded_spans`), `heal` (pin-heal's healed set against the emptied regions of a keyspace replayed over the pre-heal datasources), and `owner_pads` (members of a span's owner group that are absent on the span's region: what is evaluated over padded rows today, i.e. phase 4's worklist, not a disagreement).

## Guards

- `tests/engine/test_derived_key_domain.py`: the oracle (materialization invariance: storing a derivation as a column at its grain must never change a query's rows), `OWED` strict xfails as targets, inline-vs-named spellings, and three traps (`?` key, ROLLUP subtotal, present entity with an unbound property).
- `tests/engine/test_duckdb_partial_key_assembly.py`, `test_duckdb_partial_fk_field_report.py`, `test_multi_fact_nullable_fk_extent.py`, `tests/optimization/test_join_upgrade.py`.
- `tests/modeling` SQL-size budgets and `zquery<N>.log` hashes: the byte-identical check for phases 1-3.
- Process notes in the handoff still apply (one pytest at a time, `-v` to a log file, never `stash`/`reset` in this tree).

## Questions for the owner

1. Is a region keyed by ENTITY keys only, or by concepts? (Entity keys is the proposal; a property is never a witness, tpc_ds q70.)
2. Is `complete where` a region fact (a partition is a region of its key) or does it stay with the union machinery?
3. Does the keyspace live on `BuildInfo` (per plan) with `DomainGraph` staying model-level, or does `DomainGraph` grow a statement overlay the way scoped joins did (`with_overlay`)?
4. `select status, count(customer_id)`: the customer key is demanded only as an aggregate ARGUMENT. Under the region model the `{customer}` region exists and the orderless customer counts under `status = NULL`, which is what the materialized twin returns. Confirm that is the intended answer, since no output carries the key.
5. `undelivered_customer <- filter name where undelivered` in a SELECT restricts the row stream today (returns only customer 1, on the materialized twin too). Under the region model it is a value, NULL where the predicate fails. Confirm, because that is a visible behaviour change outside `~` models.

## What already exists to start from

The prototype (commits `e0f6424b7`..`d1c482137` on `extension-row-null-semantics`, backed out in the commit after them) implements phase 4 for ONE region shape and passes the oracle for it. Read it as evidence, not as a base:

- it proves the target plan renders and executes: `customers LEFT JOIN (derivation over orders) ON customer_id`, the dim-peel shape;
- its predicates are keyspace queries in embryo: `absent_on_extension`, `_null_on_padding`, `_solid_groups`, `_filters_span_domain`, `reads_rows_only`;
- its failures are the requirements list: q94 (presence is not FD), q95 (an atom the extension rows never see is a lost filter), the three folds (rows are not columns), sub-plan firing, SQL budgets tripping on NULL-propagating arithmetic.
