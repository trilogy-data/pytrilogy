# Project plan: a keyspace phase in discovery

> **Where this stands (2026-09-21): phases 0-3 are BUILT, on branch `extension-row-null-semantics`, draft PR #702. Phase 4 is next. A fresh session should read "The problem" and "The idea" for the vocabulary, then jump to [Status](#status) and start from "Pick up here".** The sections before Status are the original plan, left unedited on purpose: where they say "nothing here is built" or list a site as re-deriving its own answer, Status is the truth.

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

Phases 0-2 are built and phase 3 is done: five sites moved (2026-09-20 and 09-21, same branch, PR #702). Everything below this heading is a record of what they found; the sections above are the original plan, unedited.

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

Not modelled: a lookup that may miss (a source whose grain is bound `~` and that binds a further key makes that entity OPTIONAL on the row: the same bug class from the other side), `complete where` partitions (as regions; site 5 does read `non_partial_for`, only to know that mutually exclusive slices are not each other's missing rows), and regions only a join of two facts can witness (the base region stands in).

Added by site 5, described there: `Region.witnesses` and `Region.completions`.

### Phase 2: the audit (`v4_helper/keyspace_audit.py`)

Inert unless `TRILOGY_KEYSPACE_AUDIT=<file>`; then each plan (sub-plans included) appends a JSON line per disagreement. Three checks:

- `demand` (deleted with the derivation it audited, once the election read the keyspace; `8d7b0d469`..`2fdbb632a` have it): the election's `demanded_extension_spans` against `Keyspace.output_demanded_spans`. Each election-only span also reported which sources the BUILT plan reads that bound it `~` (`read`); none meant the election's demand could not have changed anything.
- `heal` (deleted with the predicate it audited, once pin-heal read the keyspace: site 5): pin-heal must never heal a span whose region is live (`healed <= emptied`, over a keyspace replayed on the pre-heal datasources). Not equality: emptied is a fact about rows, healing also has to be safe for every other merge the dropped `~` would license, so pin-heal may decline (`test_anchor_needed_stays_partial`).
- `owner_pads`: BASIC / FILTER / WINDOW members of a span owner (or its descendants) that are absent on the span's region. Not a disagreement: it is phase 4's worklist.

Run it over the planner suites (`tests/core tests/discovery tests/engine tests/join_matrix tests/modeling tests/optimization tests/complex`, ~19 min; 3405 passed, every `zquery<N>.log` byte-identical).

#### What the first rounds changed in the keyspace

- **Completion spans.** The election also demands a span when no entity is absent: a source the plan NEEDS (it alone binds something requested) holds only some of a region's rows beside a source holding all of them (`returns` beside `lines`, a partial aggregate table beside its dimension). `Region.completes` records those `~` keys. "Needed" matters: TPC-DS `store_returns` binds `~item.sk, ~ticket_number` model-wide, and only a statement reading a returns measure is completed by it.
- **Demand is the election's question**, not "any output defined on the region": a span is demanded when some output is a function of what a keyed lookup from the span ALONE reaches (`Keyspace.span_reach`). TPC-H q20/q9 (`part.id` reaches no supplier: `partsupp` is many-to-many) and TPC-DS q05 (sale lines with no return are a region, but no output hangs off `item.sk`) separated the two.
- **A `~` survives a merge.** `merge a into b` respells the bound column onto `b` while `column_level_partial_addresses` keeps `a`; the binding is found through `origin_concept_address`. `partial_bridging._structural_partial` compares `column.concept.address` only, so pin-heal never heals a merged `~` key today (conservative, a lost optimization, not a wrong answer; gcat `payload.launch.launch_tag`).
- An existence-only node is a semijoin's subselect, not a row of the plan; spans are named by the address bound `~` (the spelling every other site uses), entities by the canonical spelling.

#### Closing the gaps (rounds 4-8)

Round 3 left 289 `demand` rows over 3405 tests: 224 where the election demanded a span and the built plan read NO source binding it `~` (`licensed_extension_spans` was model-wide: `select item.category, sum(price)` over `store_sales` "demanded" `item.sk` because `store_returns` exists), 29 where the WHERE empties the span's region, and 36 where a `~` binder WAS read and the keyspace saw nothing. Those 36 were keyspace bugs, fixed one kind at a time:

| what was missing | shape that found it |
|---|---|
| a `~` recorded under the merged-away address (`origin_concept_address`) | gcat `payload.launch.launch_tag` |
| a source with no declared grain: identified by its KEY columns less the ones that identify ANOTHER source (its foreign keys), so a lookup can enter it | TPC-H instantiated `customers` (adhoc07) |
| a property that identifies some source's rows is an entity | `region_dim` grain `(region)`, `fact` binding `~?region` (`test_join_upgrade`) |
| a key computed row by row is a function of its arguments' entity, not an entity | gcat `orbit_code` |
| a GENERATED key (`unnest`, constant) is the complete side of a `~` merged onto it | `merge orid into ~orid_2` (`test_const_equivalence_merge`) |
| a fan-out BRIDGE: no lookup leads from a launch to its engines (a vehicle has many stages), so no single source carries both; the partial source that joins the region to the rest keeps the unmatched members a region | gcat `test_should_group` |
| no complete source at all: the partial ones ARE the domain and complete each other | `web_orders` / `store_orders` (`test_dataset_merge`), enum unions |
| model components (sources sharing a column), not single rows, decide which entity is cross-joined onto a region | fell out of the bridge |

Tried and reverted: resolving a rowset key to the key it wraps. A rowset is a row SOURCE; `select even_orders.store_id` must not gain the stores no order references (`test_complex::test_rowset*` turned into keyspace-only demand).

Round 8: 3410 passed, SQL byte-identical, no keyspace-only span, no `heal` row. The 11 rows left where a `~` binder is read and the keyspace demands nothing, all election over-demand:

- **Grand totals** (2): `select order_count` from `agg_by_customer(~customer_id)`. No entity, one row, nothing to extend.
- **Rowset inner plans** (4: TPC-DS q64 x3, `test_aliased_outputs_keep_the_fk_axis`): the `~` source is read inside the rowset's OWN plan (`ss.is_returned` lives in `ss_rows_99` / `ss_rows_00`), which has its own keyspace and election; `extent_free_spans` is scoped per plan, so the outer demand never reached it.
- **`complete where` partitions** (1): `~city` is the partition discriminator, the union rule heals it, and no source holds a city no partition has.
- One test that truncates the source search on purpose.

Plus a whole-corpus SQL diff (a pytest plugin wrapping `BaseDialect.compile_statement`), election reading the keyspace vs not, over the 25 files holding every disagreement: 1028 statements, 2 differ. TPC-DS q29 (CTE renames and a reordered WHERE, 45 characters shorter: `extent_free_spans` leaves a CTE identifier once nothing suppresses a span there) and one gcat statement (two SELECT columns swap). Same rows.

### Phase 3, sites 1 and 2: DONE

`elect_extent_owners` and the dim peel's `_keep_extension_families_together` take the demanded spans as an argument, and `_build_from_graph` passes `Keyspace.output_demanded_spans`. `demanded_extension_spans`, `spans_demanded_by` and the `demand` audit are deleted; `span_members` stays (it is an FD question: what an extension row of a span carries). `zquery29.log` regenerated.

### Phase 3, sites 3 and 4: DONE (2026-09-21)

The two remaining model-wide scans of `column_level_partial_addresses` in join typing now read `Keyspace.in_play_spans` (every region's `spans | completes`, emptied regions INCLUDED: their rows are gone once the WHERE has run, and a merge below that point still sees their padding). `_build_from_graph` scopes it on `BuildEnvironment.in_play_spans`; each `MergeNode` captures it at construction, the way it captures `extent_free_spans` and for the same reason.

- **Site 3, the padding-provenance matrix** (`get_node_joins`, now `_span_padding_matrix`; `licensed_extension_spans` deleted).
- **Site 4, the host grain** (`MergeNode`, `host_stitch`): which `~` outputs make a side the host. Not on the original inventory; it had its own inline copy of the model-wide set.

Method: both answers computed side by side under the audit flag, the plan still built on the old one. 250 matrix computations and 166 host grains over the planner suites; 30 and 28 disagreed. What they were:

| what | shape that found it | outcome |
|---|---|---|
| a rowset body is its own plan with its own keyspace; the OUTER plan's merge reads padding made inside it | TPC-DS q64 (`ss_rows_99` / `ss_rows_00`), `test_semi_join_pushdown` | fixed: the matrix reads the spans of every merge below it (`merge_node.tree_in_play_spans`). On the node tree, not the environment, so a history-cached sub-plan still carries its spans. First place the unmodelled "rowset as a witness" is consumed. |
| a scoped join keys the pair on its canonical, not on the member bound `~` (`subset join pr.item.sk = ss.item.sk`; the keyspace names the span `pr.item.sk`). The model-wide set only matched because an unrelated `ss` source also binds `~ss.item.sk` | `test_q64_rowset_join_with_second_fact_join_hoist` | fixed: `_span_spellings` names a span through its `scoped_join_key_groups` group |
| model-wide over-approximation: an authored `union join`'s padding, or a plain dimension lookup, called `~` padding because some source the plan never reads binds the key `~` | q29 existence feeder x2, `test_join_hoist_inlined_dim_group_key_no_dangling_source` | the keyspace's answer stands |
| a composite-key join names `vehicle.variant` beside `vehicle.name`; only the requested key is an entity. Both sides shrink alike | gcat `test_full_join_issue_2` | the keyspace's answer stands |
| host grain `{ticket_number}` because `store_returns` exists in the model, in plans with no span in play | TPC-DS q34 / q73 / q79 / q64, the `complete where` city model | the keyspace's answer stands: it is what the code's own comment says ("with no licensed keys in play, grain coverage decides"). The host grain reads its OWN plan's spans, not the tree's: a rowset below is a row source, its extension rows are not this plan's to host. |

Then flipped with the audit still comparing: 3417 passed, every `zquery<N>.log` byte-identical, the residual rows exactly the explained ones above. A compiled-SQL diff over every file holding a disagreement (178 tests) moved nothing. (`gcat::test_environment` reorders two SELECT columns BETWEEN TWO IDENTICAL RUNS: pre-existing nondeterminism, and very likely the "one gcat statement" of the sites 1-2 diff. Compare distinct SQL per test, not by statement index: the benchmark tests compile a varying number of times.)

Already done by transitivity, no change needed: `MergeNode._extent_free_partials`, `extension_padded_addresses` and `_cover_groups_for_mandatory` read the election (`extent_free_spans`, `ownership.owner_of`), and the election has read the keyspace since site 1.

### Phase 3, site 5: pin-heal asks the keyspace which rows die (2026-09-21)

`partial_bridging._extension_killed` (a proven-non-null bound concept outside the key's FD closure) is replaced by `Keyspace.binding_is_complete(source, span)`: a source's `~` on a key costs the plan nothing when the WHERE empties every kind of row that source has no match for. Pin-heal keeps what is its own: the anchor guards (`_pair_anchors` / `_anchors_dispensable`: emptied is a fact about rows, healing must also be safe for every other merge the dropped `~` licenses), the model-component gate on killers, and the `then where` exclusion. The phase 2 `heal` audit and its replay plumbing are deleted with the predicate they audited.

What had to be learned first, in order:

- **It stays at the statement seam, and the reason is not the one first assumed.** Nothing between `get_query_node` and `build_keyspace` READS a `~`, but `generate_graph` captures the datasource OBJECTS, so a heal after it rewrites a mapping nobody reads (tried: per-plan healing inside `_build_from_graph`; 17 failures, wrong rows and `duplicate alias` binder errors from healed and unhealed scans of one source sharing an identifier). So the keyspace comes to the seam instead: `build_concept_graph` + `build_keyspace` need no graph, and run only once the cheap gates pass (a `~` column exists, the WHERE proves a bound column non-null).
- **Pin-heal was model-wide too.** It healed every `~` column in the model whose extension the WHERE would kill: 143 of 331 audited plans healed only spans the plan never has in play (TPC-H q1 "healed" `part.id` and `order.customer.id`). Restricting it to the statement keyspace's in-play spans moved no SQL over the 23 files where it fires (486 tests, 374 compiled-SQL sets), so those heals were inert and `binding_is_complete` is false out of play.
- **Completion emptiness was the keyspace gap** (5 tests: TPC-DS q83, `test_return_date_pin_heals_unified_returns`, three `returns`-beside-`lines` shapes). `where return_amount is not null` kills the lines no return matches, but no entity is absent there, so no region dies (q94's lesson, from the other side: a NULL value is still rejected). `Region.completions` now names each source holding only some of a region's rows, its `~` spans, and `emptied_by`: the null-rejected concepts only that source supplies.
- **Whose rows a region is** (`Region.witnesses`). Healing P's `~` forgets only the members P lacks, so a live region P itself witnesses (q83's saleless returns) does not block it; a region another source witnesses does.
- **`complete where` slices are not each other's missing rows.** With no complete source the partial ones complete each other (`web_orders` / `store_orders`), but not across mutually exclusive partitions (the unified returns per channel): the union machinery stacks those. First place the keyspace reads `non_partial_for`.
- **Bound columns only.** `emptied_by` follows the RULE, so `status = 'delivered'` empties `{customer}`; the rendered CASE still yields a value on those rows until phase 4. Pin-heal builds its keyspace with `null_rejected=` its bound-column proofs. With that the two judgments agree on every binding in the corpus, and the flip is byte-identical. Without it there was exactly one difference, a real improvement held back for phase 4: gcat `test_spacex_aggregates` heals the MERGED key (`merge first_org into ~org.code`, which `_structural_partial` could never see) under the derived `launch_date.year >= 2015`, `LEFT OUTER JOIN` to `INNER JOIN`, same 12 rows.

Method, again: both judgments computed side by side inside the anchor guards, the plan built on the old one, every difference triaged (6, then 2, then 0), then flipped. Known noise when diffing SQL: `gcat::test_environment` reorders SELECT columns between identical runs.

### Pick up here

0. **Before any phase 4 code: owner questions 4 and 5 below need answers.** Both are visible behaviour changes phase 4 makes on purpose (an orderless customer counted under `status = NULL`; `filter ... where` in a SELECT becoming a value, outside `~` models too). Ask; do not guess. Phase 4 is also the first phase that is NOT byte-identical, so the side-by-side agreement audit stops being the method: the oracle (`tests/engine/test_derived_key_domain.py`, strict xfails as targets), row capture (`local_scripts/keyspace_ab/ks_rowdump.py`) and the SQL-size budgets are.
1. Phase 3 is complete. Two heal improvements are parked behind phase 4 because they need derived null-rejections to be true in the rendered SQL: merged `~` keys (above) and `status = 'delivered'`-style pins (`test_where_null_rejecting_an_absent_concept_empties_the_region`, `test_materialization_invariance`, `test_merge_discovery`). When phase 4 lands, drop `null_rejected=` from `_statement_keyspace` and A/B with row checks. "Pin-heal stops rewriting datasources" is further out: the network search reads partiality as one FULL/PARTIAL bit per binding off the datasource (phase 0), so something has to keep clearing that bit.
2. Phase 4 has its spec: `Keyspace.absent_regions(address)` is the prototype's `absent_on_extension`, per region instead of per span, and the worklist is the audit's `owner_pads` list: the oracle's owed queries, `test_status_on_extension_rows_is_null_without_an_aggregate`, `test_composite_grain_families_with_by_span_aggregate`, `test_by_dim_key_aggregate_vs_row_value`, `test_forked_with_status`, four gcat statements, two rowset tests, `test_inline_broadcast_join_key`. No TPC-DS or TPC-H benchmark query is on it, which is what the prototype's `_null_on_padding` gate bought by hand. The prototype's three gates map to keyspace facts: "one span" becomes one bucket per live extension region (regions are disjoint, so families cannot cross-pair); "span key projected" becomes the region's identifying keys demanded as hidden columns; "WHERE deliverable" becomes `emptied_by` (region dead, no bucket), an atom over concepts defined on the region (filter the bucket), or a null-accepting atom over an absent concept (host at FINAL).
3. Not modelled and needed by phase 4: an OPTIONAL entity (a lookup through a source whose grain is bound `~` and that binds a further key). Same bug from the other side: `coalesce(return_reason, 'none')` keyed on a return evaluates on sale lines that have none. And a rowset as a witness, related to model keys only through an authored scoped join; `tree_in_play_spans` is the stopgap that lets join typing see through one.
4. Phase 7 inherits one more piece of span plumbing: `BuildEnvironment.in_play_spans` / `MergeNode.in_play_spans`, beside `extent_free_spans`.
5. Process. Commit early, push to PR #702 and let CI run the full suite (it only triggers on pull requests; the local full suite is 2h+ and gets killed for memory, as do long idle background runs). Locally: one pytest at a time, targeted files or the ~20 minute planner suites. A known CI flake, not a finding: `tests/modeling/gcat/test_gcat.py` failing as one `UnicodeDecodeError` in `duckdb_engine` followed by "Current transaction is aborted" on every later gcat test is the test's own raw-SQL `httpfs` fetch from GCS erroring; re-run the job. `tests/engine/test_bigquery.py::test_readme` with a Google `RefreshError` is the same kind.

The owner questions below are still open; 1 and 3 were taken as proposed (entity keys, widened to anything that identifies a source's rows; `BuildInfo`), 2 stays with the union machinery, 4 and 5 only matter from phase 4.

## Guards

- `tests/engine/test_derived_key_domain.py`: the oracle (materialization invariance: storing a derivation as a column at its grain must never change a query's rows), `OWED` strict xfails as targets, inline-vs-named spellings, and three traps (`?` key, ROLLUP subtotal, present entity with an unbound property).
- `tests/engine/test_duckdb_partial_key_assembly.py`, `test_duckdb_partial_fk_field_report.py`, `test_multi_fact_nullable_fk_extent.py`, `tests/optimization/test_join_upgrade.py`.
- `tests/modeling` SQL-size budgets and `zquery<N>.log` hashes: the byte-identical check for phases 1-3.
- Process notes in the handoff still apply (one pytest at a time, `-v` to a log file, never `stash`/`reset` in this tree).
- `local_scripts/keyspace_ab/`: the A/B tooling phase 3 was verified with (compiled-SQL and row capture as pytest plugins, a per-plan keyspace printer, a distinct-SQL differ) and the traps in using it.

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
