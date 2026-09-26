# Project plan: a keyspace phase in discovery

> **Status (2026-09-26).** Phases 0-7 are built on branch `extension-row-null-semantics`, draft PR #702, except for the deletions that stay as the fallback for regions the keyspace does not model yet ([What stays](#what-stays-and-why)). The oracle owes nothing. A fresh session should read [The problem](#the-problem) and [The idea](#the-idea) for the vocabulary, then [Open items](#open-items). Before touching a seam, read its section under [Rules as built](#rules-as-built). Git history holds the per-session narratives and A/B tallies this doc used to carry.

Background: `docs/handoff_extension_row_semantics.md` (the rule this serves, and a prototype that was backed out), `docs/extent_ownership.md`, `docs/domain_graph_design.md`. Tooling: `local_scripts/keyspace_ab/README.md`.

## The problem

Every statement has a **row universe**: the kinds of row it can return, and which entities exist on each kind. Take customers and orders, where not every customer has an order. `select customer_id, status` has two kinds of row: one with a customer AND an order, and one with a customer and no order.

Before this project nothing in discovery computed that. About a dozen sites each re-derived the fragment they needed from stamps (`Modifier.PARTIAL`, `partial_concepts`, `nullable_concepts`) and from the shape of whatever was already built. They disagreed at the edges, and every disagreement was a wrong answer or a lost filter. The derived-concept key-domain rule (a derived concept is NULL where its key's entity is absent) had no single place to live. The prototype needed five new predicates and three `keep=` parameters, each restating "which rows, which entities" locally.

The original sites, and where each stands now:

| site | question | now |
|---|---|---|
| `partial_bridging.heal_pinned_partials` | does this WHERE empty a kind of row? | asks `Keyspace.binding_is_complete`; still rewrites datasources (by design, see pin-heal) |
| `join_resolution.licensed_extension_spans`, `extent_ownership.spans_demanded_by` | which extra kinds of row does the statement ask for? | deleted; `Keyspace.output_demanded_spans` |
| `extent_ownership.elect_extent_owners` | which group produces them? | reads the keyspace; its ranking branch stays as the no-domain fallback |
| `group_graph._keep_extension_families_together` | two kinds of extension row must not pair | stays (see What stays) |
| `MergeNode._extent_free_partials`, `extension_padded_addresses`, `get_join_type` | which NULLs are absence, and whose? | read the election / `region_spans` / `SpanScope` |
| `strategy_builder._cover_groups_for_mandatory` | which group holds a column's value on which rows? | reads the election and `region_reads` |
| the three folds (`_fold_passthrough_parents`, `_drop_ancestor_parents`, `_fold_covered_contributors`) | can this contributor go? | read the region contract (`region_reads`); `keep=` gone |
| `condition_placement._preserved_final_branch`, `_uncovered_exposing_output_contributor` | will a join re-admit rows this WHERE removed? | stay: load-bearing on ~20 non-`~` shapes |
| `optimizations/predicate_pushdown`, `join_upgrade`, `null_safe_join` | is this side null-extended, is that NULL a value? | unchanged consumers |

`domain_graph_design.md` and `v4_network_discovery_design.md` each have a "nullability is not modeled" section. `DomainGraph` relates VALUE SETS across the model. The keyspace is its statement-level counterpart.

## The idea

A **keyspace** is computed once per plan, before grouping, from the requested keys.

- A **region** is one kind of row: a set of entity keys that are PRESENT on it. Customers/orders gives `{customer, order}` and `{customer}`.
- The statement's universe is a set of regions. They are disjoint by construction (the Venn diagram's cells, not its circles).
- A concept is **defined** on a region when every one of its FD-minimal keys is present there. Elsewhere it is ABSENT. That renders as NULL, but it is not a NULL value.
- An aggregate is evaluated OVER regions, which is why `count(order_id) by customer_id` is 0, not NULL, on `{customer}`. A row-stream derivation is evaluated ON the regions where it is defined.

Shapes it has to get right (pinned in `tests/core/processing/test_keyspace.py`):

| model fact | regions |
|---|---|
| `orders.customer_id: ~customer_id`, customer key demanded | `{customer, order}`, `{customer}` |
| same, customer key NOT demanded (join axis only) | `{order}` |
| items bind `~user_id` and `~product_id` | `{item, order, user, product}`, `{user}`, `{product}`. No `{user, product}` cell: "families never cross-pair" is this, stated once |
| `returns` binds `~order_id, ~item_id` as its OWN grain, `lines` binds both complete | one region `{order, item}`. `_ret_order` is a NULL VALUE there and `is_returned` evaluates to false (tpc_ds q94) |
| `orders.customer_id: ?customer_id` | one region; the NULL key is a value on a real row |
| `by rollup (k)` | subtotal rows are a grouping product, not a region |
| WHERE null-rejects a concept absent on a region | that region is empty for this statement |

"The span does not FD-determine it" is NOT the region test; that breaks q94. Presence is not FD.

## Owner decisions

- **Regions are keyed by entity keys**, not concepts. A property is never a witness (tpc_ds q70), except where it identifies a source (see regions below).
- **`complete where` stays with the union machinery**; it is not a region fact. It is a trusted containment contract.
- **The keyspace lives on `BuildInfo`, per plan.** `DomainGraph` stays model-level.
- **An aggregate ARGUMENT demands its key's region** (answer 4): `select status, count(customer_id)` counts the orderless customer under `status = NULL`, as the materialized twin does.
- **A filter concept is a value** (answer 5, "a bug, not a choice"): `filter X where COND` narrows only the concept it defines, never the other columns' rows. The materialized twin restricting the stream is the same bug, so the oracle cannot judge it; it has its own expected-rows tests, and it reaches outside `~` models.
- **Two `~` bindings on a key have no defined relationship.** The full set is always a complete source (or a `complete where` slice the union machinery stacks), and that is what a heal completes against. With no complete source the key is not in play and nothing heals: `UnresolvableQueryException: no complete sources`.
- **thelook q22 was rebaselined at 872.** The solid stream as its own CTE beside the user domain is the design's shape.

## Where it sits

`concept_strategies_v4._build_from_graph` is a three-stage pipeline: `build_concept_graph` -> keyspace -> `build_group_graph` -> `build_strategy_node`. The keyspace is attached to `BuildInfo.keyspace` and handed to grouping, strategy building and FINAL assembly. `_build_from_graph` recurses (rowset bodies, WHERE-phase sub-plans), so a keyspace is per PLAN. Its demand is still the STATEMENT's: only mandatory concepts in `environment.statement_output_addresses` count, because a condition feeder lists its grain keys as outputs, and those are the axis it joins back on (TPC-H q20).

`statement_keyspace` is the one seam every plan's keyspace is built through (population, statement-output filter, witnesses). `partial_bridging.scope_statement` is the one seam for heal: authored and output addresses, `heal_pinned_partials`, `drop_excluded_partials`. Both `get_query_node` and `build_nested_select` call it, because the latter materializes a fresh environment for every rowset body, union arm and merge arm.

The keyspace does NOT own join rendering, source search, aggregate argument inlining or the optimizer; those consume its answers. Aggregate argument inlining is a stage-3 choice: an aggregate re-derives its BASIC arguments from the root, which is why padding must never happen inside one.

**The source search reads no span.** The network search reads `datasource.partial_concepts` once, as one `BindingStrength.FULL | PARTIAL` bit per binding; that bit is also the whole Rust FFI surface for partiality. `non_partial_for` + `condition_implies` can promote a binding per request. `network_coalescing.downgrade_axis_bindings` MINTS partiality for coalescing axes with no stamp behind it. Spans reach source planning only ambiently, through the `MergeNode` environment capture. The legacy single-scan path (`select_helpers/source_scoring.py`, `select_node_v2.scan_stamps`, the only `NULLABLE` reader) has the stronger partiality reasoning. There is no model-level address -> binders index, so the keyspace builds its own, cached per environment (`keyspace._model_facts`).

## Rules as built

Each bullet gives a rule and the shape that forced it. Tests named are the guards.

### Regions (`v4_helper/keyspace.py`, `models.Region` / `models.Keyspace`)

- **Entities.** A KEY is its own entity; its `keys` are an FK-path artifact. Anything else resolves through its keys recursively: an aggregate's keys are its `by`, which can be a property or a ROLLUP marker, and a ROLLUP aggregate declares no keys, so `_entity_keys` reads its `by`. A key computed row by row is a function of its arguments' entity (gcat `orbit_code`). A property that identifies some source's rows IS an entity (`region_dim` grain `(region)`).
- **Regions are generated by sources.** A datasource row carries the keys it binds plus what a keyed lookup reaches from them (whole grain only, never through a source whose own grain is bound `~`). Each carried key remembers the `~` bindings on its path (its cause). A source carrying a proper subset of the entities witnesses a smaller region. That region survives only when every source carrying MORE reaches its identifying keys through a `~`; those keys are the region's `spans`. An entity no source relates to a region's rows is cross-joined onto them, so it is present there. Model components (sources sharing a column), not single rows, decide that.
- **Sources without a declared grain** are identified by their key columns, minus the ones that identify ANOTHER source (their foreign keys) (TPC-H instantiated `customers`).
- **Fan-out bridges.** When no lookup leads from A to B (a launch has many engines), no single source carries both, and the partial source joining the region to the rest keeps the unmatched members a region (gcat `test_should_group`).
- **Generated keys** (`unnest`, constants) are the complete side of a `~` merged onto them.
- **A `~` survives a merge** and is found through `origin_concept_address`. Facts are read whether or not a license survives heal; otherwise one entity is spelled `c2` in one plan and `customer_id` in the next.
- **Demand** is the election's question: a span is demanded when some output is a function of what a lookup from the span ALONE reaches (`Keyspace.span_reach`, `output_demanded_spans`). TPC-H q20/q9 and TPC-DS q05 separate this from "any output defined on the region".
- **Completions** (`Region.completes`, `Region.completions`): a source the plan NEEDS that holds only some of a region's rows beside a source holding all of them (`returns` beside `lines`, a partial aggregate table beside its dimension). "Needed" matters: TPC-DS `store_returns` binds `~item.sk, ~ticket_number` model-wide, and only a statement reading a returns measure is completed by it. `Region.live_completes`: an emptied completion demands nothing.
- **`emptied_by`**: a WHERE null-rejecting a concept that is absent on a region empties it. This follows the RULE, so derived concepts count. It uses `gather_non_null_proofs` (BETWEEN included; `condition_proves_non_null` alone missed it: TPC-DS q77). The filter population (`statement_filter_population`) feeds it too.
- **`in_play_spans`**: every region's `spans | completes`, emptied regions INCLUDED. Their rows are gone once the WHERE runs, but a merge below that point still sees their padding.
- **`Region.witnesses`** (whose rows a region is) and **`Region.has_own_rows`** (some complete source witnesses it). A region kept only as a completion has no row where an entity is absent, so it gets no domain. Found through a `persist` into the session environment (`test_website_demo` fan-out).
- **Queries**: `Keyspace.defined_on(address, region)` is what absence means. `Keyspace.carried_on` is what an extension row holds, keyed on what a lookup from the region's spans reaches (a cross-joined entity is present but not carried). `keys_by_address`, `entity_keys`, `binding_is_complete(source, span)`.
- **A BASIC is keyed on what it READS**, not on its declared keys (`keyspace._read_addresses`). `coalesce(sum(amount), 0)` lists `order_id` as a key, and treating it as absent gave the ROLLUP trap a domain.
- **An existence-only node** is a semijoin's subselect, not a row of the plan. Spans are named by the address bound `~`, entities by the canonical spelling.
- **A rowset key is its own entity**, not the key it wraps: `select even_orders.store_id` must not gain stores no order references. `select s.sk, item_desc` is disconnected, and the keyless guard rejects it.

### Region domains (phase 4; `v4_helper/region_domains.py`, `group_graph`)

A live, demanded region with own rows gets a ROOT bucket of its own, the region's **domain** (id `grp:root:root:∅:extent:<spans>`, `GroupAttrs.extent_spans`). The exception is a region whose spans an authored coalescing relation already unions (`union join ocust = cid` IS that key's domain).

```
grp:root:root:∅                          (customer_id, delivery_date)  solid: orders rows only
grp:basic:d*:…                           (status)                      over the solid stream
grp:root:root:∅:extent:local.customer_id (customer_id[, name])         the region's rows: customers
FINAL                                    customers LEFT JOIN status-stream ON customer_id
```

- **Demanded** (`_region_is_demanded`): an output is a function of what the span reaches, or an aggregate counts the region's rows. Otherwise FINAL adds all-NULL rows past the WHERE (gcat `test_merge_with_filter`).
- **The spans ride hidden** as a secondary member of the domain and of every bucket it was split from (`region_join_keys` in `_compute_concept_sets`, ROWSET buckets included), and they are the join axis everywhere. `_final_merge_grain` counts `extent_spans`. `_refresh_input_contracts` puts them in every parent's `preserve_keys`, so no domain is sliced below its spans. `_project_basic_aggregate_inputs` keeps them. FINAL emits the spans its parents READ.
- **Only a bucket that MIXES pads.** A bucket holding only one kind is solid, or the dimension. Reading otherwise made an order-keyed dim peel "the region's rows", or fanned orders out by the other family's span.
- **A domain contributes ROWS** (`_add_region_domain_contributors` adds it as a contributor of no concepts), and it stays whole at FINAL (not bucketed per natural grain by `_wrap_for_grain`). It is added after `_promote_final_aliases_to_grouping_contributors`, which deletes contributors left with no concepts.
- **What the domain carries is PARTIAL on the solid stream**, like the span key (`_extent_free_partials`, `SpanScope.extent_free_carried`). Without that, an aggregate `by name` makes the domain look redundant and drops it.
- **The solid stream reads a span from the fact's own column.** A group built not to extend a span has that span promoted to a full binding in the network search (`network_build._candidate`) and in the legacy renderer (`excluding=`); `_complete_partial_requested` skips it. A group that reads no domain does not EMIT a carried member to a consumer that does.
- **Every row-stream derivation the region carries reads the domain** (`feed_region_domains_to_present_scalars`). A derivation that reads something carried reads the domain of every region it is NULL on the padding of (`null_on_padding`, in `extent_ownership.py`). Without "reads something carried", q77's trivial rename padded for nothing; without "every region", q08 stitched a FULL. When such a group READS only carried things (`_reads_only_carried`), the ROOT lineage edge is dropped, so it is not evaluated on the padded solid stream (the `upper(name)` fan-out).
- **Carried-only members sharing a bucket with something absent** get a bucket of their own (`split_carried_only_row_streams`, `:reads:<span>`). A rename beside a `grain()` hash in one `sig:` bucket lost its label otherwise.
- **A rename of what the domain carries is rendered on the domain** (`_add_region_domain_contributors`). An alias rides its source's ROOT bucket, so the feed rule never sees it. See the open `~?` guest item: this rule is wrong when a contributor already groups by the rename's source.
- **`_solid_groups` seeds** are derivations that TAKE A VALUE on the padding (`_takes_a_value_on_padding`), plus row streams that must not see an extension row.
- **Not modelled**: a MATERIALIZED aggregate beside a region (thelook `sales_agg`, a rollup over the region's rows held as a ROOT). Such a bucket seeds no domain and keeps the padded plan. A mandatory output no contributor renders raises, rather than FINAL silently projecting what it had.
- **The OPTIONAL entity is modelled.** `returns` binding `~order_id, ~item_id` plus its own `return_id` is a larger source reached through a `~`, so `{order, item}` is a region with those spans (`test_optional_entity_is_absent_on_rows_without_it`).

### Aggregates over a region

- **An aggregate is evaluated OVER the region** when a grouping key is carried (`count(order_id) by customer_id` is 0; `coalesce(sum(amount), 0) by rollup (customer_id)` is 0 on the orderless customer and 60 on the grand total) or when its argument is (`count(customer_id) by status`, `_aggregates_over_region`). The domain is then not a FINAL contributor a second time.
- **Its named BASIC arguments are computed on the solid rows FIRST** (`_project_basic_aggregate_inputs`), then the domain merges in: `count(status)` must not count a padded row. The seam classifies parents by `region_reads`, the solid side being the complement.
- **An INLINE argument that takes a value on padding** (`sum(case when undelivered then 1 else 0 end)`, `count(grain(...))`: the hash coalesces NULLs) keeps the aggregate solid, and the domain pads at FINAL. Never over the region when a row stream that must not see an extension row reads it (`order_status <- case when qty = min(qty) by user_id ...`).
- **A COUNT the domain pads is 0, said by the merge that pads it**: `QueryDatasource.zero_filled` -> `CTE.zero_filled`, coalesced by the dialect. The older "nullable COUNT through a join" guess stays for materialized aggregates. A union-join arm's absent count is NOT 0.

### The region contract on nodes and join typing

- **`nodes.base_node.region_reads(node)`**: the spans whose extension rows are rows of a node's stream. It stops at a rowset boundary (`StrategyNode.region_boundary`). Two contributors stand in for each other's columns only when they read the same regions; the three folds and `_parent_nodes_for` check it.
- **`StrategyNode.region_spans`** is set when the domain is built (it survives `copy()`; `_elide_single_parent_passthrough` must keep it). `StrategyNode.resolve` stamps `QueryDatasource.region_spans`, and `QueryDatasource.__add__` must carry every span field (LHS wins).
- **`get_join_type` reads it per pair** (`region_holders`). The side holding the region joined on its span is preserved. The feeder is preserved too where its key holds a value NULL (`extent_nullables`, chained through lookups keyed on a padded column). The join is FULL when the merge holds a second family. Two holders of one region pair plainly. Every merge with a domain parent is a `host_stitch`. `_is_filter_population` never calls a side the population against a partner holding the region.
- **Value NULLs** (`_unpaired_value_nulls`, holder and host vetoes alike). On the region's own key an EXTENT null on the feeder always vetoes: a guest names no member. A VALUE null, there or on any other key, pairs null-safely with a value NULL the holder carries, so a nullable stand-in key joins LEFT.
- **An extent-free span is INNER at plan time** when the other side carries the span's whole domain (`complete_key_domain`: an unfiltered, unlimited complete scan, or a read keeping every row of one through `preserved_sources`) and the binder has no NULL on it. This is the plan-time half of the optimizer's `_pair_side_fully_matches`.
- **The padding-provenance matrix** (`_span_padding_matrix`) reads its own plan's `in_play` plus `SpanScope.witnessed` (rowset body spellings, see below). `_span_spellings` names a span through its `scoped_join_key_groups` group, since a scoped join keys the pair on its canonical.
- **The host grain** (`MergeNode`, `host_stitch`) reads its OWN plan's spans, not the tree's. A rowset below is a row source.
- **A preserving join is part of a merge's identity** (`QueryDatasource._compute_identifier`, `_null_extended_sides`). The signature names the NULL-EXTENDED side, so `a LEFT JOIN b` and `b RIGHT JOIN a` are one relation. Otherwise two differently-typed merges share a CTE, or two copies of one merge fail to fold.
- **Span scope**: `BuildEnvironment.span_scope: SpanScope` (`in_play`, `demanded`, `extent_free`, `extent_free_carried`, `owned`, `witnessed`) is set and restored as a unit and captured whole by `MergeNode.span_scope`. `demanded` spans are coalesced keys for three narrowings: the merge's own WHERE (`_join_proofs`), a branch's filter over a key it was built not to extend, and the "filtered branch is the population" rule.

### WHERE by region (phase 5; `condition_placement`, `projection`)

- **A value the region's rows carry is deliverable** (`_filters_region_domain`, via `carried_on`). A domain member filters the domain. A carried value no domain column holds (a scalar over an aggregate by the span) has its producer read the domain, the condition phase included (matched on scope, not label). The atom is restated at FINAL over the united rows (`PlacementReason.FINAL_SPAN_DOMAIN`, `_reads_past_region_domain`), and the producer's constraint edges into the solid stream are dropped (`detach_final_span_domain_producers`).
- **A host the domain FEEDS is where the region's rows unite for that atom.** The detach keeps a CONSTRAINT edge into a domain-fed successor, and placement hosts the atom at the domain-fed grouping candidates (`_region_domain_grouping_hosts`), FINAL only when there are none. Without this, a null-accepting atom over an absent value under an aggregate by the span was applied above the aggregate (13 oracle DIFFs), and an aggregate grouped by an absent value was wrong on both models (the oracle is blind to it; hand-computed rows in `test_atom_under_an_aggregate_the_domain_feeds`).
- **Only when the final rows decide the atom** (`projection.decided_at_output_grain`): every output crossing an aggregate is grouped at a grain determining the atom's input, judged only over outputs the domain does NOT feed (`_fed_by_region_domain`).
- **Such a WHERE alone earns the region a domain** (`where_over_region`).
- **An atom over the span key keeps the extension row** (`where customer_id in (2, 3)`).
- **A null-rejecting atom over an absent value empties the region**: no domain, the filter sits on the fact side.
- **`null_padded_nodes` walks the accumulated left** of a RIGHT/FULL join. A null-ACCEPTING atom must not push below it. `_push_having_into_group_parent` reads the same gate (`_predicate_safe_past_null_extension`). It must also stay blocked until the final join upgrade: pushing a null-rejecting HAVING early materializes a retained duplicate (usa_names `test_filter_constant_with_constant`).
- **`_preserved_final_branch` and `_uncovered_exposing_output_contributor` stay.** A sweep (`ks_pfb.py`) shows them load-bearing on ~20 non-`~` tests: unnest, union-arm and rowset filter leaks, TPC-DS q81/q82, `test_preaggregate_dimension_peel_duplicates_filter_on_both_scans`.

### A filter concept is a value (owner answer 5)

- **No push in intermediate shapes.** `gen_filter`'s carve-outs are deleted. A filter group's predicate narrows its ROWS only when the statement SHOWS nothing but filter values over that one predicate AND this group produces them (`_filter_intrinsic_pushdown_safe`, on `statement_filter_population`). The push is over the parents as built, never a statement WHERE (`test_filter_mixed_aggregate_row_predicate`).
- **A hidden output is not shown.** `statement_filter_population(mandatory_list, hidden)` reads `BuildEnvironment.statement_hidden_addresses` (set in `scope_statement`), so a HAVING aggregate promoted into the mandatory list does not block the push (TPC-DS q41; it was wrong rows on the plain model too).
- **A responsive aggregate in the HAVING** blocks the push only when the consumer reads something off the unfiltered ancestor that the filter group does not emit (`_consumer_reads`: a group's grain and what rides through it are read as themselves). `select even_name having count(order_id) > 1` returned the NULL group before this.
- **A property collapses to its grain.** The FilterNode groups to the outputs' entity grain (`keyspace.entity_keys`) whenever a predicate input is not FD-determined by that grain (`build_fd_determines`), and `CTE.filter_collapses_to_grain` MAX-collapses it. This does not apply under an aggregate consumer (`count(line_no ? ...)` counts the per-row CASE: TPC-H q4).
- **The population reaches the row universe.** The keyspace (`emptied_by`) and pin-heal both read it; otherwise an emptied region still pads and `delivery_date is null` is TRUE on the pad.

### Pin-heal (`partial_bridging`)

- `Keyspace.binding_is_complete(source, span)`: a source's `~` costs nothing when the WHERE empties every kind of row that source has no match for. Healing P's `~` forgets only the members P lacks, so a live region P itself witnesses does not block it.
- It trusts derived null-rejections (`_proven_bound` stands a derivation for its entity keys).
- Heal is restricted to the statement keyspace's in-play spans (model-wide heals were inert).
- It keeps its own anchor guards (`_pair_anchors` / `_anchors_dispensable`), because healing must be safe for every other merge the dropped `~` licenses. It also keeps the model-component gate on killers and the `then where` exclusion. **A sibling itself `~` on the key is not an anchor** (thelook q16 was a heal artifact of one).
- **It stays a datasource REWRITE at the statement seam, and that is correct.** `generate_graph` captures datasource objects, so a heal after it rewrites a mapping nobody reads; per-plan healing inside `_build_from_graph` gave 17 failures. The heal audit (`keyspace_audit.py`) showed that "one keyspace per plan with the plan's own conditions" is WRONG. A window feeder with no WHERE of its own would pad the orderless customer and `row_number` it. Heal is a STATEMENT fact every sub-plan inherits. So the keyspace is built twice per plan (heal's over authored bindings, the plan's over rewritten ones), by design. Re-run the audit if heal or the keyspace's facts change.
- Not done: pin-heal never heals a MERGED `~` key (`_structural_partial` compares `column.concept.address` only). That is conservative, a lost optimization, not a wrong answer (gcat `payload.launch.launch_tag`). The heal keyspace takes no rowset witnesses; that was probed with no oracle failure.

### A rowset as a witness (`rowset_witness.py`, `keyspace.RowsetWitness`)

- **A rowset the plan reads is a source of its keyspace** (`_rowset_sources`). Its rows are the body's live regions, respelled in the handles: one `_SourceFacts` per body region, grain the key handles present, and a handle bound `~` when a smaller region of the body spells its key as a span. Grain and bound keys go through the reader's canonical map (`_respelled`), so a rowset over a rowset composes. The body keyspace is computed as the body's own plan computes it (`build_nested_select`, healed) and cached per rowset on the history. A multiselect body gets no regions. A rowset in progress is an empty witness: a body cannot witness itself (TPC-DS q64 recursion).
- **A key no handle spells is spelled by the handle keyed on it alone** (`s.d` for `item_sk`): that handle is the region's present handle, its span, and its padding's name. If there is no such handle, the region stays out of the reader's plan.
- **`RowsetWitness.spellings`** maps every spelling a join inside the body pads a span under to the handle -> `Keyspace.witnessed` -> `SpanScope.witnessed`.
- **A rowset bucket is split for a region only when something must be evaluated on the solid rows** (`_needs_solid_rows`). A plain read of a padded rowset keeps its one boundary, which is already the region holder. The domain is a ROWSET bucket, a `force_group` GroupNode over the boundary, to its members; `_carry_join_keys` never widens a rowset domain (a ROOT domain must still be widened with a key it binds: `test_licensed_transitive_attr_span`).
- **The solid side is the body planned with the region OWNED above** (`SpanScope.owned`, `V4History.owned_spans`, part of the build key; `body_spans_of` maps reader spans to body spans). The body KEEPS its region, since a window or aggregate it computes over the region must still read the domain, but no group extends the span. `resolve_rowset` marks the owned spans' handles and carried handles partial.
- **A handle is NULL on padding like a ROOT column** (`null_on_padding` treats `Derivation.ROWSET` like ROOT).
- `_carry_join_keys` carries a handle the node's own parent emits (`parent_output_addresses`). A ROWSET bucket gets the hidden span in `_compute_concept_sets`. Both were `ON 1=1` cross products.

### General planner bugs fixed on the way

These are not about `~`; they are worth knowing so they are not reintroduced.

- Output roots whose reach is a ROW STREAM co-source regardless of concept-graph component (`_cosource_component_groups`); two BASICs related only by a datasource FK rendered `ON 1=1`.
- A WHERE over an aggregate by a key was dropped when the key and a property were both projected. The fresh FINAL re-source now replaces the built node only when it can restate every atom. `outputs_with_parent_grain_keys` no longer offers a parent's HIDDEN outputs.
- An atom over an aggregate BY A DERIVED grain crashed (`Missing source map entry`); `_hosts_carrying_condition_grain` fixes it.
- `_concept_covers_grain`: a key covers a grain only when the WHOLE grain is that key. Otherwise a FINAL merge onto a finer row stream skipped its dedup (since #522).
- `_regraft_group_sources`: a projection placed at a grouping provider's grain (`item_desc as d` at `count(order_number)` by `item_desc`) drops the lineage parents the provider was built from. Left attached, the region domain stayed load-bearing for the span every FINAL feeder emits (`_compute_concept_sets`, `region_join_keys`), so the rename was merged back onto the aggregate ON the nullable description, and a `~?` guest's NULL group had no item to match (`GUEST_ALLDESC_CASES`). The strategy-side `_drop_ancestor_parents` could not fold it: the domain carried a column the aggregate lacked.

## What stays, and why

The step-2 retirement list was tried by deletion. These are the fallback for regions the keyspace does not model, each with its shape at the site:

- **`elect_extent_owners`' ranking branch**: a demanded span whose region got no domain. gcat `test_case_key` has a composite-key `~` region never marked demanded; TPC-DS q64 regions only a join of two facts can witness.
- **`_keep_extension_families_together`**: the merged peel cluster is where `add_region_domain_buckets` finds a region's solid source.
- **`_add_region_domain_contributors`' rename rule**: an alias rides its source's ROOT bucket.
- **host/`host_stitch` inference in `generate_joins`**: without it a domain merge FULL-joins its readers.
- **The zero-fill guess in `dialect/base.py`**: the no-domain fallback's `coalesce(count, 0)`.
- **`_extent_free_partials` / `extent_free_carried`** (`test_unsold_item_counts_no_lines[select]`).
- **`_span_padding_matrix` / `_pads_for_different_members`**: without it two families' padding pairs NULL with NULL null-safely. `region_spans` does not yet answer "two readers of different regions".
- The `span_scope` fields go with their last reader; `in_play` stays for pin-heal.

**Tried and reverted, with the reason:**

- Resolving a rowset key to the key it wraps: a rowset is a row source.
- An "aggregation level" rule in `_witnesses`: after heal it absorbed `users` and `orders`. The network search already picks by aggregation level.
- "No complete source: the partial ones complete each other" (shards): retracted by the owner rule above.
- Stamping every rowset boundary with its witness spans (5(c)): grew `test_rollup_label_over_union_joined_rowsets` 2546 -> 2948.
- Dropping the region from the rowset body's keyspace: it would compute `rank sk by d` over the solid items.
- **Flattening the solid MergeNode into the domain merge** (parked, cosmetic). It made q22 byte-identical to its old plan, but a flat chain needs three passes that do not understand a PADDED ACCUMULATED STREAM:
  - `resolve_join_order_v2` can place an INNER after a holder's RIGHT/FULL and drop the domain's rows.
  - `ensure_content_preservation` tracks only prior ON-clause lefts.
  - Handing `held` as `extent_free_spans` strips the holder's own nullable marks.
  - `in_play - demanded` as "unlicensed" over-reaches (gcat `vehicle.class, launch_count`).

## Open items

In the order to take them:

1. **TPC-H q22** keeps the padded plan (the `ks_pfb.py` "keeps its padded plan" emitter). `avg_bal_in_target` is an aggregate scalar over a `~` customer region under a per-country count, and the region does not carry country. Not yet probed with a rows test; do that before calling it plan quality.
2. **The redundant CASE over pushed rows** (`test_a_having_responsive_aggregate_is_not_shown`): the filter node collapses into the aggregate's SELECT, and its CASE is always its THEN branch.
3. **Retiring `_preserved_final_branch`**: its own A/B, with the `ks_pfb.py` list as the worklist.
4. **Rowset witness leftovers**: 5(c), an unsplit boundary is not stamped as the region holder (see tried). A region no handle is keyed on alone is still skipped, and with two properties of the key the first by name is picked.
5. **Unmodelled regions** that keep the fallbacks alive: composite-key `~` demand, regions only a join of two facts witnesses, a materialized aggregate beside a region. Also merged-`~` pin-heal (above).

**Lesson that held every session: every item labelled "plan quality", "cosmetic" or "not split" was wrong rows once a rows test existed.** Write the rows test first. Reasoning-based diagnoses were one cause short each time; trace instead.

## Guards

- `tests/engine/test_derived_key_domain.py`: the oracle (materialization invariance: storing a derivation as a column at its grain must never change a query's rows). It has 100+ cases, `OWED` strict xfails as targets (currently none), traps (`?` key, ROLLUP subtotal, present entity with an unbound property), the `upper_name` twin, and hand-computed rows where the twin is blind.
- `tests/engine/test_where_over_aggregate_by_key.py` (plain model), `test_filter_concept_is_a_value.py` (incl. the HAVING cases), `test_row_stream_outputs_share_a_scan.py`.
- `tests/engine/test_duckdb_rowset_null_group_rejoin.py`: `test_unsold_item_counts_no_lines` (direct, rowset, nested rowset), `RENAME_BESIDE_HASH_CASES`, `GUEST_CASES`, `GUEST_ALLDESC_CASES` (a `~?` guest with every item described: the NULL group has no member to pair with), and the rowset-vs-direct pairs in `test_rowset_matches_direct_spelling` on both models.
- `tests/core/processing/test_keyspace.py` (shapes table, domains, witnesses, heal inheritance), `test_extent_ownership.py`, `test_join_padding_provenance.py` (`get_join_type` rules, `complete_key_domain`), `test_v4_group_behaviors.py` (give new planner-helper params a default).
- `tests/engine/test_duckdb_partial_key_assembly.py`, `test_duckdb_partial_fk_field_report.py`, `test_duckdb_nullability_matrix.py`, `test_multi_fact_nullable_fk_extent.py`, `tests/optimization/test_join_upgrade.py`, `test_duckdb_fuzzer_regressions.py::test_rollup_label_over_union_joined_rowsets`.
- `tests/dialect/test_bigquery_full_join_keys.py`: renders planner output, anchored on a three-fact `union join`. It breaks silently when a fixture's FULL join tightens away.
- `tests/modeling` SQL-size budgets and `zquery<N>.log` hashes.

## Process and verification

- **Commit early and push to PR #702.** CI only runs on PRs; the local full suite is 2h+ and gets killed. First thing in a session: `gh run list --branch extension-row-null-semantics --limit 5`. Known CI flakes: gcat's `UnicodeDecodeError` in `duckdb_engine` followed by "transaction is aborted" (its `httpfs` GCS fetch), BigQuery `test_readme` `RefreshError`, and a windows timing assertion in `test_non_benchmark_queries.py`.
- **Probe with rows first.** Use the twin pattern in `test_derived_key_domain.py`, the plain model in `test_filter_concept_is_a_value.py`, or rowset-vs-direct pairs on `UNSOLD_MODEL`. `local_scripts/keyspace_ab/probes/` has the loop scripts and tracers (see its README). A script looping pairs and printing SAME/DIFF found every bug of a session in one run.
- **Trace, don't reason.**
  - `[v4] built grp:...` INFO lines give buckets and parents.
  - Monkeypatch `edges.add_edge`/`remove_edge` for group-graph edges (patch it in `region_domains` and `group_graph` too, since they import it).
  - Wrap `get_join_type` through `inspect.signature` (it is called positionally) for plan-time typings.
  - Dump `_model_facts` per keyspace build to see what heal changed.
  - Patch a helper where it is IMPORTED, not where it is defined. Print with `PYTHONIOENCODING=utf-8` (ids contain `∅`).
- **The A/B** (method and plugins in the tooling README) is `KS_SQLDUMP` (+ `KS_ROWDUMP`) over the planner suites, then `sqlcmp.py` / `rowcmp.py` / `sqlshow.py`. Traps not in the README:
  - Ignore the live BigQuery tests (`--ignore=tests/engine/bigquery --ignore=tests/engine/test_bigquery.py`); one hung six minutes on a socket. With them ignored the run is 8-10 minutes.
  - Capture the baseline on COMMITTED code with committed `zquery<N>.log`s. A sentinel run once rewrote `zquery77.log` before a baseline. With the tree already edited, run the baseline from a detached worktree at HEAD on the main venv with `PYTHONPATH=<worktree>;<worktree>/local_scripts/keyspace_ab` (no rust rebuild needed). Expect worktree-name path noise.
  - A baseline run can die silently part way; a `0 -> N` block in `sqlcmp` is an uncaptured baseline, not a move.
  - Launch detached with PowerShell `Start-Process` and wrap the `-m` marker expression in embedded double quotes.
  - The JSONL escapes quotes, so `[^\\]*` stops at `\"`; use `(?:(?!\\n).)*`.
  - A tracer abbreviating dict keys can collapse two node names into one entry.
  - Known noise: the four `test_duckdb_file_source` temp paths, gcat `test_environment` (column order), faa `test_llm_execution` (LLM-generated SQL), geography, `now()`, and unordered `LIMIT`s.
  - `adhoc07` and the gcat tests are SQL-only, so growth there cannot be timed.
  - A passing modeling run regenerates benchmark artifacts under `tests/modeling`; commit them.
- **Sentinel set for fast triage**: the keyspace guards plus gcat `test_case_key`, TPC-DS q64, the field report and thelook (~15 s with `ks_sqldump`). Only survivors go to the full A/B.
- One pytest at a time; never `stash`/`reset` in this tree.

## The prototype

Commits `e0f6424b7`..`d1c482137` on this branch, backed out in the next commit, implemented phase 4 for one region shape behind gates (one span, span key projected, deliverable WHERE). Its failures became the requirements: q94 (presence is not FD), q95 (an atom the extension rows never see is a lost filter), the three folds (rows are not columns), sub-plan firing, and SQL budgets tripping on NULL-propagating arithmetic. All are now handled by the rules above.
