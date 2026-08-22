"""ROWSET generator: the boundary node projecting a rowset's inner select."""

from trilogy.core.domain_graph import DomainRelation, EdgeProvenance
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildRowsetItem,
    BuildSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe,
    probe_member_address,
)
from trilogy.core.processing.nodes import History, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.history import V4History

from .condition_sources import resolve_and_inject_condition
from .nested_select import plan_nested_select


def gen_rowset(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """Boundary node for a rowset reference. The rowset's inner select is a
    self-contained sub-query, so — like ROOT — this generator ignores the
    group graph's pre-built parents and plans the inner recursively.
    Enrichment joins back to the outer query are handled by the outer group
    graph / FINAL merge, not here."""
    if not outputs or not isinstance(history, V4History):
        return None
    return resolve_rowset(
        outputs, environment, depth=0, g=g, history=history, conditions=conditions
    )


def resolve_rowset(
    outputs: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: V4History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Plan a rowset boundary node by recursively planning its inner select
    through v4, then projecting that producer under the outer handle addresses.

    The rowset's inner select is a self-contained sub-query, planned the same
    way `get_query_node` plans a statement: we build its author lineage against the base
    environment, materialize a FRESH build environment + graph for it (the
    outer environment classifies the inner's concepts under rowset aliasing —
    a plain root reads back as `derivation=rowset` there — so reusing it
    mis-buckets the inner plan), plan its outputs + WHERE
    through `search_concepts`, apply the inner HAVING as a post-aggregate
    filter, then re-expose the producer's columns under the outer rowset
    handles. Each handle is a ROWSET concept whose `lineage.content` is the
    inner column it wraps — the renderer emits the handle as that content, so
    the boundary is a thin projection whose inputs are the content columns the
    inner producer supplies.

    `outputs` are all the same rowset (the rowset grouping rule buckets one
    rowset's handles together), but a recursive nested-rowset search can hand a
    bucket of plain roots here; bail to None so the caller treats it as a
    normal group rather than asserting."""
    rowset_outputs = [o for o in outputs if isinstance(o.lineage, BuildRowsetItem)]
    if not rowset_outputs:
        # A probe-only demand (the presence-count shape: the boundary's sole
        # contract output is a member's presence probe): recover the boundary
        # through the probe's member handle, which the obligation pass below
        # then materializes alongside the probe.
        for concept in outputs:
            if not is_presence_probe(concept.address):
                continue
            member_addr = probe_member_address(concept.address, environment)
            member = environment.concepts.get(member_addr) if member_addr else None
            if member is not None and isinstance(member.lineage, BuildRowsetItem):
                rowset_outputs = [member]
                break
    if not rowset_outputs:
        return None
    lineage = rowset_outputs[0].lineage
    assert isinstance(lineage, BuildRowsetItem)
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    plan = plan_nested_select(
        select,
        history,
        depth,
        f"rowset {lineage.rowset.name} inner select",
        exclude_derived=lineage.rowset.derived_concepts,
    )
    if plan is None:
        return None
    inner_node = plan.node
    built = plan.built
    inner_env = plan.environment
    inner_g = plan.graph

    # Expose the demanded handles plus any rowset-derived handle that carries a
    # PSEUDONYM — a cross-rowset merge (`merge X.a into Y.b`) links its two
    # boundaries on the merged keys via the canonical-pseudonym map in
    # `get_node_joins`, and those keys are rarely selected by the outer query,
    # so projecting only the demanded handles would drop them and the FINAL
    # merge would degrade to a `1=1` cross product. Pseudonyms are exactly what
    # a `merge into` produces, so they single out the join keys without
    # over-projecting unrelated internals — a rowset-wrapped multiselect's bare
    # align inputs (no pseudonyms) must NOT leak out, or the outer FINAL has an
    # output no parent can source.
    # Usable (non-hidden) outputs only: a hidden inner column (a grain key the
    # inner FINAL masked) has no source-map entry in the rendered CTE, so a
    # boundary input mapped to it dangles at render time.
    produced = {o.address: o for o in inner_node.usable_outputs}
    # A coalescing scoped join (`full`/`subset`/`union`) collapses the join-key
    # group (`a.aid = b.bid`) onto ONE canonical body column, leaving the
    # authored side only as a pseudonym of that canonical — so a demanded
    # handle's content (`a.aid`) has no produced entry of its own. Re-expose
    # the content on the inner producer (sourced off the canonical column via
    # the pseudonym) so the boundary can materialize the handle.
    produced_by_pseudonym: dict[str, BuildConcept] = {}
    for out in produced.values():
        for pseudonym in out.pseudonyms:
            produced_by_pseudonym.setdefault(pseudonym, out)
    coalesced_contents: list[BuildConcept] = []
    for demanded_handle in outputs:
        dlineage = demanded_handle.lineage
        if not isinstance(dlineage, BuildRowsetItem):
            continue
        content = dlineage.content
        if content.address in produced or content.address not in produced_by_pseudonym:
            continue
        if content.address not in {c.address for c in coalesced_contents}:
            coalesced_contents.append(content)
    if coalesced_contents:
        inner_node.add_output_concepts(coalesced_contents)
        produced.update({c.address: c for c in coalesced_contents})
    derived = lineage.rowset.derived_concepts
    demanded = {o.address for o in rowset_outputs}
    handle_pool = list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    )
    handles: list[BuildConcept] = []
    inputs: list[BuildConcept] = []
    seen: set[str] = set()
    condition_row_addresses = (
        {c.address for c in conditions.row_arguments}
        if conditions is not None
        else set()
    )
    for handle in [*rowset_outputs, *handle_pool]:
        hlineage = handle.lineage
        if handle.address in seen or handle.address not in derived:
            continue
        if not isinstance(hlineage, BuildRowsetItem):
            continue
        if hlineage.content.address not in produced:
            continue
        if (
            handle.address not in demanded
            and handle.address not in condition_row_addresses
            and not handle.pseudonyms
        ):
            continue
        seen.add(handle.address)
        handles.append(handle)
        inputs.append(produced[hlineage.content.address])

    # A plain rowset's GRAIN keys (e.g. `id`) are the shared join keys back to the
    # outer query and sibling rowsets, but they're plain roots — not
    # `BuildRowsetItem` handles — so the loop above skips them. Expose any the inner
    # producer supplies so they enter the boundary grain below and the FINAL merge
    # joins on them; otherwise a shared-key rowset with no `merge into` pseudonym
    # degrades to a `1=1` cross product -> cartesian (test_rowset_alias_name_
    # collision: 3 rows -> 27). Carry the inner
    # select's demanded output_components (`additional_relevant`). Multiselect
    # grains are align concepts handled separately below, so scope to plain selects.
    #
    # Only for an UNFILTERED rowset: a WHERE/HAVING makes its key-set a proper
    # subset of the base domain, so advertising the key would let the cover step
    # satisfy the outer bare key FROM the filtered rowset and drop the unfiltered
    # source (rowset_outer_addition: odd orders must survive NULL-extended via a
    # LEFT add, not be inner-joined away). A filtered rowset stays a separate
    # outer-added contributor.
    #
    # An AGGREGATE rowset whose grain key is RENAMED into a handle (`dept_totals`
    # groups by `dept as department`) renders only the handle, so the raw
    # `local.dept` is not in `produced` and the gate below skips it — exposing it
    # anyway made assembly demand a column no CTE projects (query-structure
    # syntax example). A grain key the inner producer DOES render (a bare
    # `grp_key` beside `count(x) -> total`, or a plain projection's passthrough
    # `id`) is safe and necessary: without it two sibling rowsets at the same
    # base grain have no exposable join key and the FINAL merge cross-joins
    # ON 1=1 (alias-collision aggregates: 2 rows -> 2x2 cartesian).
    #
    # A key an EXPOSED handle already covers is not re-exposed under its raw
    # address. The handle is the rowset's own column for that key; adding the
    # base address beside it publishes a second name for the same value, and two
    # sibling rowsets over one base then appear to share a join axis they do not
    # own. That silently outranks an authored scoped join on a derived key
    # (`agg.period + 53 = fut.period`), which re-typed the relation from a
    # subset LEFT to a FULL join.
    if (
        isinstance(built, BuildSelectLineage)
        and built.where_clause is None
        and built.having_clause is None
    ):
        handle_addrs = {h.address for h in handles}
        handle_contents = {
            h.lineage.content.address
            for h in handles
            if isinstance(h.lineage, BuildRowsetItem)
        }
        for key_addr in built.grain.components:
            if (
                key_addr in produced
                and key_addr not in handle_addrs
                and key_addr not in handle_contents
            ):
                key_concept = produced[key_addr]
                handles.append(key_concept)
                inputs.append(key_concept)

    # A rowset wrapping a multiselect: an aligned handle's content is the
    # multiselect concept, which the renderer resolves via `find_source` — it
    # needs the per-arm concepts in the SAME CTE's outputs. They're not handles,
    # so carry them as HIDDEN outputs of this boundary; the aligned value is then
    # materialized here and outer CTEs just reference the column.
    hidden: set[str] = set()
    if isinstance(built, BuildMultiSelectLineage):
        handle_addrs = {h.address for h in handles}
        for item in built.align.items:
            for arm in item.concepts:
                if arm.address in produced and arm.address not in handle_addrs:
                    arm_concept = produced[arm.address]
                    handles.append(arm_concept)
                    inputs.append(arm_concept)
                    hidden.add(arm_concept.address)

    # A demanded output that is the inner multiselect's OWN concept rather
    # than a handle (a union OUTPUT like `local._combined_sort_k` — the
    # ORDER-BY carry of a grouped-away union column, tagged to this boundary
    # by the concept graph because nothing else can produce it): expose it
    # directly off the inner producer. Restricted to multiselect/union
    # derivations — the exact set the concept graph tags — so an unrelated
    # unsourceable demand still fails the search loudly instead of riding
    # the boundary into a joinless plan.
    handle_addrs = {h.address for h in handles}
    for demanded_content in outputs:
        if (
            demanded_content.derivation
            not in (Derivation.TVF_UNION, Derivation.MULTISELECT)
            or demanded_content.address in handle_addrs
            or isinstance(demanded_content.lineage, BuildRowsetItem)
            or demanded_content.address not in produced
        ):
            continue
        content_concept = produced[demanded_content.address]
        handles.append(content_concept)
        inputs.append(content_concept)
        handle_addrs.add(content_concept.address)

    # OBLIGATION: a presence probe over one
    # of this rowset's handles must be computed HERE, pre-merge — post-merge
    # the member reads as the fused group coalesce, never NULL. The probe is a
    # BASIC over the handle, so it renders inline in the boundary SELECT once
    # its member handle is materialized; expose the member as a hidden output
    # when the outer query didn't demand it directly.
    handle_addrs = {h.address for h in handles}
    for probe in outputs:
        if probe.address in handle_addrs or not is_presence_probe(probe.address):
            continue
        member_addr = probe_member_address(probe.address, environment)
        if member_addr is None or member_addr not in derived:
            continue
        member_handle = environment.concepts.get(member_addr)
        if member_handle is None:
            continue
        if member_addr not in handle_addrs and isinstance(
            member_handle.lineage, BuildRowsetItem
        ):
            if member_handle.lineage.content.address not in produced:
                continue
            handles.append(member_handle)
            inputs.append(produced[member_handle.lineage.content.address])
            handle_addrs.add(member_addr)
            hidden.add(member_addr)
        handles.append(probe)
        handle_addrs.add(probe.address)

    # OBLIGATION: a DERIVED relation member (`union join cur.wk + 53 = fut.wk`)
    # is a BASIC over THIS boundary's handles with no producer group of its own
    # — materialize it here so the completion merge can pair it with its
    # authored mate instead of cross-joining ON 1=1 (bundling the derived key
    # into the rowset body select). OUTER (full/union) relations only: a
    # directional (left/subset) relation resolves through the scoped-merge
    # collapse, which substitutes the derived key into the other side's grain
    # and computes it in a downstream projection — materializing it here too
    # displaces that path and widens the authored LEFT to FULL
    # (scoped_derived_rowset_join_matrix). A directional relation's SUBSET
    # SOURCE member is the exception: it pairs by its own value (`left join
    # ta.s = nb.s and nb.w = ta.w + 52`, mixed anchors composing to FULL), so
    # its own side still materializes it. Side identity is structural: every
    # lineage arg must already be a handle of this boundary.
    outer_relation_keys = environment.domain_graph.outer_relation_keys()
    subset_source_members = environment.domain_graph.subset_sources()
    for canonical, members in environment.scoped_join_key_groups.items():
        outer_relation = canonical in outer_relation_keys
        for member_addr in {canonical, *members}:
            if not outer_relation and member_addr not in subset_source_members:
                continue
            if member_addr in handle_addrs or is_presence_probe(member_addr):
                continue
            member_concept = environment.concepts.get(member_addr)
            if (
                member_concept is None
                # address mismatch = the scoped-merge collapse substituted this
                # member to the other side's derivation; that path owns it
                or member_concept.address != member_addr
                or member_concept.lineage is None
                or isinstance(member_concept.lineage, BuildRowsetItem)
            ):
                continue
            arg_addrs = {a.address for a in member_concept.concept_arguments}
            if not arg_addrs:
                continue
            # An arg the outer query never demanded is not a handle yet — but if
            # it IS one of this rowset's own handles the boundary can still
            # materialize it (hidden), which is the only way the derived key
            # gets a producer at all (`subset join ftr.ws - 53 = cur.ws` never
            # projects `ftr.ws`; without this the completion merge has no axis
            # and cross-joins ON 1=1).
            pending: list[tuple[BuildConcept, BuildConcept]] = []
            for arg_addr in sorted(arg_addrs - handle_addrs):
                arg_handle = environment.concepts.get(arg_addr)
                if (
                    arg_addr not in derived
                    or arg_handle is None
                    or not isinstance(arg_handle.lineage, BuildRowsetItem)
                    or arg_handle.lineage.content.address not in produced
                ):
                    break
                pending.append(
                    (arg_handle, produced[arg_handle.lineage.content.address])
                )
            else:
                for arg_handle, arg_input in pending:
                    handles.append(arg_handle)
                    inputs.append(arg_input)
                    handle_addrs.add(arg_handle.address)
                handles.append(member_concept)
                handle_addrs.add(member_addr)

    # A handle that is a declared-subset SOURCE (`subset join rs.k = anchor.k`)
    # spans only the subset side's domain: mark it partial so join resolution
    # anchors the complete side and LEFT-joins this boundary instead of
    # INNER-narrowing the anchor to the intersection. Gated on the subset edge's
    # ANCHOR (its declared superset target) being a rowset handle, not on the
    # whole relation: a mixed root↔rowset relation whose anchor is the ROOT
    # side resolves through binding substitution, and marking the rowset side
    # there re-routed a boundary measure onto the root scan (conflicting-filter
    # year-over-year join re-derived `cnt_2000` row-wise). But a ROOT member
    # elsewhere in the relation (a dim BRIDGE beside rowset-anchored subsets)
    # must not strip the flag from siblings whose own anchor IS a rowset —
    # unmarked, two subset boundaries INNER-join each other and drop the anchor
    # rows they don't share (dim_bridge all_subset_unaffected).
    # A LIMITED body overrides the ROOT-anchor exemption: the limit truncates
    # rows no condition expresses, so the anchor scan's handle binding cannot
    # stand in for the boundary's row set — unmarked, the merge INNER-narrows
    # the anchor to the limited rows (rowset_body_limit left readback).
    subset_sources = environment.domain_graph.subset_sources()

    def _subset_anchors_all_rowset(address: str) -> bool:
        targets = {
            e.target
            for e in environment.domain_graph.edges
            if e.relation is DomainRelation.SUBSET
            and e.provenance is EdgeProvenance.DECLARED
            and e.source == address
        }
        target_concepts = [environment.concepts.get(t) for t in targets]
        return bool(target_concepts) and all(
            t is not None and t.derivation == Derivation.ROWSET for t in target_concepts
        )

    scoped_partial = [
        h
        for h in handles
        if h.address in subset_sources
        and isinstance(h.lineage, BuildRowsetItem)
        and (select.limit is not None or _subset_anchors_all_rowset(h.address))
    ]
    # nullability propagates by ADDRESS between nodes, but a rowset handle is a
    # new address wrapping its body content — map through the BuildRowsetItem
    # content (and pseudonyms) so a `?` column's nullability survives the
    # boundary (else a NULL rowset join key stops matching null-safely).
    # Every handle, not just the boundary's key-like ones: a non-key property
    # becomes a join key the moment split aggregate branches over the same
    # boundary rejoin on their GROUP BY keys, where a NULL is a group label
    # and a plain `=` drops the whole group.
    base_nullable: set[str] = set()
    for c in inner_node.nullable_concepts:
        base_nullable.add(c.address)
        base_nullable.update(c.pseudonyms)
    boundary_grain = BuildGrain.from_concepts(
        [
            h
            for h in handles
            if h.address not in hidden and not is_presence_probe(h.address)
        ]
    )
    # A multiselect/union boundary's rows are at the FULL align grain even
    # when only a subset of its outputs is demanded (`subset join x =
    # all_combos.b` never projects `ch`): the projection does NOT dedup, so
    # stamping the demanded subset as the grain overclaims uniqueness and a
    # downstream aggregate elides its GROUP BY over the per-arm fan
    # (union_reproject direct-RHS: sum fanned to two rows of 10 instead of
    # re-aggregating to 20). Stamp the grain over every align output instead.
    if isinstance(built, BuildMultiSelectLineage):
        full_handles = [
            handle_concept
            for addr in sorted(derived)
            if (handle_concept := environment.concepts.get(addr)) is not None
        ]
        if full_handles:
            boundary_grain = BuildGrain.from_concepts(full_handles)
    nullable_handles = [
        h
        for h in handles
        if (
            h.address in base_nullable
            or (set(h.pseudonyms) & base_nullable)
            or (
                isinstance(h.lineage, BuildRowsetItem)
                and (
                    h.lineage.content.address in base_nullable
                    or (set(h.lineage.content.pseudonyms) & base_nullable)
                )
            )
        )
    ]
    boundary: StrategyNode = SelectNode(
        output_concepts=handles,
        input_concepts=inputs,
        parents=[inner_node],
        environment=inner_env,
        # Grain over the outer handles: lets the
        # FINAL merge join two rowsets on their shared/pseudonym grain key
        # instead of cross-joining when the boundary exposes no grain. Probes
        # are per-key presence markers, not part of the boundary's row grain.
        grain=boundary_grain,
        hidden_concepts=hidden,
        partial_concepts=scoped_partial,
        nullable_concepts=nullable_handles,
    )
    # A filter the group graph injected at this boundary is a consumer-side
    # predicate over the rowset's rows (a multiselect arm's per-arm filter over
    # the row-projection rowset it reads). The inner plan didn't apply it (it's
    # not part of the rowset's own select), so apply it here over the
    # materialized rows.
    if conditions is not None:
        condition_outputs = [
            h
            for h in handles
            if h.address not in condition_row_addresses
            or h.address in demanded
            or h.address in hidden
            or h.pseudonyms
        ]
        boundary = resolve_and_inject_condition(
            boundary,
            conditions,
            list(condition_outputs),
            environment=inner_env,
            graph=inner_g,
            history=history,
            depth=depth,
            grain=boundary.grain,
            hidden_concepts=hidden,
        )
    return boundary
