from trilogy.constants import PRESENCE_PROBE_PREFIX, logger
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import condition_proves_non_null
from trilogy.core.processing.node_generators.select_helpers.datasource_nodes import (
    create_datasource_node,
)
from trilogy.core.processing.nodes import GroupNode, History, MergeNode, StrategyNode
from trilogy.utility import string_to_hash, unique

LOGGER_PREFIX = "[GEN_PRESENCE_PROBE_NODE]"


def is_presence_probe(address: str) -> bool:
    """Per-side presence probe minted by `Factory._coalescing_presence_probe`
    (the null-test rewrite for coalescing join key-group members). The single
    place the probe naming convention is interpreted."""
    return PRESENCE_PROBE_PREFIX in address


def retain_presence_probes(base: list[BuildConcept], candidates) -> list[BuildConcept]:
    """`base` plus any presence probe among `candidates` it does not already
    carry (by address).

    The invariant: a probe is computed on a coalescing member's own side
    pre-merge and read by the outer WHERE post-merge, so every node that
    re-projects, merges, or groups its parents' outputs must keep the probes
    those parents expose — else a member null test re-derives off the fused
    coalesced key (never NULL) and silently no-ops (TPC-DS q35). Single home for
    the "probes are sticky through merges/groups" rule so each output-narrowing
    site enforces it identically."""
    have = {c.address for c in base}
    extra = unique(
        [
            c
            for c in candidates
            if is_presence_probe(c.address) and c.address not in have
        ],
        "address",
    )
    return base + extra if extra else base


def probe_member_address(
    probe_address: str, environment: BuildEnvironment
) -> str | None:
    """Recover which authored key-group member a presence probe pins.

    The probe's lineage argument is useless for this: build-time canonical
    substitution rewrote it to the group canonical, which every member's
    datasource binds identically. The probe NAME embeds the hash of the
    member's authored address (`Factory._coalescing_presence_probe`), so match
    it against the group members recorded on the build environment."""
    suffix = probe_address.rsplit(PRESENCE_PROBE_PREFIX, 1)[-1]
    for members in environment.scoped_join_key_groups.values():
        for member in members:
            if str(string_to_hash(member)) == suffix:
                return member
    return None


def member_binding_datasources(
    member_address: str, environment: BuildEnvironment
) -> list[BuildDatasource]:
    """Datasources that PHYSICALLY carry the member's authored column, best
    presence population first. After canonical substitution every group
    member's binding shares the canonical address, so side identity lives
    only in `origin_address` (set when a scoped relation substituted the
    binding) or in an unsubstituted exact address match.

    Ordering: a binding AT the member's own grain is its defining table
    (dimension PK) and spans the member's whole domain — probing it is a
    tautology whenever the relation's other side draws from that same domain
    (TPC-DS q84: `ss.return_customer_demographic.sk` is defined by
    customer_demographics but the presence question is about the FK carrier,
    store_returns). Prefer off-grain (FK-carrier) bindings; the defining
    binding is the fallback when the member has no carrier, where domain
    membership is the only readable population."""
    at_grain: list[BuildDatasource] = []
    off_grain: list[BuildDatasource] = []
    for datasource in environment.datasources.values():
        if not isinstance(datasource, BuildDatasource):
            continue
        for column in datasource.columns:
            origin = (
                column.origin_address
                if column.origin_address is not None
                else column.concept.address
            )
            if origin != member_address:
                continue
            # grain components may carry either address form (substitution
            # rewrites the column but can leave the authored grain address)
            if {column.concept.address, member_address} & datasource.grain.components:
                at_grain.append(datasource)
            else:
                off_grain.append(datasource)
            break
    return off_grain + at_grain


def co_declared_group_keys(
    member_address: str,
    datasource: BuildDatasource,
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Axis keys of OTHER statement-declared join-key groups with a member
    physically bound in `datasource`.

    A composite scoped join (`union join a.t = b.t and a.i = b.i`) declares
    presence at the compound (t, i) grain. A probe scan carrying only its own
    group's key groups the member's table to that single key and re-joins on
    it alone, silently coarsening "did (t, i) match?" to "did t match ANY
    row?" (TPC-DS q64 is_returned). Limited to statement-scoped declarations:
    a global `merge` is ambient identity, not a join constraint of this query,
    so a merged concept bound in the probed table must not narrow the probe."""
    statement_members = environment.domain_graph.statement_relation_members()
    out: list[BuildConcept] = []
    for canonical, members in sorted(environment.scoped_join_key_groups.items()):
        if member_address in members or canonical == member_address:
            continue
        if not members & statement_members:
            continue
        key = environment.concepts.get(canonical)
        if key is None:
            continue
        if any(
            datasource.name == d.name
            for m in sorted(members)
            for d in member_binding_datasources(m, environment)
        ):
            out.append(key)
    return out


def coalescing_axis_group(
    address: str, environment: BuildEnvironment
) -> tuple[str, set[str]] | None:
    """``(canonical, group)`` when `address` participates in a coalescing
    (`full`/`union`) key group — as its canonical or any member — else None.
    Under a coalescing declaration the unified axis is the union of the
    members' domains, so no single member's source can satisfy it. Member
    addresses matter for ROWSET members, which keep their own identity
    (requests for them are not canonicalized)."""
    groups = environment.scoped_join_key_groups
    if not groups:
        return None
    coalescing = environment.domain_graph.coalescing_relation_members()
    if not coalescing:
        return None
    group = groups.get(address)
    if group is not None:
        return (address, group) if group & coalescing else None
    for canonical, members in groups.items():
        if address in members and members & coalescing:
            return canonical, members
    return None


def _pinned_member_node(
    member_address: str,
    key: BuildConcept,
    environment: BuildEnvironment,
    depth: int,
) -> StrategyNode | None:
    """A scan of the member's own datasource producing the group key from the
    member's authored column, grouped to key grain."""
    candidates = member_binding_datasources(member_address, environment)
    if not candidates:
        return None
    if len(candidates) > 1:
        logger.info(
            f"{LOGGER_PREFIX} member {member_address} bound in multiple"
            f" datasources {[d.name for d in candidates]}; using the first"
        )
    node, force_group = create_datasource_node(
        candidates[0],
        [key],
        accept_partial=True,
        environment=environment,
        depth=depth + 1,
        conditions=None,
    )
    if not force_group:
        return node
    return GroupNode(
        output_concepts=node.output_concepts,
        input_concepts=node.output_concepts,
        environment=environment,
        parents=[node],
        depth=depth,
        partial_concepts=node.partial_concepts,
        nullable_concepts=node.nullable_concepts,
        force_group=True,
    )


def gen_coalescing_axis_node(
    concept: BuildConcept,
    environment: BuildEnvironment,
    depth: int,
    g=None,
    source_concepts=None,
    history: History | None = None,
) -> StrategyNode | None:
    """Materialize a coalescing (`full`/`union`) axis as the mandatory
    coalesce of EVERY group member's own side.

    Post-substitution a ROOT member's binding shares the canonical's address,
    so generic sourcing satisfies the axis from whichever single table scores
    best — silently projecting one member's domain as the unified axis. Build
    one side node per member — a scan pinned to the member's own datasource
    for bound (ROOT) members, the member's own sourcing (its rowset) for
    unbound ones — and merge them: the sides relate FULL (the union-key
    registry forbids narrowing) and render the coalesce.

    Deliberately NOT a completeness invariant: a query touching only one
    side's own attributes/predicates stays single-sourced (the author is
    querying that side; forcing the traversal is pure cost). This assembles
    the full axis only at the two sites that are ABOUT the axis: a bare axis
    projection and a presence probe's key (an `is null` probe's answer lives
    on the complement side)."""
    found = coalescing_axis_group(concept.address, environment)
    if found is None:
        return None
    canonical, group = found
    if history is not None:
        if canonical in history.coalescing_axis_in_progress:
            return None
        history.coalescing_axis_in_progress.add(canonical)
    try:
        key = environment.concepts.get(canonical) or concept
        sides: list[StrategyNode] = []
        for member in sorted(group):
            side = _pinned_member_node(member, key, environment, depth)
            if side is None and source_concepts is not None:
                # No datasource carries the member (rowset/derived): source
                # the member itself — its own scope materializes it, and the
                # in-progress guard above keeps that sourcing one-sided.
                member_concept = environment.concepts.get(member)
                if member_concept is not None:
                    side = source_concepts(
                        mandatory_list=[member_concept],
                        environment=environment,
                        g=g,
                        depth=depth + 1,
                        history=history,
                        conditions=None,
                    )
            if side is None:
                return None
            sides.append(side)
        if len(sides) < 2:
            return None
        logger.info(
            f"{LOGGER_PREFIX} assembling coalescing axis {concept.address} from"
            f" {len(sides)} member sides"
        )
        return MergeNode(
            input_concepts=unique(
                [concept] + [c for s in sides for c in s.output_concepts], "address"
            ),
            output_concepts=[concept],
            environment=environment,
            parents=sides,
            depth=depth,
            preserve_parents=True,
        )
    finally:
        if history is not None:
            history.coalescing_axis_in_progress.discard(canonical)


def _conditions_prove_probe_non_null(
    probe: BuildConcept, conditions: BuildWhereClause | None
) -> bool:
    """True when the query's WHERE provably filters out NULL values of the
    probe (`probe is not null` and negation-safe compositions) — the
    null-rejection oracle shared with join safety."""
    if conditions is None:
        return False
    return probe.address in condition_proves_non_null(conditions.conditional)


def gen_presence_probe_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Materialize a presence probe for a datasource-bound (ROOT) key-group
    member from the member's OWN datasource, pre-merge.

    The rowset analog computes the probe inside the member's rowset body
    (`_local_exposure_obligations`). A ROOT member has no such body: its
    binding was substituted onto the group canonical, so the generic BASIC
    path would source the probe's argument from whichever datasource scores
    best — including the OTHER side of the relation, whose key is present on
    exactly the rows the probe must flag as absent. Pin the scan to a
    datasource that physically carries the member's authored column and
    compute the probe there; it rides through the completion merge un-fused
    and is NULL exactly where the member's side is absent. Returns None when
    the member has no datasource binding (rowset/derived members keep their
    existing paths)."""
    member_address = probe_member_address(concept.address, environment)
    if member_address is None:
        return None
    key_args = concept.concept_arguments
    if len(key_args) != 1:
        return None
    key = key_args[0]
    candidates = member_binding_datasources(member_address, environment)
    if not candidates:
        return None
    if len(candidates) > 1:
        logger.info(
            f"{LOGGER_PREFIX} member {member_address} bound in multiple"
            f" datasources {[d.name for d in candidates]}; probing the first"
        )
    datasource = candidates[0]
    logger.info(
        f"{LOGGER_PREFIX} pinning probe {concept.address} for member"
        f" {member_address} to datasource {datasource.name}"
    )
    co_keys = co_declared_group_keys(member_address, datasource, environment)
    if co_keys:
        logger.info(
            f"{LOGGER_PREFIX} probe carries co-declared join keys"
            f" {[c.address for c in co_keys]} bound in {datasource.name}"
        )
    node, force_group = create_datasource_node(
        datasource,
        [key] + co_keys,
        accept_partial=True,
        environment=environment,
        depth=depth + 1,
        conditions=None,
    )
    node.add_output_concept(concept)
    pinned: StrategyNode = node
    if force_group:
        pinned = GroupNode(
            output_concepts=node.output_concepts,
            input_concepts=node.output_concepts,
            environment=environment,
            parents=[node],
            depth=depth,
            partial_concepts=node.partial_concepts,
            nullable_concepts=node.nullable_concepts,
            force_group=True,
        )
    parents: list[StrategyNode] = [pinned]
    # A coalescing (`full`/`union`) key: the pinned scan carries only its own
    # member's slice of the axis; merge in the full axis so this node doesn't
    # satisfy the axis address one-sided (an `is null` test's answer lives on
    # the complement side). Perf carve-out: when the statement WHERE provably
    # rejects NULL probes (`probe is not null` and negation-safe equivalents),
    # every surviving row has this member's side present, so the one-sided
    # axis is already complete for the result — skip the complement scans.
    # The loop sources sub-requests with conditions=None, so consult the
    # statement-level WHERE stashed on History; unknown stays conservative.
    statement_conditions = conditions or (
        history.statement_conditions if history is not None else None
    )
    axis: StrategyNode | None = None
    if not _conditions_prove_probe_non_null(concept, statement_conditions):
        axis = gen_coalescing_axis_node(
            key,
            environment,
            depth,
            g=g,
            source_concepts=source_concepts,
            history=history,
        )
    if axis is not None:
        parents.append(axis)
    remaining = [c for c in local_optional if c.address != key.address]
    if remaining:
        # Cover local_optional per the generator contract: the pinned scan can
        # never provide it (it carries only the group keys + probe), so source
        # the rest alongside the keys and join on them. Co-declared keys ride
        # along even when unrequested: without them the enrich side joins the
        # probe on the probed key alone, re-coarsening the compound grain.
        enrich = source_concepts(
            mandatory_list=unique([key] + co_keys + remaining, "address"),
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        if not enrich:
            return None
        parents.append(enrich)
    if len(parents) == 1:
        return pinned
    return MergeNode(
        input_concepts=unique(
            [c for p in parents for c in p.output_concepts], "address"
        ),
        output_concepts=unique([concept, key] + co_keys + remaining, "address"),
        environment=environment,
        parents=parents,
        depth=depth,
        preserve_parents=axis is not None,
    )
