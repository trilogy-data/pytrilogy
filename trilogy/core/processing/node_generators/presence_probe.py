from trilogy.constants import PRESENCE_PROBE_PREFIX, logger
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.build_environment import BuildEnvironment
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
    (dimension PK) and spans the member's whole domain, so probing it is a
    tautology whenever the relation's other side draws from that same domain.
    Prefer off-grain (FK-carrier) bindings; the defining binding is the
    fallback when the member has no carrier."""
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


def coalescing_axis_group(
    address: str, environment: BuildEnvironment
) -> tuple[str, set[str]] | None:
    """``(canonical, group)`` when `address` participates in a coalescing
    (`full`/`union`) key group, as its canonical or any member, else None.
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
    so generic sourcing would satisfy the axis from whichever single table
    scores best, projecting one member's domain as the unified axis. Instead,
    build one side node per member (a scan pinned to the member's own
    datasource for bound ROOT members, the member's own sourcing for unbound
    ones) and merge them: the sides relate FULL and render the coalesce.

    Deliberately NOT a completeness invariant: a query touching only one
    side's own attributes stays single-sourced. The full axis is assembled
    only at the sites that are ABOUT the axis: a bare axis projection and a
    presence probe's key (an `is null` probe's answer lives on the complement
    side)."""
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
                # No datasource carries the member (rowset/derived): source the
                # member itself; the in-progress guard keeps that one-sided.
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
