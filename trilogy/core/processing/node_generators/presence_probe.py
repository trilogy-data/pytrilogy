from trilogy.constants import PRESENCE_PROBE_PREFIX, logger
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildWhereClause
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
    node, force_group = create_datasource_node(
        datasource,
        [key],
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
    remaining = [c for c in local_optional if c.address != key.address]
    if not remaining:
        return pinned
    # Cover local_optional per the generator contract: the pinned scan can
    # never provide it (it carries only the group key + probe), so source the
    # rest alongside the key and join on it.
    enrich = source_concepts(
        mandatory_list=unique([key] + remaining, "address"),
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich:
        return None
    return MergeNode(
        input_concepts=unique(
            pinned.output_concepts + enrich.output_concepts, "address"
        ),
        output_concepts=unique([concept, key] + remaining, "address"),
        environment=environment,
        parents=[pinned, enrich],
        depth=depth,
    )
