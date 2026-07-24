
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_CONSTANT_NODE]"


def gen_constant_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
):
    """our only goal here is to generate a row if conditions exist, or none if they do not"""

    local_addrs = {c.address for c in local_optional}
    condition_addrs: set[str] = set()
    if conditions:
        condition_addrs = {c.address for c in conditions.row_arguments}
    # If conditions reference concepts not in local_optional, the discovery
    # loop needs the constant `concept` in targets so the connectedness check
    # ties the constant projection to the filter source (otherwise the source
    # for the filter concept never makes it into the rendered FROM clause).
    keep_concept = bool(condition_addrs - local_addrs)
    targets = [*local_optional]
    if conditions:
        targets += conditions.row_arguments
    if keep_concept:
        targets = [concept] + targets
    targets = list(unique(targets, "address"))
    if not keep_concept:
        targets = [t for t in targets if t.address != concept.address]
    if not targets:
        targets = [concept]
    parent_node: StrategyNode | None = source_concepts(
        mandatory_list=targets,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
        accept_partial=accept_partial,
    )
    if not parent_node:
        return None
    parent_node.set_output_concepts([concept] + local_optional)
    return parent_node
