from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionType
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.processing.nodes import History, StrategyNode, UnionNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_UNION_NODE]"


def is_union(c: BuildConcept):
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.UNION
    )


def build_layers(
    concepts: list[BuildConcept],
) -> tuple[list[list[BuildConcept]], list[BuildConcept]]:
    sources = {
        x.address: x.lineage.concept_arguments if x.lineage else [] for x in concepts
    }
    root = concepts[0]

    built_layers = []
    layers = root.lineage.concept_arguments if root.lineage else []
    sourced = set()
    while layers:
        layer = []
        current = layers.pop()
        sourced.add(current.address)
        layer.append(current)
        for key, values in sources.items():
            if key == current.address:
                continue
            for value in values:
                if value.address in (current.keys or []) or current.address in (
                    value.keys or []
                ):
                    layer.append(value)
                    sourced.add(value.address)
        built_layers.append(layer)
    complete = [
        x for x in concepts if all([x.address in sourced for x in sources[x.address]])
    ]
    return built_layers, complete


def gen_union_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    all_unions = [x for x in local_optional if is_union(x)] + [concept]
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} found unions {all_unions}")
    parent_nodes = []
    layers, resolved = build_layers(all_unions)
    for layer in layers:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching layer {layer} with resolved {resolved}"
        )
        parent: StrategyNode = source_concepts(
            mandatory_list=layer,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        parent.hide_output_concepts(parent.output_concepts)
        parent.add_output_concepts(resolved)
        parent_nodes.append(parent)
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not find union node parents"
            )
            return None

    return UnionNode(
        input_concepts=resolved,
        output_concepts=resolved,
        environment=environment,
        parents=parent_nodes,
    )
