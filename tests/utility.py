from logging import INFO

from trilogy.core.execute_models import BoundConcept, BoundEnvironment
from trilogy.core.processing.concept_strategies_v3 import source_query_concepts
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.nodes.group_node import GroupNode
from trilogy.hooks.query_debugger import DebuggingHook


def get_parents(node: StrategyNode):
    output = [node.__class__]
    for parent in node.parents:
        output = get_parents(parent) + output
    return output


def validate_shape(
    input: list[BoundConcept], environment: BoundEnvironment, g, levels, error: str | None = None
):
    """test that our query resolves to the expected CTES"""
    base: GroupNode = source_query_concepts(input, environment, g)
    DebuggingHook(INFO).process_root_strategy_node(base)
    final = get_parents(base)
    default_error = (
        "\n".join([str(x) for x in final])
        + "\nvs\n"
        + "\n".join([str(x) for x in levels])
    )

    assert final == levels, error or default_error
