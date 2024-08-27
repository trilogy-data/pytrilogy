from trilogy.core.models import Concept, Environment
from trilogy.core.processing.nodes.group_node import GroupNode
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.concept_strategies_v3 import source_query_concepts
from trilogy.hooks.query_debugger import DebuggingHook
from logging import INFO


def get_parents(node: StrategyNode):
    output = [node.__class__]
    for parent in node.parents:
        output = get_parents(parent) + output
    return output


def validate_shape(input: list[Concept], environment: Environment, g, levels):
    """test that our query resolves to the expected CTES"""
    base: GroupNode = source_query_concepts(input, environment, g)
    DebuggingHook(INFO).process_root_strategy_node(base)
    final = get_parents(base)
    assert final == levels, (
        "\n".join([str(x) for x in final])
        + "\nvs\n"
        + "\n".join([str(x) for x in levels])
    )
