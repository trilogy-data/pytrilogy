from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.models import Environment
from trilogy.core.env_processor import generate_graph


def test_base_node_copy():
    env = Environment()
    x = StrategyNode(
        input_concepts=[], output_concepts=[], environment=env, g=generate_graph(env)
    )

    y = x.copy()

    assert x.environment == y.environment
