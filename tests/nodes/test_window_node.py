from trilogy.core.env_processor import generate_graph
from trilogy.core.models import Environment
from trilogy.core.processing.nodes.window_node import WindowNode


def test_window_node_copy():
    env = Environment()
    x = WindowNode(
        input_concepts=[], output_concepts=[], environment=env, g=generate_graph(env)
    )

    y = x.copy()

    assert x.environment == y.environment
