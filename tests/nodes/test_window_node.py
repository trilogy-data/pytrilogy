from trilogy.core.models_environment import Environment
from trilogy.core.processing.nodes.window_node import WindowNode


def test_window_node_copy():
    env = Environment()
    x = WindowNode(input_concepts=[], output_concepts=[], environment=env)

    y = x.copy()

    assert x.environment == y.environment
