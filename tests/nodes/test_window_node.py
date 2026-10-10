from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes.window_node import WindowNode


def test_window_node_copy():
    env = Environment()
    x = WindowNode(input_concepts=[], output_concepts=[], environment=env)
    x.origin_group = "grp:window:d0:x"

    y = x.copy()

    assert x.environment == y.environment
    assert y.origin_group == x.origin_group
