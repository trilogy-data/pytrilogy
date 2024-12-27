from trilogy.core.models import BoundEnvironment
from trilogy.core.processing.nodes.window_node import WindowNode


def test_window_node_copy():
    env = BoundEnvironment()
    x = WindowNode(input_concepts=[], output_concepts=[], environment=env)

    y = x.copy()

    assert x.environment == y.environment
