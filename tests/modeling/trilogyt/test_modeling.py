from pathlib import Path

from trilogy.core.models.environment import Environment

parent = Path(__file__).parent


def test_infinite_parsing():
    text = open(parent / "order.preql").read()
    env = Environment(working_path=parent)
    try:
        env.parse(text)
    except Exception:
        pass


def test_parsing_recursion():
    Environment().from_file(parent / "customer.preql")
    Environment().from_file(parent / "order.preql")
