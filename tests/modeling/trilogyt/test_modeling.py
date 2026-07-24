from pathlib import Path

from trilogy.core.models.environment import Environment

parent = Path(__file__).parent


def test_infinite_parsing():
    with open(parent / "order.preql") as f:
        text = f.read()
    env = Environment(working_path=parent)
    try:
        env.parse(text)
    except Exception:  # noqa: S110 -- only testing for hang/recursion, not parse result
        pass


def test_parsing_recursion():
    Environment().from_file(parent / "customer.preql")
    Environment().from_file(parent / "order.preql")
