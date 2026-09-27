import re
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent


def test_adhoc07():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc07.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    results = engine.generate_sql(text)[-1]
    assert re.search(
        r'INNER JOIN "(\w+)" on "order_orders"\."o_custkey" = "\1"\."order_customer_id"',
        results,
    ), results
