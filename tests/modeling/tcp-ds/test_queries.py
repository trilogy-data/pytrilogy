from preql import parse
from pathlib import Path

from preql import Environment
from preql import Dialects
from preql.core.env_processor import generate_graph
from preql.hooks.query_debugger import DebuggingHook
from preql.core.processing.node_generators import gen_select_node

working_path = Path(__file__).parent
test = working_path / "store.preql"


def test_one():
    env = Environment(working_path=working_path)

    with open(test) as f:
        text = f.read()
        env, queries = parse(text, env)

    exec = Dialects.DUCK_DB.default_executor(environment=env, hooks=[DebuggingHook()])

    env, queries = parse("""import store_returns as returns;""", env)

    for k, c in env.concepts.items():
        if c.namespace == "local":
            continue
        if c.address != k:
            raise ValueError(f"{c.address} != {k} for {c.address}")
        if c.address not in env.concepts:
            raise ValueError(f"{c.address} not in env.concepts")
    assert "returns.return_date.date" in env.datasources
    found = False
    for c in env.datasources["returns.return_date.date"].output_concepts:
        assert c.namespace == "returns.return_date"
        if c.address == "returns.return_date.year":
            found = True
    assert found
    assert env.concepts["returns.return_date.year"] in env.materialized_concepts
    assert len(env.datasources["returns.store_returns"].concepts) == 7
    assert (
        len(
            list(
                set(
                    x.address for x in env.datasources["returns.store_returns"].concepts
                )
            )
        )
        == 7
    )
    g = generate_graph(env)

    node = gen_select_node(
        env.concepts["returns.return_amount"],
        [env.concepts["returns.store.id"]],
        accept_partial=True,
        environment=env,
        g=g,
        depth=0,
    )
    assert len(node.output_concepts) == 2

    sql = exec.generate_sql(
        """select
    returns.customer.id,
    returns.store.id,
    returns.store.state,
    returns.return_date.year,
    sum(returns.return_amount)-> total_returns
where
    returns.return_date.year = 2022
    and returns.store.state = 'CA';"""
    )
    assert "select" in sql


if __name__ == "__main__":
    test_one()
