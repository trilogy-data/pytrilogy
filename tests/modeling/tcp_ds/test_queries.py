from trilogy import parse
from pathlib import Path

from trilogy import Environment, Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.core.processing.nodes import SelectNode, MergeNode, GroupNode
from trilogy.core.env_processor import generate_graph
from tests.utility import validate_shape

working_path = Path(__file__).parent
test = working_path / "store.preql"


def test_one():
    env = Environment(working_path=working_path)
    with open(test) as f:
        text = f.read()
        env, queries = parse(text, env)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(process_other=False, process_ctes=False)]
    )

    # Grain<returns.customer.id,returns.store.id,returns.item.id,returns.store_sales.ticket_number>
    # Grain<returns.customer.id,returns.store.id,returns.return_date.id,returns.item.id,returns.store_sales.ticket_number>
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

    parsed = exec.parse_text(
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

    validate_shape(
        parsed[-1].output_columns,
        env,
        g,
        levels=[
            SelectNode,
            SelectNode,  # select store
            SelectNode,  # select store
            SelectNode,  # select year
            MergeNode,  # merge year into fact
            MergeNode,
            MergeNode,  # merge year into fac
            GroupNode,  # calculate aggregate
            MergeNode,  # enrich store name
            GroupNode,  # final node
            # MergeNode,  # enrich store name
            # GroupNode,  # final node
        ],
    )

    sql = exec.generate_sql(
        """import customer as customer;
import store as store;
import store_returns as returns;

# query 1
rowset ca_2022 <-select
    returns.customer.id,
    returns.store.id,
    returns.store.state,
    returns.return_date.year,
    sum(returns.return_amount)-> total_returns
where
    returns.return_date.year = 2002
    and returns.store.state = 'CA';

auto avg_store_returns <- avg(ca_2022.total_returns) by ca_2022.returns.store.id;

select
    ca_2022.returns.customer.id,
    -- ca_2022.total_returns,
    -- avg_store_returns,
where
    ca_2022.total_returns > (1.2*avg_store_returns)
order by ca_2022.total_returns desc
limit 100;"""
    )
    assert "SELECT" in sql[-1]
    # check that our casts returned properly
    assert "INVALID_ALIAS" not in sql[-1]


def test_three():
    env = Environment(working_path=working_path)
    with open(working_path / "query3.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    select = queries[-1]

    # g = generate_graph(env)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(process_other=False, process_ctes=False)]
    )
    sql = exec.generate_sql(select)
    assert "SELECT" in sql[-1]
    # assert sql[0] == '123'


def test_three_alt():
    env = Environment(working_path=working_path)
    with open(working_path / "query3_alt.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    select = queries[-1]

    # g = generate_graph(env)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(process_other=False, process_ctes=False)]
    )
    sql = exec.generate_sql(select)
    assert "SELECT" in sql[-1]
    # assert sql[0] == '123'
