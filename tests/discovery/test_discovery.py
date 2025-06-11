from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import BuildComparison, BuildWhereClause, Factory
from trilogy.core.processing.concept_strategies_v3 import History
from trilogy.core.query_processor import generate_graph
from trilogy.hooks.query_debugger import DebuggingHook

working_dir = Path(__file__).parent


def test_history():
    env = Environment(working_path=working_dir).from_file(working_dir / "inputs.preql")
    build_env = Factory(environment=env).build(env)
    graph = generate_graph(build_env)

    history = History(base_environment=env)
    targets = ["local.customer_id", "local.total_customer_revenue"]
    concepts = [build_env.concepts[x] for x in targets]
    generation = history.gen_select_node(
        concepts=concepts,
        environment=build_env,
        g=graph,
        depth=0,
        fail_if_not_found=True,
        accept_partial=False,
        conditions=None,
    )
    assert generation is not None, "Generation should not be None"
    assert (
        generation.parents == []
    ), "Parents should be empty for the initial generation"
    final = generation.resolve()
    assert final.datasources[0].name == "customer_revenue"


def test_history_partial():
    env = Environment(working_path=working_dir).from_file(working_dir / "inputs.preql")
    build_env = Factory(environment=env).build(env)
    graph = generate_graph(build_env)

    history = History(base_environment=env)
    targets = ["local.customer_id", "local.total_customer_revenue"]
    concepts = [build_env.concepts[x] for x in targets]
    target = BuildWhereClause(
        conditional=BuildComparison(
            left=build_env.concepts["local.customer_id"],
            right=2,
            operator=ComparisonOperator.EQ,
        )
    )
    generation = history.gen_select_node(
        concepts=concepts,
        environment=build_env,
        g=graph,
        depth=0,
        fail_if_not_found=True,
        accept_partial=False,
        conditions=target,
    )
    assert generation is not None, "Generation should not be None"
    assert (
        generation.parents == []
    ), "Parents should be empty for the initial generation"
    final = generation.resolve()
    assert final.datasources[0].name == "customer_revenue_for_two"

    assert (
        generation.preexisting_conditions is not None
    ), "Conditions should not be None"
    assert generation.preexisting_conditions == target.conditional


def test_history_e2e():
    env = Environment(working_path=working_dir).from_file(working_dir / "inputs.preql")
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    DebuggingHook()

    cmd = exec.generate_sql(
        """where customer_id = 2
        select local.customer_id, local.total_customer_revenue;
        
        """
    )[-1]
    assert (
        cmd.strip()
        == """SELECT
    "customer_revenue_for_two"."customer_id" as "customer_id",
    "customer_revenue_for_two"."total_customer_revenue" as "total_customer_revenue"
FROM
    (
select
    2 as customer_id,
    11.03 as total_customer_revenue
) as "customer_revenue_for_two" """.strip()
    )
