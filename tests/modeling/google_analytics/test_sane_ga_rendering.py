from logging import INFO
from pathlib import Path

from trilogy import Dialects, parse
from trilogy.core.enums import Granularity, Purpose, Derivation
from trilogy.core.functions import CurrentDatetime
from trilogy.core.models.author import Concept, Function
from trilogy.core.models.core import (
    DataType,
)
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.statements.author import SelectItem
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook

ENVIRONMENT_CONCEPTS = [
    Concept(
        name="static",
        namespace="local",
        datatype=DataType.DATETIME,
        purpose=Purpose.CONSTANT,
        lineage=CurrentDatetime([]),
        granularity = Granularity.SINGLE_ROW,
        derivation = Derivation.CONSTANT
    )
]


def enrich_environment(env: Environment):
    for concept in ENVIRONMENT_CONCEPTS:
        env.add_concept(concept)
    return env


def test_sane_rendering():
    with open(Path(__file__).parent / "daily_visits.preql") as f:
        sql = f.read()

    env, statements = parse(
        sql, environment=Environment(working_path=Path(__file__).parent)
    )
    enrich_environment(env)
    local_static = env.concepts["local.static"]
    assert local_static.granularity == Granularity.SINGLE_ROW
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(INFO)]
    )
    statements[-1].select.selection.append(SelectItem(content=local_static))
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    # this should be a
    # constant node since we have constants
    # assert select.ctes[0].source.source_type == SourceType.DIRECT_SELECT
    # basic derivation
    # assert select.ctes[1].source.source_type == SourceType.MERGE

    # select node for the data
    # assert select.ctes[2].source.source_type == SourceType.GROUP
    # a group as groups happen without any constants
    # assert select.ctes[2].source.source_type == SourceType.CONSTANT
    # merge node to get in constants
    # assert select.ctes[3].source.source_type == SourceType.MERGE
    # assert select.ctes[2].source.source_type == SourceType.MERGE
    # assert len(select.ctes) == 5

    _ = engine.generator.compile_statement(select)
    # this statement should have this structure
    # selectnode
    # group node
    # output


def test_daily_job():
    with open(Path(__file__).parent / "daily_analytics.preql") as f:
        sql = f.read()

    env, statements = parse(
        sql, environment=Environment(working_path=Path(__file__).parent)
    )
    enrich_environment(env)
    env = env.materialize_for_select()
    local_static = env.concepts["local.static"]
    assert local_static.granularity == Granularity.SINGLE_ROW

    case = env.concepts["all_sites.clean_url"]

    assert isinstance(case.lineage, Function)
    assert local_static.granularity == Granularity.SINGLE_ROW

    for x in case.lineage.concept_arguments:
        test = case.lineage.with_namespace("all_sites")
        for y in test.concept_arguments:
            assert y.namespace == "all_sites"
        assert x.namespace == "all_sites", type(case.lineage)

    parents = resolve_function_parent_concepts(case, environment=env)
    for x in parents:
        assert x.namespace == "all_sites"

    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(INFO)]
    )
    statements[-1].select.selection.append(SelectItem(content=local_static))
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    _ = engine.generator.compile_statement(select)


def test_rolling_analytics():
    with open(Path(__file__).parent / "rolling_analytics.preql") as f:
        sql = f.read()

    env, statements = parse(
        sql, environment=Environment(working_path=Path(__file__).parent)
    )

    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[
            DebuggingHook(
                INFO, process_other=False, process_ctes=False, process_datasources=False
            )
        ],
    )
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    compiled = engine.generator.compile_statement(select)

    # make sure we got the where clause
    assert ">= date_add(current_date(), INTERVAL -30 day)" in compiled, compiled


def test_counts():
    with open(Path(__file__).parent / "daily_visits.preql") as f:
        sql = f.read()

    env, statements = parse(
        sql, environment=Environment(working_path=Path(__file__).parent)
    )
    enrich_environment(env)
    env = env.materialize_for_select()
    local_static = env.concepts["local.static"]
    assert local_static.granularity == Granularity.SINGLE_ROW

    case = env.concepts["all_sites.clean_url"]

    assert isinstance(case.lineage, Function)
    assert local_static.granularity == Granularity.SINGLE_ROW

    for x in case.lineage.concept_arguments:
        test = case.lineage.with_namespace("all_sites")
        for y in test.concept_arguments:
            assert y.namespace == "all_sites"
        assert x.namespace == "all_sites", type(case.lineage)

    parents = resolve_function_parent_concepts(case, environment=env)
    for x in parents:
        assert x.namespace == "all_sites"

    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(INFO)]
    )
    statements[-1].select.selection.append(SelectItem(content=local_static))
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    comp = engine.generator.compile_statement(select)
    assert '"all_sites__row_id" =' not in comp
