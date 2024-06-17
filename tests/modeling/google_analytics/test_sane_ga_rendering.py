from preql import parse, Dialects
from preql.executor import Executor
from pathlib import Path
from preql.core.models import (
    ProcessedQuery,
    SourceType,
    Environment,
    Concept,
    DataType,
    Function,
)
from preql.core.enums import Purpose, Granularity
from preql.core.functions import CurrentDatetime
from preql.hooks.query_debugger import DebuggingHook
from logging import INFO

ENVIRONMENT_CONCEPTS = [
    Concept(
        name="static",
        namespace="local",
        datatype=DataType.DATETIME,
        purpose=Purpose.CONSTANT,
        lineage=CurrentDatetime([]),
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
    statements[-1].select.selection.append(local_static)
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    # this should be a
    # constant node since we have constants
    assert select.ctes[0].source.source_type == SourceType.DIRECT_SELECT
    # select node for the data
    assert select.ctes[1].source.source_type == SourceType.GROUP
    # a group as groups happen without any constants
    assert select.ctes[2].source.source_type == SourceType.CONSTANT
    # merge node to get in constants
    assert select.ctes[3].source.source_type == SourceType.MERGE
    # assert select.ctes[2].source.source_type == SourceType.MERGE
    # assert len(select.ctes) == 5

    engine.generator.compile_statement(select)
    # this statement should have this structure

    # select node
    # group node
    # output


def test_daily_job():
    with open(Path(__file__).parent / "daily_analytics.preql") as f:
        sql = f.read()

    env, statements = parse(
        sql, environment=Environment(working_path=Path(__file__).parent)
    )
    enrich_environment(env)
    local_static = env.concepts["local.static"]
    assert local_static.granularity == Granularity.SINGLE_ROW

    case = env.concepts["all_sites.clean_url"]

    assert isinstance(case.lineage, Function)
    # assert set([x.address for x in case.lineage.concept_arguments]) == set([env.concepts["all_sites.page_location"].address])
    assert local_static.granularity == Granularity.SINGLE_ROW
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(INFO)]
    )
    statements[-1].select.selection.append(local_static)
    pstatements = engine.generator.generate_queries(env, statements)
    select: ProcessedQuery = pstatements[-1]
    engine.generator.compile_statement(select)


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
    assert ">= date_add(current_date(), INTERVAL 30 day)" in compiled
