from pathlib import Path

from pytest import raises

from trilogy import Dialects
from trilogy.core.exceptions import NoDatasourceException
from trilogy.core.models.build import BuildColumnAssignment, BuildDatasource, Factory
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import (
    StrategyNode,
    validate_stack,
)
from trilogy.core.processing.nodes.select_node_v2 import SelectNode


def test_query():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")

    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    _ = duckdb.generate_sql(
        """SELECT
    symbol.sector,
    symbol.industry,
    sum(holdings.qty) as total_holding_qty,
    sum(holdings.value) as total_holding_value,
    sum(dividend.amount) as total_dividend
order by
    total_holding_qty desc
;
    """
    )


def test_import():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")

    assert env.concepts["holdings.cost_basis"].grain.components == {
        "provider.id",
        "symbol.id",
    }
    assert env.concepts["holdings.profitable"].grain.components == {
        "provider.id",
        "symbol.id",
    }
    profitable = env.concepts["holdings.profitable"]
    for x in profitable.lineage.concept_arguments:
        assert env.concepts[x.address].grain.components == {
            "provider.id",
            "symbol.id",
        }, profitable.lineage

    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    _ = duckdb.generate_sql(
        """SELECT
    holdings.profitable,
    holdings.return,
WHERE
    holdings.profitable
;
    """
    )


def test_query_results():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)
    duckdb.parse_text(
        """import dividend as dividend;
import symbol as symbol;
import provider as provider;
import holdings as holdings;
import std.display;

merge dividend.symbol.* into ~symbol.*;
merge holdings.symbol.* into ~symbol.*;

# analyze which stocks had the highest return

select
  symbol.ticker,
  symbol.name,
  symbol.sector,
  holdings.return
order by
  holdings.return desc
limit 10;
  """
    )

    duckdb.generate_sql(
        """import dividend as dividend;
import symbol as symbol;
import provider as provider;
import holdings as holdings;
import std.display;

merge dividend.symbol.* into ~symbol.*;
merge holdings.symbol.* into ~symbol.*;

# analyze which stocks had the highest return

select
  symbol.ticker,
  symbol.name,
  symbol.sector,
  holdings.return
order by
  holdings.return desc
limit 10;
  """
    )


def test_provider_name():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook
    from logging import INFO

    # test covering root cause of
    # INFO   [DISCOVERY LOOP] finished sourcing loop (complete: ValidationResult.INCOMPLETE), have {'dividend.amount', 'dividend.id', 'provider.id', 'symbol.sector'} from [MergeNode<dividend.amount,dividend.id,provider.id...1 more>]
    #  (missing {'dividend.symbol.sector'}), attempted {'dividend.amount'}, virtual set()
    DebuggingHook(INFO)
    build_env: BuildEnvironment = Factory(environment=env).build(env)
    assert (
        "dividend.symbol.sector" in build_env.materialized_concepts
    ), build_env.materialized_concepts
    assert (
        "provider.id"
        in build_env.alias_origin_lookup["dividend.provider.id"].pseudonyms
    ), build_env.alias_origin_lookup["dividend.provider.id"].pseudonyms
    test_concepts = [
        build_env.concepts["dividend.amount"],
        build_env.concepts["dividend.id"],
        build_env.concepts["provider.id"],
        build_env.concepts["symbol.sector"],
    ]
    target_concepts = [
        build_env.concepts["dividend.amount"],
        build_env.concepts["dividend.id"],
        build_env.concepts["provider.id"],
        build_env.concepts["holdings.symbol.sector"],
    ]
    complete, found_c, missing_c, partial, virtual = validate_stack(
        concepts=test_concepts,
        stack=[
            StrategyNode(
                input_concepts=test_concepts,
                output_concepts=test_concepts,
                environment=build_env,
                parents=[
                    SelectNode(
                        input_concepts=test_concepts,
                        output_concepts=test_concepts,
                        environment=build_env,
                        datasource=BuildDatasource(
                            name="dummy",
                            address="dummy",
                            columns=[
                                BuildColumnAssignment(alias=c.address, concept=c)
                                for c in test_concepts
                            ],
                        ),
                    )
                ],
            )
        ],
        mandatory_with_filter=target_concepts,
        environment=build_env,
    )

    assert not partial, partial
    assert not missing_c, missing_c

    duckdb = Dialects.DUCK_DB.default_executor(environment=env)
    #     duckdb.parse_text(
    #         """select
    #     symbol.sector,
    #     provider.name,

    #     sum(dividend.amount) as total_div;
    #   """
    #     )
    build_concept = build_env.concepts["provider.name"]
    assert build_concept.address in build_env.materialized_concepts
    assert build_concept.canonical_address in build_env.canonical_concepts
    assert build_concept.canonical_address in build_env.non_partial_materialized_canonical_concepts
    sql = duckdb.generate_sql(
        """select
    symbol.sector,
    provider.name,
    sum(dividend.amount) as total_div;
  """
    )[0]
    assert "reference" not in sql.lower(), sql
    assert sql.count("JOIN") == 3, sql


def test_filter():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    with raises(NoDatasourceException):
        duckdb.generate_sql(
            """import entrypoint;
        where symbol.class = 'STOCK'
    and (
        symbol.industry like '%solar%'
        or symbol.sector like '%solar%'
    )
        select 1 as test_filter;"""
        )


def test_filter_sector():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    sql = duckdb.generate_sql(
        """

where symbol.sector in ('Energy', 'Materials', 'Utilities') 
  or symbol.industry in ('Oil & Gas', 'Coal', 'Mining', 'Paper & Forest Products', 'Chemicals')
  or symbol.name like '%Oil%' 
  or symbol.name like '%Coal%' 
select
    symbol.sector,
    sum(dividend.amount) as total_dividends,
order by
    total_dividends desc
limit 100;

                              """
    )[0]
    assert "reference" not in sql.lower(), sql


def test_filter_sector_two():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    sql = duckdb.generate_sql(
        """
where symbol.sector in ('Energy', 'Materials', 'Utilities') 
  or symbol.industry in ('Oil & Gas', 'Coal', 'Mining', 'Paper & Forest Products', 'Chemicals')
  or symbol.name like '%Oil%' 
  or symbol.name like '%Coal%'
  select
    symbol.sector,
    provider.name,
    sum(dividend.amount) as total_div;
    """
    )[0]
    assert "reference" not in sql.lower(), sql


def test_bind_filter_reassignment():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    sql = duckdb.generate_sql(
        """
SELECT
    holdings.provider.name,
    count(holdings.symbol.id) as  holding_count,
;
    """
    )[0]
    assert "dividend" not in sql.lower(), sql


def test_bind_filter_reassignment_two():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    sql = duckdb.generate_sql(
        """
SELECT
    provider.name,
    count(holdings.symbol.id) as  holding_count,
    sum(holdings.value) as  holding_value
;
    """
    )[0]
    assert "dividend" not in sql.lower(), sql


def test_calculated_field():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    sql = duckdb.generate_sql(
        """
SELECT
    --provider.id,
    provider.name,
    symbol.country,
	symbol.city,
	symbol.latitude,
	symbol.longitude,
	symbol.state,
        sum(holdings.value) as  holding_value,
    count(holdings.symbol.id) as  holding_count,

;  """
    )[0]

    # assert env.concepts['provider.name'].grain.components == {
    #     "provider.id",
    #     "symbol.id"
    # }
    assert (
        """STRING_SPLIT( "holdings_symbol_cities"."state_iso_code" , '-' )[1]""" in sql
    ), sql
    # we should have 3 selects, because both aggregates can get merged
    assert sql.count("SELECT") == 3, sql.count("SELECT")
