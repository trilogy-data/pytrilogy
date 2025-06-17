from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.env_processor import generate_graph


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


from trilogy.core.processing.concept_strategies_v3 import (
    search_concepts,
    History,
    validate_stack,
    StrategyNode,
)
from trilogy.core.processing.nodes.select_node_v2 import SelectNode
from trilogy.core.models.build import Factory, BuildDatasource, BuildColumnAssignment
from trilogy.core.models.build_environment import BuildEnvironment
# SyntaxError: Missing source map entry for provider.name on input_concepts with pseudonyms {'holdings.provider.name', 'provider.name', 'provider.__pre_persist_name', 'dividend.provider.name'}, 
# have map: {'dividend.amount': {dividend.dividend_data_at_dividend_id_join_symbol.symbol_data_at_symbol_id_join_provider.provider_data_at_provider_id_at_provider_id_dividend_id_symbol_id_join_symbol.symbol_data_at_symbol_id_grouped_by_symbol.sector_at_symbol_sector_at_provider_id_dividend_id_symbol_id@<Grain<dividend.id,provider.id,symbol.id>>}, 
# 'dividend.id': {dividend.dividend_data_at_dividend_id_join_symbol.symbol_data_at_symbol_id_join_provider.provider_data_at_provider_id_at_provider_id_dividend_id_symbol_id_join_symbol.symbol_data_at_symbol_id_grouped_by_symbol.sector_at_symbol_sector_at_provider_id_dividend_id_symbol_id@<Grain<dividend.id,provider.id,symbol.id>>}, 'dividend.symbol.sector': {dividend.dividend_data_at_dividend_id_join_symbol.symbol_data_at_symbol_id_join_provider.provider_data_at_provider_id_at_provider_id_dividend_id_symbol_id_join_symbol.symbol_data_at_symbol_id_grouped_by_symbol.sector_at_symbol_sector_at_provider_id_dividend_id_symbol_id@<Grain<dividend.id,provider.id,symbol.id>>}, 
# 'dividend.provider.__pre_persist_name': {dividend.dividend_data_at_dividend_id_join_symbol.symbol_data_at_symbol_id_join_provider.provider_data_at_provider_id_at_provider_id_dividend_id_symbol_id_join_symbol.symbol_data_at_symbol_id_grouped_by_symbol.sector_at_symbol_sector_at_provider_id_dividend_id_symbol_id@<Grain<dividend.id,provider.id,symbol.id>>}, 'local.total_div': set()}


def test_provider_name():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    # test covering root cause of
    # INFO   [DISCOVERY LOOP] finished sourcing loop (complete: ValidationResult.INCOMPLETE), have {'dividend.amount', 'dividend.id', 'provider.id', 'symbol.sector'} from [MergeNode<dividend.amount,dividend.id,provider.id...1 more>]
    #  (missing {'dividend.symbol.sector'}), attempted {'dividend.amount'}, virtual set()
    DebuggingHook()
    build_env: BuildEnvironment = Factory(environment=env).build(env)
    assert 'dividend.symbol.sector' in build_env.materialized_concepts, build_env.materialized_concepts
    assert 'dividend.provider.__pre_persist_name' in build_env.concepts['provider.name'].pseudonyms, build_env.concepts['provider.name'].pseudonyms
    assert 'provider.id' in build_env.alias_origin_lookup['dividend.provider.id'].pseudonyms, build_env.alias_origin_lookup['dividend.provider.id'].pseudonyms
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
                                BuildColumnAssignment(alias=c.name, concept=c)
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
    print(complete, found_c, missing_c, partial, virtual)
    assert not partial, partial
    assert not missing_c, missing_c


    duckdb = Dialects.DUCK_DB.default_executor(environment=env)
    duckdb.parse_text(
        """select
    symbol.sector,
    provider.name,

    sum(dividend.amount) as total_div;
  """
    )

    sql = duckdb.generate_sql(
        """select
    symbol.sector,
    provider.name,
    sum(dividend.amount) as total_div;
  """
    )
    assert sql == '123', sql


def test_filter():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    
    sql = duckdb.generate_sql(
        """import entrypoint;
     where symbol.class = 'STOCK'
  and (
    symbol.industry like '%solar%'
    or symbol.sector like '%solar%'
  )
    select 1 as test_filter;"""
    )