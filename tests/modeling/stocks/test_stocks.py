from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment


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
        "holdings.provider.id",
        "symbol.id",
    }
    assert env.concepts["holdings.profitable"].grain.components == {
        "holdings.provider.id",
        "symbol.id",
    }
    profitable = env.concepts["holdings.profitable"]
    for x in profitable.lineage.concept_arguments:
        assert env.concepts[x.address].grain.components == {
            "holdings.provider.id",
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
