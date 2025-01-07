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
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
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
        assert x.grain.components == {
            "holdings.provider.id",
            "symbol.id",
        }, profitable.lineage

    duckdb = Dialects.DUCK_DB.default_executor(environment=env)

    _ = duckdb.generate_sql(
        """SELECT
    holdings.profitable,
WHERE
    holdings.profitable
;
    """
    )
