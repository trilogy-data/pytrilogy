from trilogy import Environment, Dialects
from pathlib import Path


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
