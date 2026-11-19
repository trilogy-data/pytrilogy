"""A managed parquet write stores each numeric column at its declared kind, so
the rebuilt file matches its declaration and the next refresh converges."""

import duckdb

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.execution.state.state_store import refresh_stale_assets
from trilogy.execution.state.watermarks import schema_mismatch

MODEL = """key row_id int;
property row_id.grp string;
property row_id.x float;
property row_id.whole int;

root datasource rows (row_id, grp, x, whole)
grain (row_id)
query '''
select * from (values (1, 'a', 1.0, 1.0::double), (2, 'a', 2.0, 2.0::double),
(3, 'b', 3.0, 3.0::double)) t(row_id, grp, x, whole)
''';

auto n <- count(x) by grp;
auto total <- count(x) by *;
auto x_sum <- sum(whole) by grp;

datasource fits (grp, n, total, x_sum)
grain (grp)
file `{target}`;
"""


def _types(path) -> dict[str, str]:
    con = duckdb.connect()
    try:
        rows = con.execute(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        con.close()


def test_a_reaggregated_count_is_written_as_its_declared_int(tmp_path):
    target = tmp_path / "fits.parquet"
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(MODEL.format(target=target.as_posix()))

    assert refresh_stale_assets(executor).refreshed_count == 1
    assert _types(target) == {
        "grp": "VARCHAR",
        "n": "BIGINT",
        "total": "BIGINT",
        "x_sum": "BIGINT",
    }
    assert schema_mismatch(executor.environment.datasources["fits"], executor) is None
    assert refresh_stale_assets(executor).refreshed_count == 0
