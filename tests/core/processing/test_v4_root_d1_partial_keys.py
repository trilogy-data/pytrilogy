"""Aggregate input roots use the fact stream's partial key population."""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_TPCH = Path(__file__).parents[2] / "modeling" / "tpc_h"


def test_q20_d1_root_does_not_complete_part_key_from_dimension():
    environment = Environment(working_path=_TPCH)
    engine = Dialects.DUCK_DB.default_executor(environment=environment)
    sql = engine.generate_sql((_TPCH / "query20.preql").read_text())[-1]
    assert sql.count('"memory"."part" as "part_part"') == 1, sql
    assert '"part_part"."p_name" like \'forest%\'' in sql, sql
