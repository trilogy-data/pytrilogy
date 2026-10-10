"""The optimizer repeats its rule plan until a pass changes nothing, so a
phase that leaves work for an earlier one needs no hand-wired re-fire."""

import logging
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.constants import logger
from trilogy.core import optimization

MODELING = Path(__file__).parent.parent / "modeling"


def _sql(folder: str, file: str) -> str:
    path = MODELING / folder
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=path)
    )
    return executor.generate_sql((path / file).read_text())[-1]


def test_ratio_over_group_folds_without_a_refire():
    sql = _sql("ncaa", "adhoc03.preql")
    assert sql.count("GROUP BY") == 1, sql
    assert "/ count(distinct CASE" in sql, sql


def test_every_where_atom_moves_into_having_in_one_pushdown(monkeypatch, caplog):
    monkeypatch.setattr(optimization, "MAX_OPTIMIZATION_PASSES", 3)
    logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger=logger.name):
            sql = _sql("tpc_ds_duckdb", "query04.preql")
    finally:
        logger.removeHandler(caplog.handler)
    assert "still changing" not in caplog.text, caplog.text
    assert sql.count(" as (") == 2, sql
