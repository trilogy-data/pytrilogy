"""The planner never joins a FINAL contributor it reads no column from.

Election picks a contributor per mandatory concept off `output_concepts`;
join-key materialization then widens a sibling PAST that projection boundary,
which can leave the elected contributor rendering nothing at all.
`_fold_covered_contributors` drops it before the merge node is built, so the
join is never added rather than deleted afterwards -- the reason no optimizer
rule needs to sweep these up any more. Asserted structurally: every CTE the
final statement joins must appear in its projection.
"""

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.engine.test_duckdb_partial_fk_field_report import MODEL, QUERY
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper.strategy_builder import (
    _fold_covered_contributors,
)
from trilogy.dialect.config import DuckDBConfig

THELOOK = Path(__file__).parents[1] / "modeling" / "thelook_duckdb"


def _unread_joins(sql: str) -> set[str]:
    """Names the final statement joins but never reads a column from. An
    inlined datasource renders under its alias, so that is the name to match."""
    final = sql[sql.rindex("SELECT") :]
    projection, _, source = final.partition("\nFROM\n")
    joined = {
        alias or name
        for name, alias in re.findall(r'JOIN "(\w+)"(?: as "(\w+)")?', source)
    }
    return joined - set(re.findall(r'"(\w+)"\.', projection))


def test_field_report_joins_nothing_it_does_not_read(tmp_path):
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(tmp_path)), conf=DuckDBConfig()
    )
    engine.parse_text(MODEL)
    sql = engine.generate_sql(QUERY)[-1]

    assert not _unread_joins(sql), sql
    assert "is not distinct from" not in sql, sql


@pytest.mark.parametrize("name", ["adhoc01", "adhoc02", "adhoc03", "adhoc04"])
def test_thelook_adhoc_joins_nothing_it_does_not_read(name: str):
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=THELOOK)
    )
    sql = engine.generate_sql((THELOOK / f"{name}.preql").read_text())[-1]

    assert not _unread_joins(sql), sql


def _stub(grain: set[str]) -> SimpleNamespace:
    return SimpleNamespace(
        parents=[],
        partial_concepts=[],
        region_spans=frozenset(),
        region_boundary=False,
        conditions=None,
        existence_concepts=[],
        limit=None,
        resolve=lambda: SimpleNamespace(grain=SimpleNamespace(components=grain)),
    )


def test_a_covered_contributor_folds_on_what_its_siblings_are_stated_to_hold() -> None:
    environment = SimpleNamespace(all_scoped_join_group_members=frozenset)
    lookup, rows = _stub({"customer_id"}), _stub({"order_id"})
    parents = [lookup, rows]
    args = (environment, {"customer_id", "order_id"}, {id(lookup)}, set())
    owed = [{"customer_id"}, {"order_id", "customer_id"}]
    assert _fold_covered_contributors(parents, *args, owed) == [rows]
    short = [{"customer_id"}, {"order_id"}]
    assert _fold_covered_contributors(parents, *args, short) == parents
