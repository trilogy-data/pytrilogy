"""A PERSIST that drops the key a concept is identified by must not then be
used to answer a query needing that key.

`split <- unnest(int_array)` carries keys={scalar}. Persisting `select split`
alone materializes a table with no `scalar` column, so it cannot rejoin. The
planner used to accept it as a component source anyway and cross-join it to the
scalar side, turning 4 rows into 8. `test_complex` missed this because its
fixture holds exactly one scalar row, where a cartesian and a join agree.
"""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

GENERIC = """key scalar int;
property scalar.int_array list<int>;
auto split <- unnest(int_array);

datasource avalues (
    int_array:int_array,
    scalar:scalar
)
grain (scalar)
query '''
select [1,2] as int_array, 2 as scalar
union all select [3,4], 5
''';
"""

PERSIST_SPLIT_ONLY = """import generic as generic;

PERSIST split_only INTO split_only FROM SELECT
    generic.split,;
"""

QUERY = """select generic.split, generic.scalar
order by generic.split asc, generic.scalar asc;"""

EXPECTED = [(1, 2), (2, 2), (3, 5), (4, 5)]


def _model(tmp_path: Path) -> Path:
    (tmp_path / "generic.preql").write_text(GENERIC, encoding="utf-8")
    (tmp_path / "optimize.preql").write_text(PERSIST_SPLIT_ONLY, encoding="utf-8")
    return tmp_path


def test_multi_row_baseline(tmp_path: Path) -> None:
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=_model(tmp_path))
    )
    engine.execute_text("import generic as generic;")
    rows = sorted(tuple(r) for r in engine.execute_text(QUERY)[-1].fetchall())
    assert rows == EXPECTED


def test_key_dropping_persist_does_not_cross_join(tmp_path: Path) -> None:
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=_model(tmp_path))
    )
    engine.execute_file(tmp_path / "optimize.preql")
    sql = engine.generate_sql(QUERY)[-1]
    assert " on 1=1" not in sql.lower()
    rows = sorted(tuple(r) for r in engine.execute_text(QUERY)[-1].fetchall())
    assert rows == EXPECTED
