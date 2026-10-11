"""A datasource's `where` gates its build as a HAVING: the build writes exactly
the rows a read of the table keeps, and every column keeps the value its
concept has over the whole population. Pruning rows never recomputes an
aggregate column over the survivors. The `where` reads only what the columns
resolve: a stored column or a row-level derivation of them.

Group `a` holds ids 1 and 2 (total 30), `b` holds 3 (5), `c` holds 4 (7).
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.parsing.v2.model import HydrationError

SOURCES = """
key id int;
property id.grp string;
property id.v int;
property id.flag bool;
property id.home string;
auto grp_total <- sum(v) by grp;
auto grp_rows <- count(id) by grp;
auto anchor <- min(id) by grp;
auto is_anchor <- id = anchor;

datasource rows (id: id, grp: grp, v: v, flag: flag, home: home)
grain (id)
query '''
select * from (values
    (1, 'a', 10, true, 'east'),
    (2, 'a', 20, false, 'west'),
    (3, 'b', 5, true, 'east'),
    (4, 'c', 7, false, 'east')
) as t(id, grp, v, flag, home)
''';
"""

CASES = {
    "stored scalar": ("id, v, flag", "flag = true", [(1, 10), (3, 5)]),
    "stored scalar beside aggregate": (
        "id, grp_total, flag",
        "flag = true",
        [(1, 30), (3, 5)],
    ),
    "stored identity gate": (
        "id, grp_total, anchor",
        "id = anchor",
        [(1, 30), (3, 5), (4, 7)],
    ),
    "derived boolean gate": (
        "id, grp_total, anchor",
        "is_anchor",
        [(1, 30), (3, 5), (4, 7)],
    ),
    "stored aggregate gate": (
        "id, grp_total",
        "grp_total > 6",
        [(1, 30), (2, 30), (4, 7)],
    ),
    "stored count gate": ("id, v, grp_rows", "grp_rows > 1", [(1, 10), (2, 20)]),
    "and gate": (
        "id, grp_total, flag, home",
        "flag = true and home = 'east'",
        [(1, 30), (3, 5)],
    ),
    "or gate": (
        "id, grp_total, flag, grp_rows",
        "flag = true or grp_rows = 1",
        [(1, 30), (3, 5), (4, 7)],
    ),
    "aggregate and row gate": (
        "id, grp_total, grp_rows, flag",
        "grp_rows > 1 and flag = false",
        [(2, 30)],
    ),
}

UNRESOLVABLE = {
    "unstored scalar": ("id, v", "home = 'east'", "local.home"),
    "unstored aggregate": ("id, v", "grp_rows > 1", "local.grp_rows"),
    "unstored identity": ("id, grp_total", "id = anchor", "local.anchor"),
    "derived over unstored": ("id, grp_total", "is_anchor", "local.is_anchor"),
    "one side unstored": (
        "id, grp_total, flag",
        "flag = true or grp_rows = 1",
        "local.grp_rows",
    ),
}


def _value_column(columns: str) -> str:
    return "grp_total" if "grp_total" in columns else "v"


@pytest.mark.parametrize("case", sorted(CASES))
def test_table_build_writes_read_rows_with_population_values(case: str):
    columns, gate, expected = CASES[case]
    env = Environment()
    env.parse(
        SOURCES
        + f"datasource target ({columns})\ngrain (id)\naddress target_rows\n"
        + f"where {gate};"
    )
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["target"])
    rows = executor.execute_raw_sql(
        f"select id, {_value_column(columns)} from target_rows order by id"
    ).fetchall()
    assert [tuple(r) for r in rows] == expected


@pytest.mark.parametrize("case", sorted(UNRESOLVABLE))
def test_where_must_resolve_from_the_columns(case: str):
    columns, gate, missing = UNRESOLVABLE[case]
    with pytest.raises(HydrationError, match=missing):
        Environment().parse(
            SOURCES
            + f"datasource target ({columns})\ngrain (id)\naddress target_rows\n"
            + f"where {gate};"
        )


@pytest.mark.parametrize("case", sorted(CASES))
def test_file_build_writes_only_declared_columns(tmp_path: Path, case: str):
    columns, gate, expected = CASES[case]
    env = Environment(working_path=tmp_path)
    env.parse(
        SOURCES
        + f"datasource target ({columns})\ngrain (id)\nfile `./target.parquet`\n"
        + f"where {gate};"
    )
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["target"])
    path = (tmp_path / "target.parquet").as_posix()
    written = executor.execute_raw_sql(f"select * from read_parquet('{path}')")
    assert list(written.keys()) == [c.strip() for c in columns.split(",")]
    rows = executor.execute_raw_sql(
        f"select id, {_value_column(columns)} from read_parquet('{path}') order by id"
    ).fetchall()
    assert [tuple(r) for r in rows] == expected


PARTITIONED = """
key id int;
key region enum<string>['east', 'west'];
property id.grp string;
property id.v int;
property id.flag bool;
auto grp_total <- sum(v) by grp;
auto grp_rows <- count(id) by grp;
auto anchor <- min(id) by grp;

datasource rows (id: id, grp: grp, v: v, flag: flag, region: region)
grain (id)
query '''
select * from (values
    (1, 'a', 10, true, 'east'),
    (2, 'a', 20, false, 'east'),
    (3, 'b', 5, true, 'east'),
    (4, 'c', 7, false, 'west')
) as t(id, grp, v, flag, region)
''';

partial datasource target (id, region, grp_total, {columns})
grain (id)
complete where region = 'east'
address target_rows
where {gate};
"""


@pytest.mark.parametrize(
    "columns, gate, expected",
    [
        ("flag", "flag = true", [(1, 30), (3, 5)]),
        ("anchor", "id = anchor", [(1, 30), (3, 5)]),
        ("grp_rows", "grp_rows > 1", [(1, 30), (2, 30)]),
        ("anchor", "id = anchor and grp_total > 6", [(1, 30)]),
    ],
)
def test_partial_build_scopes_by_complete_where_and_gates_by_where(
    columns, gate, expected
):
    """`complete where` scopes the build to this table's slice; the `where`
    gates which of the slice's rows are written, never their values."""
    env = Environment()
    env.parse(PARTITIONED.format(columns=columns, gate=gate))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["target"])
    rows = executor.execute_raw_sql(
        "select id, grp_total from target_rows order by id"
    ).fetchall()
    assert [tuple(r) for r in rows] == expected
