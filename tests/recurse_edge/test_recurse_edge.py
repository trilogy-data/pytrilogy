from pathlib import Path

import pytest

from trilogy import Dialects, Environment

HERE = Path(__file__).parent


def run(query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=HERE)
    )
    return [tuple(row) for row in executor.execute_text(query)[-1].fetchall()]


def test_count_key_by_recursive_concept():
    assert run(
        "import proc; select app_pid, count(pid) -> n order by app_pid asc;"
    ) == [(1, 3), (4, 2)]


def test_sum_measure_by_recursive_concept():
    assert run("import proc; select app_pid, sum(mem) -> m order by app_pid asc;") == [
        (1, 16),
        (4, 10),
    ]


@pytest.mark.parametrize(
    "outputs, expected",
    [
        ("app.pid", [(1, 16), (4, 10)]),
        ("app.name", [("code", 16), ("fx", 10)]),
        ("app.pid, app.name", [(1, "code", 16), (4, "fx", 10)]),
    ],
)
def test_metric_by_merged_root(outputs: str, expected: list[tuple]):
    first = outputs.split(",")[0]
    assert (
        run(f"import tree; select {outputs}, total_mem order by {first} asc;")
        == expected
    )


@pytest.mark.parametrize(
    "outputs, expected",
    [
        ("app.pid", [(1, 16), (2, None), (3, None), (4, 10), (5, None)]),
        (
            "app.name",
            [("code", 16), ("fx", 10), ("fxc", None), ("node", None), ("py", None)],
        ),
        (
            "app.pid, app.name",
            [
                (1, "code", 16),
                (2, "node", None),
                (3, "py", None),
                (4, "fx", 10),
                (5, "fxc", None),
            ],
        ),
    ],
)
def test_metric_by_subset_merged_root_extends(outputs: str, expected: list[tuple]):
    first = outputs.split(",")[0]
    assert (
        run(f"import tree_subset; select {outputs}, total_mem order by {first} asc;")
        == expected
    )


@pytest.mark.parametrize(
    "outputs, expected",
    [
        ("app.pid", [(1, 16), (4, 10)]),
        ("app.name", [("code", 16), ("fx", 10)]),
        ("app.pid, app.name", [(1, "code", 16), (4, "fx", 10)]),
    ],
)
def test_self_imported_tree(outputs: str, expected: list[tuple]):
    first = outputs.split(",")[0]
    assert (
        run(f"import process; select {outputs}, sum(mem) -> m order by {first} asc;")
        == expected
    )


def test_self_import_completes_namespace():
    assert run(
        "import process; where pid is not null "
        "select pid, parent.name order by pid asc;"
    ) == [(1, None), (2, "code"), (3, "node"), (4, None), (5, "fx")]


def test_orphan_walk_ends_at_missing_parent():
    assert run("import orphans; select pid, app_pid order by pid asc;") == [
        (1, 1),
        (2, 1),
        (6, 99),
        (7, 99),
        (8, 99),
        (9, 98),
    ]


def test_orphan_rollup_groups_by_missing_parent():
    assert run(
        "import orphans; select app_pid, sum(mem) -> m order by app_pid asc;"
    ) == [(1, 15), (98, 3), (99, 11100)]


def test_orphan_recursion_renders_left_step():
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=HERE)
    )
    sql = executor.generate_sql("import orphans; select pid, app_pid;")[-1]
    assert "LEFT OUTER JOIN" in sql
    assert "coalesce" not in sql.lower()


CHILD_ROWS = [
    (1, 1, "code"),
    (2, 1, "code"),
    (6, 99, None),
    (7, 99, None),
    (8, 99, None),
    (9, 98, None),
]


def test_union_merge_keeps_orphan_walks():
    assert (
        run(
            "import orphans_union; select pid, app_pid, app.name "
            "having pid is not null order by pid asc;"
        )
        == CHILD_ROWS
    )


@pytest.mark.xfail(
    strict=True,
    reason="the `pid is not null` atom is hosted on the scan feeding the "
    "recursion, where it is a tautology, so the union's completion merge "
    "null-extends pid past it; `union join app_pid = app.pid` leaks the same",
)
def test_union_merge_where_filters_null_extended_side():
    assert (
        run(
            "import orphans_union; where pid is not null "
            "select pid, app_pid, app.name order by pid asc;"
        )
        == CHILD_ROWS
    )


@pytest.mark.xfail(
    strict=True,
    reason="planner leaves the measure unsourced (`Missing source reference`); "
    "`union join app_pid = app.pid` fails the same",
)
def test_union_merge_measure_with_filter_on_side():
    assert run(
        "import orphans_union; where pid is not null "
        "select pid, app_pid, sum(mem) -> m order by pid asc;"
    ) == [
        (1, 1, 10),
        (2, 1, 5),
        (6, 99, 100),
        (7, 99, 1000),
        (8, 99, 10000),
        (9, 98, 3),
    ]


def test_union_merge_axis_is_both_key_sets():
    assert run(
        "import orphans_union; select app.pid, app.name order by app.pid asc;"
    ) == [
        (1, "code"),
        (2, "node"),
        (6, "orphan"),
        (7, "kid"),
        (8, "gkid"),
        (9, "lone"),
        (98, None),
        (99, None),
    ]


def test_union_merge_rollups_agree():
    by_pid = run(
        "import orphans_union; select app_pid, sum(mem) -> m having m is not null "
        "order by app_pid asc;"
    )
    by_app = run(
        "import orphans_union; select app_pid, app.name, sum(mem) -> m "
        "having m is not null order by app_pid asc;"
    )
    assert by_pid == [(1, 15), (98, 3), (99, 11100)]
    assert by_app == [(1, "code", 15), (98, None, 3), (99, None, 11100)]


def test_union_merge_counts_children_per_axis_value():
    assert run(
        "import orphans_union; select app_pid, count(pid) -> n order by app_pid asc;"
    ) == [(1, 2), (2, 0), (6, 0), (7, 0), (8, 0), (9, 0), (98, 1), (99, 3)]


def test_union_merge_matches_union_join(tmp_path: Path):
    model = (HERE / "orphans_union.preql").read_text()
    (tmp_path / "joined.preql").write_text(
        model.replace("merge union app_pid = app.pid;", "")
    )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    for select in (
        "select pid, app_pid, app.name",
        "select app_pid, sum(mem) -> m",
        "select app.name, count(pid) -> n",
    ):
        joined = executor.execute_text(
            f"import joined; union join app_pid = app.pid {select};"
        )[-1]
        assert sorted(map(tuple, joined.fetchall()), key=str) == sorted(
            run(f"import orphans_union; {select};"), key=str
        )


def test_identity_merge_drops_orphan_walks(tmp_path: Path):
    model = (HERE / "orphans_union.preql").read_text()
    (tmp_path / "equal.preql").write_text(
        model.replace("merge union app_pid = app.pid;", "merge app_pid into app.pid;")
    )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    rows = executor.execute_text("import equal; select pid, app_pid order by pid asc;")
    assert [tuple(r) for r in rows[-1].fetchall()] == [(1, 1), (2, 1)]
