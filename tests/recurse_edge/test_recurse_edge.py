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


@pytest.mark.parametrize("model", ["process", "process_by_path"])
@pytest.mark.parametrize(
    "outputs, expected",
    [
        ("app.pid", [(1, 16), (4, 10)]),
        ("app.name", [("code", 16), ("fx", 10)]),
        ("app.pid, app.name", [(1, "code", 16), (4, "fx", 10)]),
    ],
)
def test_self_imported_tree(model: str, outputs: str, expected: list[tuple]):
    first = outputs.split(",")[0]
    assert (
        run(f"import {model}; select {outputs}, sum(mem) -> m order by {first} asc;")
        == expected
    )


@pytest.mark.parametrize("model", ["process", "process_by_path"])
def test_self_import_completes_namespace(model: str):
    assert run(
        f"import {model}; where pid is not null "
        "select pid, parent.name order by pid asc;"
    ) == [(1, None), (2, "code"), (3, "node"), (4, None), (5, "fx")]
