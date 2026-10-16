"""A presence probe (`member is [not] null` on one side of a coalescing
relation) is computed on a scan that physically carries that member. Under a
self-import the imported copy of a datasource shares the original's NAME
(`emps` and `boss.emps`), so side identity must be the datasource identifier:
computing the probe on the other side's scan makes it never NULL, and the
filter silently keeps rows whose member is absent (employee 4's manager 9 is
nobody)."""

from pathlib import Path

import pytest

from tests.join_matrix.harness import run_cell

MODEL = """self import as boss;
key id int;
property id.manager int?;
property id.salary int;
{merge}
datasource emps (id: id, manager: manager, salary: salary)
grain (id)
query '''
select * from (values (1, null, 100), (2, 1, 50), (3, 1, 40), (4, 9, 30))
    as t(id, manager, salary)
''';
"""


def _query(tmp_path: Path, form: str, where: str, select: str) -> list[tuple]:
    merge = "merge union manager = boss.id;" if form == "merge" else ""
    (tmp_path / "emp.preql").write_text(MODEL.format(merge=merge))
    clause = "" if form == "merge" else "union join manager = boss.id "
    return run_cell(tmp_path, f"import emp; where {where} {clause}select {select};")


@pytest.mark.parametrize("form", ["join", "merge"])
@pytest.mark.parametrize(
    "where, select, expected",
    [
        (
            "boss.id is not null",
            "manager, count(id) -> n",
            [(1, 2), (2, 0), (3, 0), (4, 0)],
        ),
        ("boss.id is null", "manager, count(id) -> n", [(9, 1), (None, 1)]),
        (
            "boss.id is not null",
            "manager, sum(salary) -> s",
            [(1, 90), (2, None), (3, None), (4, None)],
        ),
        (
            "boss.id is not null",
            "id, manager",
            [(2, 1), (3, 1), (None, 2), (None, 3), (None, 4)],
        ),
    ],
)
def test_probe_reads_the_members_own_copy(
    tmp_path: Path, form: str, where: str, select: str, expected: list
):
    assert _query(tmp_path, form, where, select) == expected
