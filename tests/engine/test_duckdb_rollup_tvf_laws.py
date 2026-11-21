"""A rollup read through a rowset is the single statement's rows.

Each body runs once as a statement (the oracle) and again as a rowset read
five ways: whole, without its flags, filtered on a flag, filtered on a rolled
key, and re-aggregated per level. The rows of every read are the oracle's,
projected in Python. A projection that drops the flags is distinct rows, as
for any rowset.
"""

import itertools

import pytest

from trilogy import Dialects

MODEL = """
key eid int;
property eid.department string?;
property eid.course string;
property eid.credits int;
property eid.student int;
datasource enroll (eid: eid, department: department, course: course, credits: credits, student: student)
grain (eid)
query '''
select 1 eid, 'math' department, 'calc' course, 4 credits, 10 student union all
select 2, 'math', 'algebra', 3, 10 union all
select 3, 'art', 'paint', 2, 11 union all
select 4, null, 'paint', 2, 12 union all
select 5, 'art', 'calc', 5, 12
''';
"""

LABELLED = (
    "case when grouping(department) = 1 then 'ALL' else department end as lbl,"
    " sum(credits) as m"
)
MEASURES = [
    ("sum(credits) as m", ["m"]),
    ("avg(credits) as m", ["m"]),
    ("count_distinct(student) as m", ["m"]),
    ("sum(credits) as m, count(eid) as n", ["m", "n"]),
    ("sum(credits) / count(eid) as m", ["m"]),
    (LABELLED, ["lbl", "m"]),
    ("upper(department) as up, max(credits) as m", ["up", "m"]),
]
SPECS = [
    "rollup (department, course)",
    "cube (department, course)",
    "grouping sets ((department), (course))",
    "rollup (department)",
    "rollup (course, department)",
]
HAVINGS = ["", " having m > 2"]
WHERES = ["", "where credits > 2 "]

CASES = list(itertools.product(MEASURES, SPECS, HAVINGS, WHERES))


def call(select: str, spec: str) -> str:
    """`select` grouped by the SQL `spec` (`rollup (a, b)`), as the call form."""
    name, keys = spec.split(" (", 1)
    return f"{name.replace(' ', '_')}(({select}), {keys}"


def run(query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    return sorted(
        (tuple(r) for r in executor.execute_text(query)[-1].fetchall()), key=repr
    )


def expect(rows) -> list[tuple]:
    return sorted(rows, key=repr)


@pytest.mark.parametrize(
    "measure,spec,having,where",
    CASES,
    ids=[f"{m[1][-1]}{i}" for i, (m, *_rest) in enumerate(CASES)],
)
def test_rowset_reads_are_the_statement_rows(measure, spec, having, where):
    items, names = measure
    keys = ["department", "course"] if "course" in spec else ["department"]
    flags = [f"g_{k}" for k in keys]
    flag_items = ", ".join(f"grouping({k}) as g_{k}" for k in keys)
    body = call(f"{where}select {', '.join(keys)}, {items}, {flag_items}{having}", spec)
    columns = keys + names + flags
    oracle = run(body + ";")
    rowset = f"with r as {body};\n"
    every = ", ".join(f"r.{c}" for c in columns)
    flag_at = columns.index("g_department")
    measure_at = columns.index("m")

    assert run(rowset + f"select {every};") == oracle

    shown = ", ".join(f"r.{c}" for c in columns[: -len(flags)])
    assert run(rowset + f"select {shown};") == expect(
        {row[: -len(flags)] for row in oracle}
    )

    assert run(rowset + f"where r.g_department = 1 select {every};") == expect(
        row for row in oracle if row[flag_at] == 1
    )

    assert run(rowset + f"where r.department is null select {every};") == expect(
        row for row in oracle if row[0] is None
    )

    levels: dict[int, list] = {}
    for row in oracle:
        levels.setdefault(row[flag_at], []).append(row[measure_at])
    assert run(
        rowset + "select r.g_department, max(r.m) as mx, count(r.m) as n;"
    ) == expect((level, max(v), len(v)) for level, v in levels.items())
