"""A top-level `rollup|cube|grouping_sets` call statement is its SQL's rows.

The statement is planned as a select over its own rollup TVF and rendered under
the authored names, so its only independent oracle is DuckDB: each body runs as
a statement and as the hand-written SQL, bare and under ORDER BY + LIMIT.
"""

import itertools

import pytest

from trilogy import Dialects

ROWS = """
select 1 eid, 'math' department, 'calc' course, 4 credits, 10 student union all
select 2, 'math', 'algebra', 3, 10 union all
select 3, 'art', 'paint', 2, 11 union all
select 4, null, 'paint', 2, 12 union all
select 5, 'art', 'calc', 5, 12
"""

MODEL = f"""
key eid int;
property eid.department string?;
property eid.course string;
property eid.credits int;
property eid.student int;
datasource enroll (eid: eid, department: department, course: course, credits: credits, student: student)
grain (eid)
query '''{ROWS}''';
"""

LABELLED = (
    "case when grouping(department) = 1 then 'ALL' else department end as lbl,"
    " sum(credits) as m"
)
# (trilogy items, sql items, output names)
MEASURES = [
    ("sum(credits) as m", "sum(credits) as m", ["m"]),
    ("avg(credits) as m", "avg(credits) as m", ["m"]),
    ("count_distinct(student) as m", "count(distinct student) as m", ["m"]),
    (
        "sum(credits) as m, count(eid) as n",
        "sum(credits) as m, count(eid) as n",
        ["m", "n"],
    ),
    (LABELLED, LABELLED, ["lbl", "m"]),
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


def ordered(rows) -> list[tuple]:
    return sorted((tuple(r) for r in rows), key=repr)


@pytest.mark.parametrize(
    "measure,spec,having,where",
    CASES,
    ids=[f"{m[2][-1]}{i}" for i, (m, *_rest) in enumerate(CASES)],
)
def test_statement_rows_are_the_sql_rows(measure, spec, having, where):
    items, sql_items, names = measure
    keys = ["department", "course"] if "course" in spec else ["department"]
    flags = ", ".join(f"grouping({k}) as g_{k}" for k in keys)
    columns = keys + names + [f"g_{k}" for k in keys]
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    select = f"{where}select {', '.join(keys)}, {items}, {flags}{having}"
    sql = (
        f"select {', '.join(keys)}, {sql_items}, {flags} from ({ROWS}) {where}"
        f"group by {spec}{having}"
    )
    result = executor.execute_text(call(select, spec) + ";")[-1]
    assert list(result.keys()) == columns
    assert ordered(result.fetchall()) == ordered(
        executor.execute_raw_sql(sql).fetchall()
    )

    # a total order, so the limit names the same rows on both sides
    order = ", ".join(f"{c} asc nulls first" for c in columns)
    assert [
        tuple(r)
        for r in executor.execute_text(
            call(f"{select} order by {order} limit 3", spec) + ";"
        )[-1].fetchall()
    ] == [
        tuple(r)
        for r in executor.execute_raw_sql(f"{sql} order by {order} limit 3").fetchall()
    ]
