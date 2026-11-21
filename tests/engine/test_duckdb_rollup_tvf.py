"""A rowset whose body is `by rollup|cube|grouping sets` is a relational TVF.

Its handles read NEW concepts (one per body column), not the body's own: a
rolled key is a nullable key of that relation, its grouping flags complete the
identity, and a measure is a property of both. Nothing downstream of the
rowset has to know the NULLs came from a grouping set.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation, Modifier, Purpose
from trilogy.core.models.author import RollupSelectLineage, RowsetItem, SelectLineage
from trilogy.parsing.render import Renderer

MODEL = """
key eid int;
property eid.department string;
property eid.course string;
property eid.credits int;
datasource enroll (eid: eid, department: department, course: course, credits: credits)
grain (eid)
query '''
select 1 eid, 'math' department, 'calc' course, 4 credits union all
select 2, 'math', 'algebra', 3 union all
select 3, 'art', 'paint', 2
''';

key sid int;
property sid.dept string;
property sid.salary int;
datasource staff (sid: sid, dept: dept, salary: salary)
grain (sid)
query '''
select 1 sid, 'math' dept, 100 salary union all
select 2, 'math', 50 union all
select 3, 'music', 70
''';
"""

CREDITS = (
    "with a as select department, course, sum(credits) as total_credits,"
    " grouping(department) as g_dept, grouping(course) as g_course"
    " by rollup (department, course);\n"
)
PAYROLL = (
    "with b as select dept, sum(salary) as payroll, grouping(dept) as g"
    " by rollup (dept);\n"
)


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(MODEL)
    return exec


def rows(executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


def test_handles_read_new_dimensions():
    env = Environment()
    env.parse(MODEL + CREDITS)
    handle = env.concepts["a.department"]
    assert isinstance(handle.lineage, RowsetItem)
    assert isinstance(handle.lineage.rowset.select, RollupSelectLineage)
    content = env.concepts[handle.lineage.content.address]
    assert content.address != "local.department"
    assert content.derivation == Derivation.TVF_UNION
    assert content.purpose == Purpose.KEY
    assert Modifier.NULLABLE in content.modifiers


def test_grouping_flags_complete_the_identity():
    env = Environment()
    env.parse(MODEL + CREDITS)
    identity = {"a.department", "a.course", "a.g_dept", "a.g_course"}
    assert env.concepts["a.total_credits"].keys == identity
    flag = env.concepts[env.concepts["a.g_dept"].lineage.content.address]
    assert flag.purpose == Purpose.KEY
    assert Modifier.NULLABLE not in flag.modifiers


def test_injected_flags_complete_the_identity_unasked():
    env = Environment()
    env.parse(
        MODEL + "with a as select department, sum(credits) as t by rollup (department);"
    )
    keys = env.concepts["a.t"].keys
    assert keys and "a.department" in keys and len(keys) == 2


def test_spec_over_no_keys_stays_a_select():
    env = Environment()
    env.parse(MODEL + "with a as select sum(credits) as t by rollup ();")
    assert isinstance(env.concepts["a.t"].lineage.rowset.select, SelectLineage)


@pytest.mark.parametrize(
    "spec",
    [
        "rollup (department, course)",
        "cube (department, course)",
        "grouping sets ((department), (course), ())",
    ],
)
def test_rowset_matches_the_single_statement(executor, spec):
    body = f"select department, course, sum(credits) as t by {spec}"
    order = (
        " order by {0}department asc nulls first, {0}course asc nulls first, {0}t asc;"
    )
    direct = rows(executor, body + order.format(""))
    through = rows(
        executor,
        f"with a as {body};\nselect a.department, a.course, a.t" + order.format("a."),
    )
    assert through == direct


def test_filter_on_a_flag(executor):
    assert rows(
        executor,
        CREDITS + "where a.g_course = 1 select a.department, a.total_credits"
        " order by a.department asc nulls first;",
    ) == [(None, 9), ("art", 2), ("math", 7)]


def test_reaggregate_the_leaf_rows(executor):
    assert rows(
        executor,
        CREDITS + "where a.g_course = 0 select a.department,"
        " sum(a.total_credits) as t, count(a.course) as n"
        " order by a.department asc;",
    ) == [("art", 2, 1), ("math", 7, 2)]


def test_rollup_of_a_rollup(executor):
    assert rows(
        executor,
        CREDITS + "where a.g_course = 0 select a.department,"
        " sum(a.total_credits) as t by rollup (a.department)"
        " order by a.department asc nulls first;",
    ) == [(None, 9), ("art", 2), ("math", 7)]


def test_rollup_rowset_over_a_rollup_rowset(executor):
    assert rows(
        executor,
        CREDITS + "with c as where a.g_course = 0 select a.department,"
        " max(a.total_credits) as biggest by rollup (a.department);\n"
        "select c.department, c.biggest order by c.department asc nulls first;",
    ) == [(None, 4), ("art", 2), ("math", 4)]


def test_two_rollups_pair_their_subtotals(executor):
    """A rolled key's NULL is a value: the two grand totals are one row."""
    one_key = (
        "with a as select department, sum(credits) as total_credits,"
        " grouping(department) as g_dept by rollup (department);\n"
    )
    assert rows(
        executor,
        one_key + PAYROLL + "select coalesce(a.department, b.dept) as d,"
        " a.total_credits, b.payroll union join a.department = b.dept"
        " order by d asc nulls first;",
    ) == [(None, 9, 220), ("art", 2, None), ("math", 7, 150), ("music", None, 70)]


def test_two_rollups_pair_on_keys_and_flags(executor):
    one_key = (
        "with a as select department, sum(credits) as total_credits,"
        " grouping(department) as g_dept by rollup (department);\n"
    )
    assert rows(
        executor,
        one_key + PAYROLL + "select coalesce(a.department, b.dept) as d,"
        " coalesce(a.g_dept, b.g) as lvl, a.total_credits, b.payroll"
        " union join a.department = b.dept union join a.g_dept = b.g"
        " order by lvl asc, d asc nulls first;",
    ) == [
        ("art", 0, 2, None),
        ("math", 0, 7, 150),
        ("music", 0, None, 70),
        (None, 1, 9, 220),
    ]


def test_union_of_two_rollups(executor):
    assert rows(
        executor,
        CREDITS + PAYROLL + "from union("
        "(select a.department, a.total_credits where a.g_course = 1),"
        " (select b.dept, b.payroll)) -> (d, v)"
        " select d, v order by d asc nulls first, v asc;",
    ) == [
        (None, 9),
        (None, 220),
        ("art", 2),
        ("math", 7),
        ("math", 150),
        ("music", 70),
    ]


def test_union_arm_passes_a_rowset_through(executor):
    assert rows(
        executor,
        "with p as select dept, sum(salary) as payroll;\n"
        "from union((select p.dept, p.payroll), (select p.dept, p.payroll))"
        " -> (d, v) select d, v, __arm order by d asc, __arm asc;",
    ) == [
        ("math", 150, 0),
        ("math", 150, 1),
        ("music", 70, 0),
        ("music", 70, 1),
    ]


def test_each_rollup_is_one_pass(executor):
    sql = executor.generate_sql(
        CREDITS + PAYROLL + "where a.g_course = 1 select a.department,"
        " a.total_credits, b.payroll union join a.department = b.dept;"
    )[-1]
    assert sql.count("ROLLUP (") == 2


def test_hidden_flag_filters_inside_the_body(executor):
    assert rows(
        executor,
        "with h as select department, sum(credits) as t, --grouping(department) as g"
        " by rollup (department) having g = 1;\nselect h.department, h.t;",
    ) == [(None, 9)]


def test_rowset_renders_as_authored():
    env = Environment()
    _, statements = env.parse(
        MODEL + "with a as select department, sum(credits) as t by rollup (department);"
    )
    rendered = Renderer().to_string(statements[-1])
    assert rendered.startswith("rowset a <- select")
    assert "by rollup (department)" in rendered
    assert "union" not in rendered


def test_imported_rollup_rowset(tmp_path):
    (tmp_path / "enroll.preql").write_text(
        MODEL + "with rolled as select department, sum(credits) as total,"
        " grouping(department) as g by rollup (department);"
    )
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    assert rows(
        exec,
        "import enroll as e; select e.rolled.department, e.rolled.total, e.rolled.g"
        " order by e.rolled.g asc, e.rolled.department asc;",
    ) == [("art", 2, 0), ("math", 7, 0), (None, 9, 1)]
