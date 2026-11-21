"""A `rollup|cube|grouping_sets` call read through a rowset is a relational TVF.

Its handles read NEW concepts (one per body column), not the body's own: a
rolled key is a nullable key of that relation, its grouping flags complete the
identity, and a measure is a property of both. Nothing downstream of the
rowset has to know the NULLs came from a grouping set.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation, JoinType, Modifier, Purpose
from trilogy.core.models.author import RollupSelectLineage, RowsetItem, SelectLineage
from trilogy.core.query_processor import _collect_rowset_scoped_joins
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
    "with a as rollup((select department, course, sum(credits) as total_credits, "
    "grouping(department) as g_dept, grouping(course) as g_course), department, "
    "course);\n"
)
PAYROLL = (
    "with b as rollup((select dept, sum(salary) as payroll, grouping(dept) as g), "
    "dept);\n"
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
        MODEL + "with a as rollup((select department, sum(credits) as t), department);"
    )
    keys = env.concepts["a.t"].keys
    assert keys and "a.department" in keys and len(keys) == 2


def test_spec_over_no_keys_stays_a_select():
    env = Environment()
    env.parse(MODEL + "with a as rollup((select sum(credits) as t));")
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
    name, keys = spec.split(" (", 1)
    call = (
        name.replace(" ", "_") + "((select department, course, sum(credits) as t{0}), "
    )
    call += keys
    order = (
        " order by {0}department asc nulls first, {0}course asc nulls first, {0}t asc"
    )
    direct = rows(executor, "select *" + call.format(order.format("")) + ";")
    through = rows(
        executor,
        f"with a as {call.format('')};\nselect a.department, a.course, a.t"
        + order.format("a.")
        + ";",
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
        CREDITS
        + "select *rollup((where a.g_course = 0 select a.department, sum(a.total_credits) as t "
        "order by a.department asc nulls first), a.department);",
    ) == [(None, 9), ("art", 2), ("math", 7)]


def test_rollup_rowset_over_a_rollup_rowset(executor):
    assert rows(
        executor,
        CREDITS + "with c as rollup((where a.g_course = 0 select a.department, "
        "max(a.total_credits) as biggest), a.department);\n"
        "select c.department, c.biggest order by c.department asc nulls first;",
    ) == [(None, 4), ("art", 2), ("math", 4)]


def test_two_rollups_pair_their_subtotals(executor):
    """A rolled key's NULL is a value: the two grand totals are one row."""
    one_key = (
        "with a as rollup((select department, sum(credits) as total_credits, "
        "grouping(department) as g_dept), department);\n"
    )
    assert rows(
        executor,
        one_key + PAYROLL + "select coalesce(a.department, b.dept) as d,"
        " a.total_credits, b.payroll union join a.department = b.dept"
        " order by d asc nulls first;",
    ) == [(None, 9, 220), ("art", 2, None), ("math", 7, 150), ("music", None, 70)]


def test_two_rollups_pair_on_keys_and_flags(executor):
    one_key = (
        "with a as rollup((select department, sum(credits) as total_credits, "
        "grouping(department) as g_dept), department);\n"
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
        "with h as rollup((select department, sum(credits) as t, "
        "--grouping(department) as g having g = 1), department);\n"
        "select h.department, h.t;",
    ) == [(None, 9)]


def test_rowset_renders_as_authored():
    env = Environment()
    _, statements = env.parse(
        MODEL + "with a as rollup((select department, sum(credits) as t), department);"
    )
    rendered = Renderer().to_string(statements[-1])
    assert rendered.startswith("rowset a <- rollup(")
    assert "union" not in rendered


def test_imported_rollup_rowset(tmp_path):
    (tmp_path / "enroll.preql").write_text(
        MODEL + "with rolled as rollup((select department, sum(credits) as total, "
        "grouping(department) as g), department);"
    )
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    assert rows(
        exec,
        "import enroll as e; select e.rolled.department, e.rolled.total, e.rolled.g"
        " order by e.rolled.g asc, e.rolled.department asc;",
    ) == [("art", 2, 0), ("math", 7, 0), (None, 9, 1)]


def rolled(tail: str = "") -> str:
    return (
        "select *rollup((select department, sum(credits) as total_credits, "
        f"--grouping(department) as g{tail}), department);"
    )


def test_statement_is_planned_over_its_own_tvf():
    env = Environment()
    _, statements = env.parse(MODEL + rolled(" order by g asc limit 2"))
    statement = statements[-1]
    outer = statement.relational
    assert outer is not None and outer.grouping is None
    assert outer.limit == 2 and statement.limit == 2
    handles = [env.concepts[ref.address] for ref in outer.output_components]
    assert all(isinstance(h.lineage, RowsetItem) for h in handles)
    assert isinstance(handles[0].lineage.rowset.select, RollupSelectLineage)
    assert statement.relational_names == {
        authored.address: handle.address
        for authored, handle in zip(statement.output_components, handles)
    }
    assert [ref.address for ref in statement.output_components] == [
        "local.department",
        "local.total_credits",
        "local.g",
    ]


def test_statement_over_no_keys_stays_a_select():
    env = Environment()
    _, statements = env.parse(MODEL + "select *rollup((select sum(credits) as t));")
    assert statements[-1].relational is None


def test_statement_renders_under_authored_names(executor):
    result = executor.execute_text(rolled(" order by g asc, department asc limit 2"))[
        -1
    ]
    assert list(result.keys()) == ["department", "total_credits"]
    assert [tuple(r) for r in result.fetchall()] == [("art", 2), ("math", 7)]


def test_statement_is_one_pass_ordered_by_its_columns(executor):
    sql = executor.generate_sql(
        rolled(" order by g asc, department asc").replace("--", "")
    )[-1]
    assert sql.count("ROLLUP (") == 1
    assert sql.count("GROUP BY") == 1
    assert "grouping(" not in sql.split("ORDER BY")[-1]


def test_reparsed_statement_reuses_its_relation():
    env = Environment()
    env.parse(MODEL)
    before = len(env.concepts)
    env.parse(rolled())
    once = len(env.concepts)
    env.parse(rolled())
    assert once > before and len(env.concepts) == once


def test_two_statements_keep_their_own_relations(executor):
    _, first = executor.environment.parse(rolled(" order by g asc, department asc"))
    _, second = executor.environment.parse(
        "select *rollup((select course, sum(credits) as by_course order by course asc nulls "
        "first), course);"
    )
    assert [tuple(r) for r in executor.execute_query(first[-1]).fetchall()] == [
        ("art", 2),
        ("math", 7),
        (None, 9),
    ]
    assert [tuple(r) for r in executor.execute_query(second[-1]).fetchall()] == [
        (None, 9),
        ("algebra", 3),
        ("calc", 4),
        ("paint", 2),
    ]


def test_statement_with_a_renamed_key(executor):
    assert rows(
        executor,
        "select *rollup((select department as dept_name, sum(credits) as t order by dept_name "
        "asc nulls first), department);",
    ) == [(None, 9), ("art", 2), ("math", 7)]


def test_rowset_with_an_unprojected_key(executor):
    assert rows(
        executor,
        "with a as rollup((select case when grouping(department) = 1 then 'all' else "
        "department end as label, sum(credits) as t), department);\n"
        "select a.label, a.t order by a.label asc;",
    ) == [("all", 9), ("art", 2), ("math", 7)]


def test_rollup_body_keeps_its_scoped_join(executor):
    body = (
        "rollup((select department, sum(credits) as t, sum(salary) as paid union join "
        "department = dept{0}), department)"
    )
    direct = rows(
        executor, "select *" + body.format(" order by department asc nulls first") + ";"
    )
    assert direct == rows(
        executor,
        f"with a as {body.format('')};\nselect a.department, a.t, a.paid"
        " order by a.department asc nulls first;",
    )
    assert (None, 9, 220) in direct


def test_rollup_body_with_a_scoped_join_reads_its_flag(executor):
    assert rows(
        executor,
        "with a as rollup((select department, sum(credits) as t, sum(salary) as paid, "
        "grouping(department) as g union join department = dept), department);\n"
        "select a.department, a.t, a.paid, a.g order by a.g asc, a.department asc;",
    ) == [
        ("art", 2, None, 0),
        ("math", 7, 150, 0),
        ("music", None, 70, 0),
        (None, 9, 220, 1),
    ]


def test_a_statement_join_stays_its_own():
    env = Environment()
    join = ("local.department", "local.dept", JoinType.FULL)
    _, statements = env.parse(
        MODEL
        + "select *rollup((select department, sum(credits) as t union join department = dept), "
        "department);\n"
        "select *rollup((select course, sum(credits) as c), course);"
    )
    joined, plain = statements[-2], statements[-1]
    assert join in _collect_rowset_scoped_joins(env, joined.planned)
    assert _collect_rowset_scoped_joins(env, plain.planned) == []
