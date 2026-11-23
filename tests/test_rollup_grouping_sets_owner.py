from types import SimpleNamespace

from trilogy import Dialects
from trilogy.core.enums import AggregateGroupingMode
from trilogy.core.models.build import BuildAggregateWrapper, BuildGroupingSets
from trilogy.core.models.execute import CTE

MODEL = """
key eid int;
property eid.department string?;
property eid.course string;
property eid.credits int;
datasource enroll (eid: eid, department: department, course: course, credits: credits)
grain (eid)
address enroll;
"""


def _ctes(query: str) -> tuple:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    processed = executor.parse_text(query)[-1]
    return executor, processed


def test_the_group_cte_owns_its_grouping_sets():
    _, processed = _ctes(
        "select *rollup((select department, course, sum(credits) as m, "
        "grouping(course) as g having m > 2), department, course);"
    )
    owners = [c for c in processed.ctes if isinstance(c, CTE) and c.grouping_sets]
    assert len(owners) == 1
    spec = owners[0].grouping_sets
    assert spec.mode == AggregateGroupingMode.ROLLUP
    assert [c.address for c in spec.by] == ["local.department", "local.course"]
    assert spec.has_empty_set


def test_grouping_sets_render_off_the_cte_not_the_aggregates():
    executor, processed = _ctes(
        "select *grouping_sets((select department, course, sum(credits) as m), "
        "(department), (course), ());"
    )
    assert "GROUPING SETS ((" in executor.generator.compile_statement(processed)
    for cte in processed.ctes:
        if isinstance(cte, CTE):
            cte.grouping_sets = None
    assert "GROUPING SETS" not in executor.generator.compile_statement(processed)


def test_a_folded_group_keeps_its_rollup():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
key group_id int;
key event_id int;
property event_id.active bool;
property event_id.amount int;
datasource events (eid: event_id, gid: group_id, active: active, amount: amount)
grain (event_id)
query '''
select 1 eid, 1 gid, true active, 10 amount union all
select 2, 1, false, 5 union all
select 3, 2, true, 7
''';
rowset per_pair <- select group_id as g, active as a, sum(amount) as amt;
""")
    rows = executor.execute_text(
        "select *rollup((select per_pair.g, per_pair.a, sum(per_pair.amt) as total), "
        "per_pair.g);"
    )[-1].fetchall()
    assert sorted(map(tuple, rows), key=repr) == [
        (1, False, 15),
        (1, True, 15),
        (2, True, 7),
        (None, None, 22),
    ]


def _aggregate_over(*keys: str) -> SimpleNamespace:
    return SimpleNamespace(
        lineage=BuildAggregateWrapper(
            function=None,  # type: ignore[arg-type]
            by=[SimpleNamespace(address=k) for k in keys],  # type: ignore[misc]
        )
    )


def test_the_pass_computes_every_aggregate_at_its_keys():
    keys = [SimpleNamespace(address=k) for k in ("a", "b")]
    spec = BuildGroupingSets(mode=AggregateGroupingMode.ROLLUP, by=keys)  # type: ignore[arg-type]
    assert spec.computes(_aggregate_over("b", "a"))  # type: ignore[arg-type]
    assert not spec.computes(_aggregate_over("a"))  # type: ignore[arg-type]
    assert not spec.computes(SimpleNamespace(lineage=None))  # type: ignore[arg-type]
