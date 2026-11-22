from trilogy import Dialects
from trilogy.core.enums import AggregateGroupingMode
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
