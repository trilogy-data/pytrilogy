from trilogy.core.processing.condition_context import BuildConditionContext
from trilogy.core.models.build import Factory
from trilogy.parser import parse


def _context(test_environment, where: str) -> BuildConditionContext:
    env, parsed = parse(
        f"""
{where}
select
    total_revenue
;
""",
        environment=test_environment,
    )
    lineage = Factory(env).build(parsed[-1].as_lineage(env))
    context = BuildConditionContext.from_where_clauses(lineage.where_clauses)
    assert context is not None
    return context


def test_condition_context_preserves_order_and_views(test_environment):
    context = _context(
        test_environment,
        """
where
    order_id > 1
then where
    revenue > 0
then where
    total_revenue > 10
""",
    )

    assert len(context.pending) == 3
    assert "order_id" in str(context.active_where)
    assert "total_revenue" in str(context.full_where)

    advanced = context.advance()
    assert "order_id" in str(advanced.applied_where)
    assert "revenue" in str(advanced.active_where)
    assert "total_revenue" not in str(advanced.active_where)


def test_condition_context_atomizes_top_level_and_only(test_environment):
    context = _context(
        test_environment,
        """
where
    (order_id = 1 or order_id = 2)
    and revenue > 0
""",
    )

    assert len(context.current_stage) == 2
    assert any("or" in str(atom) for atom in context.current_stage)
    assert any("revenue" in str(atom) for atom in context.current_stage)


def test_condition_context_focus_uses_applied_plus_local(test_environment):
    context = _context(
        test_environment,
        """
where
    order_id > 1
then where
    revenue > 0
then where
    total_revenue > 10
""",
    ).advance()
    local = _context(
        test_environment,
        """
where
    total_revenue > 10
""",
    ).active_where

    focused = context.focus(local)
    assert focused is not None
    assert "order_id" in str(focused.active_where)
    assert "total_revenue" in str(focused.active_where)
    assert "ref:local.revenue > 0" not in str(focused.active_where)


def test_condition_context_for_child_respects_aggregate_stage_boundary(
    test_environment,
):
    context = _context(
        test_environment,
        """
where
    order_id > 1
    and revenue > 0
then where
    total_revenue > 10
    and revenue < 100
""",
    ).advance()
    owner = test_environment.concepts["total_revenue"]

    child = context.for_child(owner)
    assert child is not None
    assert "order_id" in str(child.active_where)
    assert "revenue > 0" in str(child.active_where)
    assert "total_revenue" not in str(child.active_where)
    assert "revenue < 100" not in str(child.active_where)


def test_condition_context_for_child_removes_repeated_owner_atoms(test_environment):
    context = _context(
        test_environment,
        """
where
    total_revenue > 10
then where
    total_revenue < 100
""",
    ).advance()
    owner = test_environment.concepts["total_revenue"]

    assert context.for_child(owner) is None
