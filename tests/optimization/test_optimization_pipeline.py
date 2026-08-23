from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from trilogy.constants import CONFIG
from trilogy.core.optimization import (
    OptimizationRulePlan,
    PredicatePushdownRemove,
    build_optimization_rule_plan,
    validate_optimization_rule_plan,
)

FLAGS = {
    "merge_aggregate",
    "merge_irrelevant_group_by",
    "join_hoist",
    "datasource_inlining",
    "predicate_pushdown",
    "push_filtered_aggregate_input",
    "push_filtered_count_into_join",
    "push_semi_join_into_aggregate",
    "upgrade_condition_joins",
    "upgrade_outer_key_set_equivalence",
    "simplify_null_safe_joins",
    "strip_redundant_not_null",
    "union_dim_pushdown",
    "hide_unused_concepts",
    "order_inner_joins_first",
}


@contextmanager
def _optimization_flags(**overrides: bool) -> Iterator[None]:
    original = {field: getattr(CONFIG.optimizations, field) for field in FLAGS}
    try:
        for field in FLAGS:
            setattr(CONFIG.optimizations, field, False)
        for field, value in overrides.items():
            setattr(CONFIG.optimizations, field, value)
        yield
    finally:
        for field, value in original.items():
            setattr(CONFIG.optimizations, field, value)


def test_pipeline_skips_union_refire_when_union_dim_pushdown_disabled():
    with _optimization_flags(predicate_pushdown=True):
        plan = build_optimization_rule_plan()

    names = [phase.name for phase in plan]
    # merge_aggregate is disabled here (all flags off then predicate_pushdown on),
    # so the passthrough-only cleanup phase runs after predicate pushdown removes.
    assert names == [
        "predicate_pushdown.initial",
        "predicate_pushdown.remove",
        "collapse_single_parent.passthrough_after_pushdown",
    ]
    # The point of this test: with union_dim_pushdown disabled, the union-triggered
    # predicate refire is absent. (Assert the intent directly rather than "nothing
    # refires" -- unrelated phases may legitimately carry refire triggers.)
    assert "predicate_pushdown.after_union_dim" not in names


def test_pipeline_runs_datasource_inlining_before_predicate_pushdown():
    with _optimization_flags(datasource_inlining=True, predicate_pushdown=True):
        plan = build_optimization_rule_plan()

    names = [phase.name for phase in plan]
    assert names == [
        "inline_datasource",
        "predicate_pushdown.initial",
        "predicate_pushdown.remove",
        "collapse_single_parent.passthrough_after_pushdown",
    ]
    by_name = {phase.name: phase for phase in plan}
    assert by_name["predicate_pushdown.initial"].depends_on == ("inline_datasource",)


def test_pipeline_runs_datasource_inlining_before_join_hoist():
    with _optimization_flags(
        datasource_inlining=True,
        join_hoist=True,
        predicate_pushdown=True,
    ):
        plan = build_optimization_rule_plan()

    names = [phase.name for phase in plan]
    assert names == [
        "inline_datasource",
        "join_hoist",
        "predicate_pushdown.initial",
        "predicate_pushdown.remove",
        "collapse_single_parent.passthrough_after_pushdown",
    ]
    by_name = {phase.name: phase for phase in plan}
    assert by_name["join_hoist"].depends_on == ("inline_datasource",)
    assert by_name["predicate_pushdown.initial"].depends_on == (
        "inline_datasource",
        "join_hoist",
    )


def test_pipeline_marks_predicate_refire_dependency_on_union_dim_pushdown():
    with _optimization_flags(
        predicate_pushdown=True,
        upgrade_condition_joins=True,
        union_dim_pushdown=True,
    ):
        plan = build_optimization_rule_plan()

    by_name = {phase.name: phase for phase in plan}
    assert list(by_name) == [
        "predicate_pushdown.initial",
        "upgrade_join_on_guards.base_join_only",
        "union_dim_pushdown",
        "predicate_pushdown.after_union_dim",
        "predicate_pushdown.remove",
        "collapse_single_parent.passthrough_after_pushdown",
        "upgrade_join_on_guards.final",
        "predicate_pushdown.after_final_upgrade",
        "predicate_pushdown.remove.after_join_upgrades",
    ]
    assert by_name["union_dim_pushdown"].depends_on == (
        "predicate_pushdown.initial",
        "upgrade_join_on_guards.base_join_only",
    )
    assert by_name["predicate_pushdown.after_union_dim"].refires_after == (
        "union_dim_pushdown",
    )
    assert by_name["predicate_pushdown.remove"].depends_on == (
        "predicate_pushdown.after_union_dim",
    )
    assert by_name["predicate_pushdown.after_final_upgrade"].depends_on == (
        "upgrade_join_on_guards.final",
    )
    assert by_name["predicate_pushdown.after_final_upgrade"].refires_after == (
        "upgrade_join_on_guards.final",
    )


def test_pipeline_refires_group_merge_after_shape_cleanup():
    with _optimization_flags(
        merge_irrelevant_group_by=True,
        join_hoist=True,
        predicate_pushdown=True,
    ):
        plan = build_optimization_rule_plan()

    by_name = {phase.name: phase for phase in plan}
    assert list(by_name) == [
        "merge_irrelevant_group_by",
        "join_hoist",
        "predicate_pushdown.initial",
        "predicate_pushdown.remove",
        "merge_irrelevant_group_by.after_predicate_remove",
        "collapse_single_parent.passthrough_after_pushdown",
    ]
    assert by_name[
        "merge_irrelevant_group_by.after_predicate_remove"
    ].refires_after == ("predicate_pushdown.remove",)


def test_pipeline_orders_inner_joins_last_after_join_type_upgrades():
    with _optimization_flags(
        upgrade_condition_joins=True,
        upgrade_outer_key_set_equivalence=True,
        order_inner_joins_first=True,
    ):
        plan = build_optimization_rule_plan()

    names = [phase.name for phase in plan]
    assert names[-1] == "order_inner_joins_first"
    by_name = {phase.name: phase for phase in plan}
    assert by_name["order_inner_joins_first"].depends_on == (
        "upgrade_join_on_guards.final",
        "upgrade_outer_key_set_equivalence",
    )


def test_pipeline_omits_inner_join_ordering_when_disabled():
    with _optimization_flags(predicate_pushdown=True):
        plan = build_optimization_rule_plan()

    assert "order_inner_joins_first" not in [phase.name for phase in plan]


def test_pipeline_validation_rejects_forward_and_unknown_dependencies():
    with _optimization_flags(predicate_pushdown=True):
        plan = build_optimization_rule_plan()
    validate_optimization_rule_plan(plan)

    late = OptimizationRulePlan(
        name="late", rule_factory=PredicatePushdownRemove, depends_on=("early",)
    )
    early = OptimizationRulePlan(name="early", rule_factory=PredicatePushdownRemove)
    validate_optimization_rule_plan([early, late])
    with pytest.raises(ValueError, match="does not run before"):
        validate_optimization_rule_plan([late, early])
    with pytest.raises(ValueError, match="does not run before"):
        validate_optimization_rule_plan([late])
    with pytest.raises(ValueError, match="registered twice"):
        validate_optimization_rule_plan([early, early])


@pytest.mark.parametrize("flag", sorted(FLAGS))
def test_single_flag_plans_validate(flag: str):
    with _optimization_flags(**{flag: True}):
        build_optimization_rule_plan()


def test_all_flags_plan_validates():
    with _optimization_flags(**{flag: True for flag in FLAGS}):
        build_optimization_rule_plan()
