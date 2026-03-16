from trilogy.core.enums import ComparisonOperator, Derivation, SourceType
from trilogy.core.models.build import BuildComparison, BuildConcept, BuildGrain
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import CTE, QueryDatasource
from trilogy.core.optimizations.merge_irrelevant_group_by import (
    MergeIrrelevantGroupBy,
    _is_group_by_cte,
)


def _get_cols(test_environment: Environment) -> list[BuildConcept]:
    env = test_environment.materialize_for_select()
    ds = list(env.datasources.values())[0]
    return [c.concept for c in ds.columns]


def _make_group_cte(
    name: str,
    cols: list[BuildConcept],
    parent_ctes: list | None = None,
) -> CTE:
    ds = QueryDatasource(
        input_concepts=cols,
        output_concepts=cols,
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={c.address: set() for c in cols},
        source_type=SourceType.GROUP,
    )
    return CTE(
        name=name,
        source=ds,
        output_columns=list(cols),
        grain=BuildGrain(),
        source_map={c.address: [name + "_src"] for c in cols},
        parent_ctes=parent_ctes or [],
        group_to_grain=True,
    )


def test_is_group_by_cte(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    col = cols[0]
    group_cte = _make_group_cte("a", cols)
    assert _is_group_by_cte(group_cte) is True

    ds = QueryDatasource(
        input_concepts=[col],
        output_concepts=[col],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={col.address: set()},
        source_type=SourceType.SELECT,
    )
    non_group = CTE(
        name="b",
        source=ds,
        output_columns=[col],
        grain=BuildGrain(),
        source_map={},
    )
    assert _is_group_by_cte(non_group) is False


def test_basic_merge(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    parent = _make_group_cte("parent", cols)
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier

    inverse_map: dict = {parent.name: [child]}
    rule = MergeIrrelevantGroupBy()
    merged, cte_map = rule.optimize(child, inverse_map)

    assert merged is True
    assert cte_map == {child.name: parent.name}


def test_no_merge_when_parent_has_multiple_children(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    parent = _make_group_cte("parent", cols)
    child1 = _make_group_cte("child1", cols, parent_ctes=[parent])
    child1.base_alias_override = parent.safe_identifier
    child2 = _make_group_cte("child2", cols, parent_ctes=[parent])

    inverse_map: dict = {parent.name: [child1, child2]}
    rule = MergeIrrelevantGroupBy()
    merged, _ = rule.optimize(child1, inverse_map)
    assert merged is False


def test_no_merge_non_group_parent(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    col = cols[0]
    ds = QueryDatasource(
        input_concepts=[col],
        output_concepts=[col],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={col.address: set()},
        source_type=SourceType.SELECT,
    )
    parent = CTE(
        name="parent",
        source=ds,
        output_columns=[col],
        grain=BuildGrain(),
        source_map={col.address: ["src"]},
    )
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier

    inverse_map: dict = {parent.name: [child]}
    rule = MergeIrrelevantGroupBy()
    merged, _ = rule.optimize(child, inverse_map)
    assert merged is False


def test_no_merge_with_condition(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    col = cols[0]
    parent = _make_group_cte("parent", cols)
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier
    child.condition = BuildComparison(left=col, right=1, operator=ComparisonOperator.EQ)

    inverse_map: dict = {parent.name: [child]}
    rule = MergeIrrelevantGroupBy()
    merged, _ = rule.optimize(child, inverse_map)
    assert merged is False


def test_no_merge_ineligible_derivation(test_environment: Environment):
    env = test_environment.materialize_for_select()
    agg_cols = [
        c for c in env.concepts.values() if c.derivation == Derivation.AGGREGATE
    ]
    if not agg_cols:
        return
    cols = _get_cols(test_environment)[:1] + [agg_cols[0]]
    parent = _make_group_cte("parent", cols)
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier

    inverse_map: dict = {parent.name: [child]}
    rule = MergeIrrelevantGroupBy()
    merged, _ = rule.optimize(child, inverse_map)
    assert merged is False


def test_no_double_merge(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    parent = _make_group_cte("parent", cols)
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier

    inverse_map: dict = {parent.name: [child]}
    rule = MergeIrrelevantGroupBy()
    assert rule.optimize(child, inverse_map)[0] is True
    assert rule.optimize(child, inverse_map)[0] is False


def test_consumers_repointed_after_merge(test_environment: Environment):
    cols = _get_cols(test_environment)[:1]
    parent = _make_group_cte("parent", cols)
    child = _make_group_cte("child", cols, parent_ctes=[parent])
    child.base_alias_override = parent.safe_identifier
    grandchild = _make_group_cte("grandchild", cols, parent_ctes=[child])

    inverse_map: dict = {parent.name: [child], child.name: [grandchild]}
    rule = MergeIrrelevantGroupBy()
    merged, _ = rule.optimize(child, inverse_map)

    assert merged is True
    assert parent in grandchild.parent_ctes
    assert child not in grandchild.parent_ctes
