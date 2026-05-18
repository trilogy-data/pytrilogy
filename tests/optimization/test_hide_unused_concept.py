from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.optimizations.hide_unused_concept import HideUnusedConcepts


def _simple_cte(name: str, datasource, columns) -> CTE:
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=list(columns),
            output_concepts=list(columns),
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={c.address: {datasource} for c in columns},
            base_datasource=datasource,
        ),
        output_columns=list(columns),
        parent_ctes=[],
        grain=BuildGrain(),
        source_map={c.address: [datasource.name] for c in columns},
        existence_source_map={},
    )


def _simple_union(name: str, branches: list[CTE], columns) -> UnionCTE:
    return UnionCTE(
        name=name,
        source=QueryDatasource(
            input_concepts=list(columns),
            output_concepts=list(columns),
            datasources=[b.source for b in branches],
            grain=BuildGrain(),
            joins=[],
            source_map={c.address: {b.source for b in branches} for c in columns},
        ),
        parent_ctes=list(branches),
        internal_ctes=list(branches),
        output_columns=list(columns),
        grain=BuildGrain(),
    )


def test_hide_branch_only_union_outputs(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    branch = _simple_cte("branch", products, [product_id, category_id])
    union = _simple_union("unioned", [branch], [product_id])

    assert HideUnusedConcepts()._hide_branch_only_outputs(union) is True
    assert category_id.address in branch.hidden_concepts
    assert product_id.address not in branch.hidden_concepts


def test_hide_branch_only_keeps_one_visible_projection(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    product_id = env.concepts["product_id"]
    branch = _simple_cte("branch", products, [product_id])
    union = _simple_union("unioned", [branch], [])

    assert HideUnusedConcepts()._hide_branch_only_outputs(union) is False
    assert branch.hidden_concepts == set()


def test_hide_unused_keeps_group_key_used_by_visible_lineage(
    test_environment, monkeypatch
):
    env = test_environment.materialize_for_select()
    category = env.datasources["category"]
    category_name = env.concepts["category_name"]
    category_name_length = env.concepts["category_name_length"]
    parent = _simple_cte("parent", category, [category_name, category_name_length])
    parent.group_to_grain = True
    child = _simple_cte("child", category, [category_name_length])

    def used_map(_cte):
        return {parent.name: {category_name_length.address}}

    monkeypatch.setattr(
        "trilogy.core.optimizations.hide_unused_concept.render_cte_used_map",
        used_map,
    )

    changed, _ = HideUnusedConcepts().optimize(parent, {parent.name: [child]})

    assert changed is False
    assert parent.hidden_concepts == set()
