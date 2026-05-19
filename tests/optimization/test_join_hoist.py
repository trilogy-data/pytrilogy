from trilogy import Dialects
from trilogy.constants import MagicConstants
from trilogy.core.enums import BooleanOperator, ComparisonOperator, JoinType, SourceType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildGrain,
    BuildSubselectComparison,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    CTEConceptPair,
    Join,
    QueryDatasource,
)
from trilogy.core.optimizations.join_hoist import JoinHoist


def _hoist_setup(executor):
    """Tiny per-day fact table with two channels — enough to set up two
    parallel cumulative windows whose comparison gets considered for hoist."""
    executor.execute_text("""
        key id int;
        property id.channel string;
        property id.day int;
        property id.amount int;

        datasource sales (
            id: id,
            channel: channel,
            day: day,
            amount: amount,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'WEB' AS channel, 1 AS day, 10 AS amount UNION ALL
        SELECT 2, 'STORE', 1, 5 UNION ALL
        SELECT 3, 'WEB', 2, 20 UNION ALL
        SELECT 4, 'STORE', 2, 30
        ''';
    """)


def test_hoist_preserves_concepts_referenced_via_output_lineage():
    """Regression: ``JoinHoist`` would strip a join the outer SELECT still
    needs through an alias's lineage. ``store_c <- alias(store_cume)`` makes
    ``store_cume`` reachable only via the joined CTE; without walking output
    lineage, the optimizer hoisted the join + cume comparison and the renderer
    fell back to ``INVALID_REFERENCE_BUG_<...>`` markers in the emitted SQL."""
    executor = Dialects.DUCK_DB.default_executor()
    _hoist_setup(executor)

    queries = executor.parse_text("""
        auto web_daily <- sum(amount ? channel = 'WEB') by day;
        auto store_daily <- sum(amount ? channel = 'STORE') by day;
        property day.web_cume <- sum web_daily over day order by day asc;
        property day.store_cume <- sum store_daily over day order by day asc;
        auto web_visible <- case when web_daily is not null then web_cume else null end;
        auto store_visible <- case when store_daily is not null then store_cume else null end;

        WHERE channel in ('WEB', 'STORE') and web_cume > store_cume
        SELECT
            day,
            web_visible as web_v,
            store_visible as store_v,
            web_cume as web_c,
            store_cume as store_c
        ORDER BY day asc;
    """)
    sql = executor.generate_sql(queries[-1])[0]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = executor.execute_raw_sql(sql).fetchall()
    # day=1 has web_cume=10 < store_cume=5? actually 10 > 5 → row survives
    # day=2 has web_cume=30 vs store_cume=35 → fails predicate → only day=1 row
    assert len(rows) == 1
    assert rows[0].day == 1
    assert rows[0].web_c == 10
    assert rows[0].store_c == 5


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


def _category_filter_child(
    parent: CTE,
    dim: CTE,
    product_id,
    category_id,
    category_name,
    condition: BuildComparison,
) -> CTE:
    return CTE(
        name="child",
        source=QueryDatasource(
            input_concepts=[product_id, category_id, category_name],
            output_concepts=[product_id],
            datasources=[parent.source, dim.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {parent.source},
                category_id.address: {parent.source, dim.source},
                category_name.address: {dim.source},
            },
        ),
        output_columns=[product_id],
        parent_ctes=[parent, dim],
        condition=condition,
        grain=BuildGrain(),
        source_map={
            product_id.address: [parent.name],
            category_id.address: [parent.name, dim.name],
            category_name.address: [dim.name],
        },
        existence_source_map={},
        joins=[
            Join(
                right_cte=dim,
                jointype=JoinType.LEFT_OUTER,
                left_cte=parent,
                joinkey_pairs=[
                    CTEConceptPair(
                        left=category_id,
                        right=category_id,
                        existing_datasource=parent.source,
                        cte=parent,
                    )
                ],
            )
        ],
    )


def test_join_hoist_rejects_null_preserving_outer_join(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    product_id = env.concepts["product_id"]
    parent = _simple_cte("parent", products, [product_id, category_id])
    dim = _simple_cte("category_dim", category, [category_id, category_name])
    child = _category_filter_child(
        parent,
        dim,
        product_id,
        category_id,
        category_name,
        BuildComparison(
            left=category_name,
            right=MagicConstants.NULL,
            operator=ComparisonOperator.IS,
        ),
    )

    plan = JoinHoist()._join_hoist_plan(child, parent, {parent.name: [child]})

    assert plan is None


def test_join_hoist_pushes_guarded_left_join_into_base_parent(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    product_id = env.concepts["product_id"]
    parent = _simple_cte("parent", products, [product_id, category_id])
    parent.source.source_type = SourceType.GROUP
    dim = _simple_cte("category_dim", category, [category_id, category_name])
    condition = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    child = _category_filter_child(
        parent,
        dim,
        product_id,
        category_id,
        category_name,
        condition,
    )

    changed, _ = JoinHoist().optimize(child, {parent.name: [child]})

    assert changed is True
    assert child.condition is None
    assert child.joins == []
    assert parent.condition == condition
    assert len(parent.joins) == 1
    assert parent.joins[0].jointype == JoinType.INNER
    assert len(parent.source.joins) == 1
    assert isinstance(parent.source.joins[0], BaseJoin)
    assert parent.source.joins[0].join_type == JoinType.INNER


def test_join_hoist_preserves_inlined_dim_on_parent(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    product_id = env.concepts["product_id"]
    parent = _simple_cte("parent", products, [product_id, category_id])
    parent.source.source_type = SourceType.GROUP
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    condition = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    child = _category_filter_child(
        parent,
        dim,
        product_id,
        category_id,
        category_name,
        condition,
    )
    assert child.inline_parent_datasource(dim) is True

    changed, _ = JoinHoist().optimize(child, {parent.name: [child]})

    assert changed is True
    assert child.condition is None
    assert child.joins == []
    assert parent.condition == condition
    assert parent.inlined_parents == [dim]
    assert dim not in parent.parent_ctes
    assert parent.source_map[category_name.address] == [parent.source_key_for(dim)]
    assert parent.source_map[category_name.address] == [category.safe_identifier]


def test_join_hoist_skips_existence_only_parent(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    parent = _simple_cte("existence_parent", products, [category_id])
    parent.source.source_type = SourceType.GROUP
    dim = _simple_cte("category_dim", category, [category_id, category_name])
    dim_filter = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    existence_filter = BuildSubselectComparison(
        left=category_id,
        right=category_id,
        operator=ComparisonOperator.IN,
    )
    child = CTE(
        name="child",
        source=QueryDatasource(
            input_concepts=[category_id, category_name],
            output_concepts=[category_id],
            datasources=[parent.source, dim.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                category_id.address: {parent.source, dim.source},
                category_name.address: {dim.source},
            },
        ),
        output_columns=[category_id],
        parent_ctes=[parent, dim],
        condition=BuildConditional(
            left=dim_filter,
            right=existence_filter,
            operator=BooleanOperator.AND,
        ),
        grain=BuildGrain(),
        source_map={
            category_id.address: [parent.name, dim.name],
            category_name.address: [dim.name],
        },
        existence_source_map={},
        joins=[
            Join(
                right_cte=dim,
                jointype=JoinType.INNER,
                left_cte=parent,
                joinkey_pairs=[
                    CTEConceptPair(
                        left=category_id,
                        right=category_id,
                        existing_datasource=parent.source,
                        cte=parent,
                    )
                ],
            )
        ],
    )

    changed, _ = JoinHoist().optimize(child, {parent.name: [child]})

    assert changed is False
    assert child.joins
    assert parent.joins == []
    assert parent.condition is None


def test_join_hoist_rejects_single_consumer_non_group_parent(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    product_id = env.concepts["product_id"]
    parent = _simple_cte("parent", products, [product_id, category_id])
    dim = _simple_cte("category_dim", category, [category_id, category_name])
    child = _category_filter_child(
        parent,
        dim,
        product_id,
        category_id,
        category_name,
        BuildComparison(
            left=category_name,
            right="special",
            operator=ComparisonOperator.EQ,
        ),
    )

    changed, _ = JoinHoist().optimize(child, {parent.name: [child]})

    assert changed is False
    assert child.joins
    assert parent.joins == []
