from trilogy import parse
from trilogy.core.enums import (
    JoinType,
    Modifier,
)
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import BaseJoin, ConceptPair, QueryDatasource
from trilogy.core.processing.utility import find_nullable_concepts


def test_find_nullable_concepts():
    env, _ = parse("""
key order_id int;
key product_id int;
key customer_id int;
property product_id.product_name string;
          
datasource orders (
    order_id:order_id,
    product_id:?product_id,)
grain (order_id)
query '''
select 1 as order_id, null as product_id, 1 as customer_Id
union all
select 2 as order_id, 2 as product_id, 1 as customer_id
union all
select 3 as order_id, 1 as product_id, 1 as customer_Id
          ''';

datasource products (
    product_id:product_id,
    name:product_name)
grain (product_id)
query '''
select 1 as product_id, 'product 1' as product_name
union all
select 2 as product_id, 'product 2' as product_name
          ''';

          
datasource customers (
    customer_id:customer_id,
    )
grain (customer_id)
query '''
select 1 as customer_id
            ''';

          """)
    env = env.materialize_for_select()
    order_id = env.concepts["order_id"]
    product_id = env.concepts["product_id"]
    product_name = env.concepts["product_name"]
    customer_id = env.concepts["customer_id"]
    order_ds = env.datasources["orders"]
    product_ds = env.datasources["products"]
    order_qds = QueryDatasource(
        input_concepts=[order_id, product_id],
        output_concepts=[order_id, product_id],
        datasources=[order_ds],
        grain=BuildGrain(components={order_id.address}),
        joins=[],
        source_map={order_id.address: {order_ds}, product_id.address: {order_ds}},
        nullable_concepts=[product_id],
    )
    product_qds = QueryDatasource(
        input_concepts=[product_id, product_name],
        output_concepts=[product_id, product_name],
        datasources=[product_ds],
        grain=BuildGrain(components={product_id.address}),
        joins=[],
        source_map={
            product_id.address: {product_ds},
            product_name.address: {product_ds},
        },
    )
    join = BaseJoin(
        left_datasource=order_qds,
        right_datasource=product_qds,
        join_type=JoinType.LEFT_OUTER,
        concepts=[],
        concept_pairs=[
            ConceptPair(
                left=product_id, right=product_id, existing_datasource=order_qds
            )
        ],
    )
    source_map = {
        order_id.address: {order_qds},
        product_id.address: {product_qds, order_qds},
        product_name.address: {product_qds},
    }
    assert join.concept_pairs[0].left in join.left_datasource.nullable_concepts
    nullable = find_nullable_concepts(
        source_map=source_map, datasources=[order_qds, product_qds], joins=[join]
    )
    assert nullable == [product_id.address, product_name.address], nullable
    order_qds

    # now check for straight inheritance
    source_map = {
        order_id.address: {order_qds},
        product_id.address: {order_qds},
    }
    customer_ds = env.datasources["customers"]
    customer_qds = QueryDatasource(
        input_concepts=[customer_id],
        output_concepts=[customer_id],
        datasources=[customer_ds],
        grain=BuildGrain(components={customer_id.address}),
        joins=[],
        source_map={
            customer_id.address: {customer_ds},
        },
    )

    nullable = find_nullable_concepts(
        source_map=source_map, datasources=[order_qds, customer_qds], joins=[]
    )
    assert nullable == [product_id.address], nullable


def test_find_nullable_concepts_missing_right_datasource_no_crash():
    # A join on a nullable condition whose right_datasource is a synthetic
    # self-join-key pseudonym absent from datasource_map must not KeyError
    # (regression: utility.py used a hard subscript instead of guarded .get).
    env, _ = parse("""
key order_id int;
key product_id int;
datasource orders (
    order_id:order_id,
    product_id:?product_id,)
grain (order_id)
query '''select 1 as order_id, null as product_id''';
""")
    env = env.materialize_for_select()
    order_id = env.concepts["order_id"]
    product_id = env.concepts["product_id"]
    order_ds = env.datasources["orders"]
    order_qds = QueryDatasource(
        input_concepts=[order_id, product_id],
        output_concepts=[order_id, product_id],
        datasources=[order_ds],
        grain=BuildGrain(components={order_id.address}),
        joins=[],
        source_map={order_id.address: {order_ds}, product_id.address: {order_ds}},
        nullable_concepts=[product_id],
    )
    synthetic_qds = QueryDatasource(
        input_concepts=[product_id],
        output_concepts=[product_id],
        datasources=[order_ds],
        grain=BuildGrain(components={product_id.address}),
        joins=[],
        source_map={product_id.address: {order_ds}},
    )
    join = BaseJoin(
        left_datasource=order_qds,
        right_datasource=synthetic_qds,
        join_type=JoinType.INNER,
        concepts=[],
        concept_pairs=[
            ConceptPair(
                left=product_id, right=product_id, existing_datasource=order_qds
            )
        ],
    )
    # `datasources` deliberately omits synthetic_qds → its identifier is absent
    # from datasource_map; the nullable product_id condition reaches the lookup.
    nullable = find_nullable_concepts(
        source_map={order_id.address: {order_qds}, product_id.address: {order_qds}},
        datasources=[order_qds],
        joins=[join],
    )
    assert product_id.address in nullable


def test_nullable_inheritance():
    env, _ = parse("""
key order_id int?;

auto ten_x_order <- order_id *10;
auto ten_x_order <- order_id *10;
property order_id.ten_x<- cast(order_id / 10 as int) * 10;
""")

    assert env.concepts["order_id"].modifiers == [Modifier.NULLABLE]
    assert env.concepts["ten_x_order"].modifiers == [Modifier.NULLABLE]
    assert env.concepts["ten_x"].modifiers == [Modifier.NULLABLE]


def _is_nullable(env, name: str) -> bool:
    return Modifier.NULLABLE in env.concepts[name].modifiers


def test_case_derivation_nullability():
    """A CASE that can fall through to NULL must be marked nullable; a fully
    covered CASE whose branches are all non-null must not be."""
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;

auto no_else <- case when qty > 0 then 'in' when qty = 0 then 'out' end;
auto with_else <- case when qty > 0 then 'in' else 'out' end;
auto else_null <- case when qty > 0 then 'in' else null end;
auto else_branch_nullable <- case when qty > 0 then 1 else nullif(qty, 0) end;
""")
    # no ELSE — unmatched rows fall through to NULL (the headline bug)
    assert _is_nullable(env, "no_else")
    # ELSE present, every branch value non-null — not nullable
    assert not _is_nullable(env, "with_else")
    # explicit ELSE NULL
    assert _is_nullable(env, "else_null")
    # ELSE present but a branch value is itself nullable
    assert _is_nullable(env, "else_branch_nullable")


def test_case_nullable_comparison_does_not_leak():
    """A nullable concept used only in a WHEN *condition* (not a branch value)
    does not make a covered CASE nullable."""
    env, _ = parse("""
key inv_id int;
property inv_id.qty int?;

auto covered <- case when qty > 0 then 'in' else 'out' end;
""")
    assert env.concepts["qty"].modifiers == [Modifier.NULLABLE]
    assert not _is_nullable(env, "covered")


def test_coalesce_nullability():
    """coalesce proves non-null when any fallback is non-null."""
    env, _ = parse("""
key inv_id int;
property inv_id.qty int?;

auto rescued <- coalesce(qty, 0);
auto still_null <- coalesce(qty, qty);
""")
    assert not _is_nullable(env, "rescued")
    assert _is_nullable(env, "still_null")


def test_nullif_nullability():
    env, _ = parse("""
key inv_id int;

auto maybe <- nullif(inv_id, 0);
""")
    # inv_id is non-null, but nullif(a, b) is NULL whenever a == b
    assert not _is_nullable(env, "inv_id")
    assert _is_nullable(env, "maybe")


def test_aggregate_nullability():
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;
property inv_id.qty_n int?;

auto cnt <- count(inv_id);
auto total_safe <- sum(qty);
auto total_nullable <- sum(qty_n);
""")
    assert not _is_nullable(env, "cnt")
    assert not _is_nullable(env, "total_safe")
    assert _is_nullable(env, "total_nullable")


def test_filtered_value_nullability():
    """A filtered value is a partial property of its base: at any grain wider
    than the filter it null-extends, so it (and any aggregate over it) is
    nullable even when the source is not. count stays non-null."""
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;
property inv_id.flag bool;

auto filtered <- qty ? flag;
auto total_filtered <- sum(qty ? flag);
auto cnt_filtered <- count(inv_id ? flag);
""")
    assert _is_nullable(env, "filtered")
    assert _is_nullable(env, "total_filtered")
    assert not _is_nullable(env, "cnt_filtered")


def test_window_nullability():
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;

auto ranking <- rank inv_id order by qty desc;
auto previous <- lag qty order by inv_id asc;
""")
    # rank always produces a value
    assert not _is_nullable(env, "ranking")
    # lag yields NULL past the partition edge
    assert _is_nullable(env, "previous")


def test_arithmetic_nullability_and_transitivity():
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;
property inv_id.qty_n int?;

auto safe_math <- qty * 10;
auto nullable_math <- qty_n * 10;
auto downstream <- nullable_math + 1;
""")
    assert not _is_nullable(env, "safe_math")
    assert _is_nullable(env, "nullable_math")
    # nullability propagates transitively through a second derivation
    assert _is_nullable(env, "downstream")


def test_case_without_else_but_provably_complete_is_not_nullable():
    """A CASE with no ELSE whose WHEN conditions provably cover the whole
    domain cannot fall through to NULL — reuses the datasource-injection
    completeness proof."""
    env, _ = parse("""
key inv_id int;
property inv_id.flag bool;
property inv_id.flag_n bool?;
key raw_channel string;
auto channel <- raw_channel::enum<string>['WEB','CATALOG','STORE'];

auto bool_full <- case when flag = true then 1 when flag = false then 2 end;
auto bool_partial <- case when flag = true then 1 end;
auto bool_nullable_switch <- case
    when flag_n = true then 1 when flag_n = false then 2 end;
auto int_range_full <- case when inv_id >= 0 then 1 when inv_id < 0 then 2 end;
auto enum_full <- case
    when channel = 'WEB' then 1
    when channel = 'CATALOG' then 2
    when channel = 'STORE' then 3 end;
auto enum_partial <- case
    when channel = 'WEB' then 1 when channel = 'CATALOG' then 2 end;
auto simple_enum_full <- case channel
    when 'WEB' then 1 when 'CATALOG' then 2 when 'STORE' then 3 end;
""")
    # provably exhaustive — no fall-through possible
    assert not _is_nullable(env, "bool_full")
    assert not _is_nullable(env, "int_range_full")
    assert not _is_nullable(env, "enum_full")
    assert not _is_nullable(env, "simple_enum_full")
    # not exhaustive — still nullable
    assert _is_nullable(env, "bool_partial")
    assert _is_nullable(env, "enum_partial")
    # exhaustive over values, but a nullable switch can itself be NULL
    assert _is_nullable(env, "bool_nullable_switch")


def test_derived_nullable_folds_into_query_datasource():
    """A QueryDatasource must report an intrinsically-nullable derived output
    in ``nullable_concepts`` even though it is not a datasource column."""
    env, _ = parse("""
key inv_id int;
property inv_id.qty int;

auto shelf_status <- case when qty > 0 then 'in' end;

datasource inventory (
    inv_id: inv_id,
    qty: qty,
)
grain (inv_id)
query '''select 1 as inv_id, 5 as qty''';
""")
    env = env.materialize_for_select()
    inv_id = env.concepts["inv_id"]
    qty = env.concepts["qty"]
    shelf_status = env.concepts["shelf_status"]
    assert Modifier.NULLABLE in shelf_status.modifiers
    ds = env.datasources["inventory"]
    qds = QueryDatasource(
        input_concepts=[inv_id, qty],
        output_concepts=[inv_id, qty, shelf_status],
        datasources=[ds],
        grain=BuildGrain(components={inv_id.address}),
        joins=[],
        source_map={
            inv_id.address: {ds},
            qty.address: {ds},
            shelf_status.address: {ds},
        },
        nullable_concepts=[],
    )
    assert shelf_status.address in [c.address for c in qds.nullable_concepts]
