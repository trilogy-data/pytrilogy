from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.core.models import QueryDatasource, BaseJoin, Grain, JoinType, ConceptPair
from trilogy import parse
from trilogy.core.enums import Modifier


def test_find_nullable_concepts():

    env, _ = parse(
        """
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

          """
    )
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
        grain=Grain(components=[order_id]),
        joins=[],
        source_map={order_id.address: {order_ds}, product_id.address: {order_ds}},
        nullable_concepts=[product_id],
    )
    product_qds = QueryDatasource(
        input_concepts=[product_id, product_name],
        output_concepts=[product_id, product_name],
        datasources=[product_ds],
        grain=Grain(components=[product_id]),
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
        concept_pairs=[ConceptPair(left=product_id, right=product_id)],
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
        grain=Grain(components=[customer_id]),
        joins=[],
        source_map={
            customer_id.address: {customer_ds},
        },
    )

    nullable = find_nullable_concepts(
        source_map=source_map, datasources=[order_qds, customer_qds], joins=[]
    )
    assert nullable == [product_id.address], nullable


def test_nullable_inheritance():
    env, _ = parse(
        """
key order_id int?;

auto ten_x_order <- order_id *10;
auto ten_x_order <- order_id *10;
property order_id.ten_x<- cast(order_id / 10 as int) * 10;
"""
    )

    assert env.concepts["order_id"].modifiers == [Modifier.NULLABLE]
    assert env.concepts["ten_x_order"].modifiers == [Modifier.NULLABLE]
    assert env.concepts["ten_x"].modifiers == [Modifier.NULLABLE]
