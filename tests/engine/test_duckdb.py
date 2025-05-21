from datetime import datetime, timedelta
from pathlib import Path

import networkx as nx

from trilogy import Dialects
from trilogy.core.enums import Derivation, FunctionType, Granularity, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.author import (
    Concept,
    Grain,
)
from trilogy.core.models.build import BuildFilterItem, BuildSubselectComparison, Factory
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import get_upstream_concepts
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_function_parent_concepts,
)
from trilogy.core.statements.author import ShowStatement
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse_text


def test_basic_query(duckdb_engine: Executor, expected_results):
    graph = generate_graph(duckdb_engine.environment.materialize_for_select())

    list(nx.neighbors(graph, "c~local.count@Grain<local.item,local.store_id>"))
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_concept_derivation():
    duckdb_engine = Dialects.DUCK_DB.default_executor()
    test_datetime = datetime(hour=12, day=1, month=2, year=2022, second=34)

    duckdb_engine.execute_text(
        f"""const test <- cast('{test_datetime.isoformat()}' as datetime);
    """
    )
    for property, check in [
        ["date", test_datetime.date()],
        ["hour", test_datetime.hour],
        ["second", test_datetime.second],
        ["minute", test_datetime.minute],
        ["year", test_datetime.year],
        ["month", test_datetime.month],
    ]:
        # {test_datetime.isoformat()}
        test_query = f""" 


        select local.test.{property};
        
        """
        query = duckdb_engine.parse_text(test_query)
        name = f"local.test.{property}"
        assert duckdb_engine.environment.concepts[name].address == name
        assert query[-1].output_columns[0].address == f"local.test.{property}"
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert results[0][0] == check
    for truncation in [
        "month",
        "year",
    ]:
        test_query = f"""
        select local.test.{truncation}_start;
        """
        query = duckdb_engine.parse_text(test_query)
        name = f"local.test.{truncation}_start"
        assert duckdb_engine.environment.concepts[name].address == name
        assert query[-1].output_columns[0].address == f"local.test.{truncation}_start"
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert (
            results[0][0]
            == test_datetime.replace(
                day=1,
                month=1 if truncation == "year" else test_datetime.month,
                hour=0,
                minute=0,
                second=0,
            ).date()
        )


def test_render_query(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results


def test_aggregate_at_grain(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select avg_count_per_product;""")[
        0
    ].fetchall()
    assert results[0].avg_count_per_product == expected_results["avg_count_per_product"]


def test_empty_string(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select '' as empty_string;""")[0].fetchall()
    assert results[0].empty_string == ""


def test_order_of_operations(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_query(
        """
    const x <- 7;
    const y <- 8;

    auto z <- x + y;
    auto a <- z/2;
                                          
    select a;
"""
    ).fetchall()
    assert results[0].a == 7.5, results[0].a


def test_constants(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """const usd_conversion <- 2;

    auto converted_total_count <-  total_count * usd_conversion;
    
    select converted_total_count ;
    """
    )[0].fetchall()
    # expected_results["converted_total_count"]

    scaled_metric = duckdb_engine.environment.concepts["converted_total_count"]

    assert (
        duckdb_engine.environment.concepts["usd_conversion"].granularity
        == Granularity.SINGLE_ROW
    )
    parent_arg: Concept = duckdb_engine.environment.concepts[
        [
            x.address
            for x in scaled_metric.lineage.concept_arguments
            if x.address == "local.total_count"
        ][0]
    ]
    assert (
        len(
            duckdb_engine.environment.concepts[
                parent_arg.lineage.concept_arguments[0].address
            ].grain.components
        )
        == 2
    )
    # assert Grain(components = [duckdb_engine.environment.concepts['usd_conversion']]) == Grain()
    assert results[0].converted_total_count == expected_results["converted_total_count"]


def test_unnest(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """const array <- [1,2,3];
    """
    )
    array = duckdb_engine.environment.concepts["array"]
    assert array.lineage
    assert array.lineage.arguments[0] == [1, 2, 3]

    results = duckdb_engine.execute_text(
        """const array <- [1,2,3];
const unnest_array <- unnest(array);
    
    select unnest_array
    order by unnest_array asc;
    """
    )[0].fetchall()
    assert [x.unnest_array for x in results] == [1, 2, 3]


def test_partial(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """select item, sum(discount_value)-> total_discount
        order by item desc;
    """
    )[0].fetchall()
    assert len(results) == 2


def test_show(duckdb_engine: Executor, expected_results):
    test = """show 
select
    item, 
    sum(discount_value)-> total_discount
order by item desc;
    """

    _, parsed_0 = parse_text(test, duckdb_engine.environment)

    assert len(parsed_0) == 1
    assert isinstance(parsed_0[0], ShowStatement)

    parsed = duckdb_engine.parse_text(test)

    assert len(parsed) == 1
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert '"fact_items"."item" as "item"' in results[0]["__preql_internal_query_text"]


def test_rollback(duckdb_engine: Executor, expected_results):
    try:
        _ = duckdb_engine.execute_raw_sql("select abc")
    except Exception:
        pass

    results = duckdb_engine.execute_raw_sql("select 1")
    assert results.fetchall()[0] == (1,)


def test_basic(duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  auto tomorrow <- date_add(today, day, 1);
  select 
    tomorrow,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year 
  ;
    """
    duckdb_engine.parse_text(test)
    assert (
        duckdb_engine.environment.concepts["tomorrow"].granularity
        == Granularity.SINGLE_ROW
    )
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 3


def test_rowset(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, z where z = 1;

select my_rowset.x, my_rowset.z;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain.components == {
        x.address,
    }
    assert str(z) == "local.z@Grain<local.x>"
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1


def test_rowset_agg(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, max(z)->max_rank;

select my_rowset.x, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3


def test_rowset_join(duckdb_engine: Executor):
    test = """
key x int;

datasource x_data (
x:x)
query '''
select unnest([1,2,2,3]) as x
'''
;
    
const y <- 5;
auto z <- rank x order by x desc;
auto w <- rank x order by x asc;

rowset my_rowset <- select x, max(z)->max_rank;

select x, w, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3
    for row in results:
        if row.x == 2:
            assert row.my_rowset_max_rank == 2


def test_default_engine(default_duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  
  select 
    date_add(today, day, 1)->tomorrow,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year 
  ;
    """
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 3


def test_complex(default_duckdb_engine: Executor):
    test = """
const list <- [1,2,2,3];
const orid <- unnest(list);

auto half_orid <- ((orid+17)/2);

select 
    orid,
    half_orid,
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    listc = default_duckdb_engine.environment.concepts["list"]
    assert listc.purpose == Purpose.CONSTANT
    orid = default_duckdb_engine.environment.concepts["orid"]
    half = default_duckdb_engine.environment.concepts["half_orid"]
    assert orid.address in [x.address for x in half.concept_arguments]
    assert set([x for x in half.keys]) == {
        "local.orid",
    }
    assert half.lineage.operator == FunctionType.DIVIDE
    assert half.derivation == Derivation.BASIC
    assert half.granularity == Granularity.MULTI_ROW
    assert len(results) == 4


def test_array_inclusion(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
const orid <- unnest(list);
const orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where orid_2 in even_orders;

select 
    filtered_even_orders
where
    filtered_even_orders
;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3


def test_array_inclusion_aggregate_one(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    default_duckdb_engine.hooks = [DebuggingHook()]
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where orid_2 in even_orders;
metric f_ord_count <- count(filtered_even_orders);

select 
    f_ord_count
;
    """
    _ = default_duckdb_engine.parse_text(test)[-1]
    orig_env = default_duckdb_engine.environment
    env = orig_env.materialize_for_select()
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, BuildFilterItem)
    assert isinstance(agg_parent.lineage.where.conditional, BuildSubselectComparison)
    _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3


def test_array_inclusion_aggregate(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    default_duckdb_engine.hooks = [DebuggingHook()]
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where 1=1 and orid_2 in even_orders;
metric f_ord_count <- count(filtered_even_orders);
metric ord_count <- count(orid_2);

select 
    f_ord_count
;
    """
    _ = default_duckdb_engine.parse_text(test)[-1]
    orig_env = default_duckdb_engine.environment
    env = orig_env.materialize_for_select()
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, BuildFilterItem)
    _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3
    comp = """
where orid_2 in even_orders
select
    ord_count
;
"""
    results = default_duckdb_engine.execute_text(comp)[0].fetchall()
    assert results[0].ord_count == 3


def test_agg_demo(default_duckdb_engine: Executor):
    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource agg_example(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
''';

auto avg_customer_orders <- avg(customer_orders);

select 
    avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    build_env = default_duckdb_engine.environment.materialize_for_select()
    customer_orders = build_env.concepts["customer_orders"]
    assert set([x for x in customer_orders.keys]) == {"local.customer"}
    assert set([x for x in customer_orders.grain.components]) == {"local.customer"}

    customer_orders_2 = customer_orders
    assert set([x for x in customer_orders_2.keys]) == {"local.customer"}
    assert set([x for x in customer_orders_2.grain.components]) == {"local.customer"}

    count_by_customer = build_env.concepts[
        "avg_customer_orders"
    ].lineage.concept_arguments[0]
    # assert isinstance(count_by_customer, AggregateWrapper)
    assert set([x for x in count_by_customer.keys]) == {"local.customer"}
    assert set([x for x in count_by_customer.grain.components]) == {"local.customer"}
    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 1.33


def test_constant_group(default_duckdb_engine: Executor):
    test = """
const x <- 1;
const x2 <- x+1;

auto constant_group_orid <- unnest([1,2,3]);
property constan_group_orid.mod_two <- constant_group_orid % 2;

select 
    mod_two,
    x2
order by
    mod_two asc
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (0, 2)


def test_case_group():

    default_duckdb_engine = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    test = """
const x <- 1;
const x2 <- x+1;

auto orid <- unnest([1,2,3,6,10]);
property orid.mod_two <- orid % 2;

property orid.cased <-CASE WHEN mod_two = 0 THEN 1 ELSE 0 END;

auto total_mod_two <- sum(cased);

select 
    total_mod_two
  ;
    """
    factory = Factory(environment=default_duckdb_engine.environment)
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    cased = factory.build(default_duckdb_engine.environment.concepts["cased"])
    total = factory.build(default_duckdb_engine.environment.concepts["total_mod_two"])
    assert cased.purpose == Purpose.PROPERTY
    assert cased.keys == {"local.orid"}
    assert total.derivation == Derivation.AGGREGATE
    x = resolve_function_parent_concepts(
        total, environment=default_duckdb_engine.environment
    )
    assert len(cased.concept_arguments) == 1
    assert "local.orid" in get_upstream_concepts(cased)

    # for x in total.lineage.concept_arguments:
    #     if isinstance(x, Concept) and x.purpose == Purpose.PROPERTY and x.keys:
    #         raise SyntaxError(x.keys)
    assert "local.cased" in x
    assert "local.orid" in x
    # function_to_concept(
    #     parent=function_to_concept()
    # )
    assert results[0] == (3,)


def test_demo_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([1,2,2,3]);

auto even_x <- filter x where (x % 2) = 0;

select 
    x, 
    even_x
order by x 
asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (1, None)
    assert results[1] == (2, 2)
    assert len(results) == 4


def test_demo_filter_select():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """const x <- unnest([1,2,2,3]);

select
  ~x,
  x*x*x -> x_cubed
where 
  (x % 2) = 0;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2, 8)
    assert len(results) == 2


def test_demo_filter_rowset():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const x <- unnest([1,2,3,4]);

with even_squares as select 
    x, 
    x*x as x_squared
having (x_squared %2) = 0;

select 
    even_squares.x_squared
order by
    even_squares.x_squared asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)
    assert len(results) == 2


def test_filter_count():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([1,2,2,3]);

auto y <- x+1;

auto odd_y <- filter x where (x % 2) = 0;

select 
    count(odd_y) -> odd_y_count,
    count(x) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2, 4)
    assert len(results) == 1


def test_boolean_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([0, 1,2,2,3]);

select 
    count(x ? x ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)


def test_nullif_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([0, 1,2,2,3]);

select 
    count(x ? nullif(x, 2) ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2,)


def test_mod_parse_order():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const x <- unnest([1,2,3,4]);

with even_squares as select 
    x, 
    x*x as x_squared
having x_squared %2  = 0;

select 
    even_squares.x_squared
order by
    even_squares.x_squared asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)
    assert len(results) == 2


def test_raw_sql():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
raw_sql('''
select unnest([1,2,3,4]) as x
order by x asc
''')
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (1,)
    assert len(results) == 4


def test_numeric():
    from decimal import Decimal

    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const number <- 1.456789;
const reduced <- cast(number as numeric(3,2));

select reduced;
"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (Decimal("1.46"),)
    assert len(results) == 1


def test_filter_promotion(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
SELECT
    item
where
    value>1;

"""

    duckdb_engine.hooks = [DebuggingHook()]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 2


def test_filter_promotion_complicated(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
auto all_store_count <- sum(count);
SELECT
    item,
    all_store_count
where
    store_id in (1,3)
    and item = 'hammer'
order by
    item desc;
"""

    duckdb_engine.hooks = [DebuggingHook()]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    # derived = duckdb_engine.environment.concepts["all_store_count"]
    # assert isinstance(derived.lineage, AggregateWrapper)
    # assert derived.lineage.by == [duckdb_engine.environment.concepts["item"]]
    assert len(results) == 1
    assert results[0] == ("hammer", 4)


def test_filtered_datasource():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
'''
where store = 'store2';


select 
    avg(customer_orders) -> avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 2


def test_cte_filter_promotion():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    test = """key orid int;
key store string;
key customer int;


datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
''';


with orders_145 as
SELECT
    store,
    count(orid) -> store_order_count
where
    customer=145
;

select
    orders_145.store,
    orders_145.store_order_count
;


"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert round(results[0].orders_145_store_order_count, 2) == 1


def test_filter_promotion_inline_aggregate_filtered(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """


WHERE
    store_id = 1
SELECT
    item,
    sum(count) -> all_store_count
having
    all_store_count > 1
order by
    item desc;
"""

    duckdb_engine.hooks = [DebuggingHook()]

    # assert target.grain.components == [duckdb_engine.environment.concepts["item"]]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    # derived = parsed.local_concepts["local.all_store_count"]
    # assert isinstance(derived.lineage, AggregateWrapper)
    # assert derived.lineage.by == [duckdb_engine.environment.concepts["item"]]
    assert len(results) == 1
    assert results[0] == ("hammer", 2)


def test_duckdb_load():
    env = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=env)

    results = exec.execute_query(
        r"""
        auto csv <- _env_working_path || '/test.csv';

        RAW_SQL('''
        CREATE TABLE ages AS FROM read_csv(:csv);
        '''
        );"""
    )

    results = exec.execute_raw_sql("SELECT * FROM ages;").fetchall()

    assert results[0].age == 23


def test_duckdb_string_quotes():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(
        r"""
        const csv <- '''this string has quotes ' like this''';

    select csv;
        """
    )

    results = results.fetchall()

    assert results[0].csv == """this string has quotes ' like this"""


def test_demo_recursive_error():
    query = """key idx int;
property idx.idx_val int;
datasource numbers(
    idx: idx,
    x: idx_val
)
grain (idx)
query '''
select 1 idx, 1 x
union all
select 2, 2
union all
select 3, 2
union all
select 4, 3
''';

SELECT
  idx_val,
  count(idx) as number_count
order by
    idx_val asc;"""

    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(query)

    results = results.fetchall()


def test_union():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(
        r"""
key space_one int;
key space_two int;


property space_one.one_name string;
property space_two.two_name string;

auto space_all <- union(space_one, space_two);
auto name <- union(one_name, two_name);

datasource sone (
x: space_one,
y: one_name )
grain (space_one)
query '''
select 2 as x , 'test' as y
union all
select 1, 'fun'
''';


datasource stwo (
x: space_two,
y: two_name )
grain (space_two)
query '''
select 4 as x, 'bravo' as y
union all
select 5, 'alpha'
''';



select
    space_all,
    name,
order by
    space_all asc
limit 100;
        """
    )

    results = list(results.fetchall())

    assert results[0].space_all == 1
    assert results[0].name == "fun"
    assert results[-1].space_all == 5


def test_multi_select_mutation():
    exec = Dialects.DUCK_DB.default_executor()

    queries = exec.parse_text(
        """

auto x <- 1;
                    
select
    x + 1 -> x_next;
                    
select
    x + 2 -> x_next;
                    
"""
    )

    for idx, x in enumerate(queries):
        results = exec.execute_query(x).fetchall()
        assert results[0].x_next == 2 + idx


def test_commit():
    exec = Dialects.DUCK_DB.default_executor()
    exec.connection.begin()
    exec.execute_raw_sql("create table test (test int);")
    exec.connection.commit()
    exec.connection.begin()
    exec.execute_raw_sql("insert into test values (1);")
    exec.execute_raw_sql("insert into test values (2);")
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 2
    exec.connection.rollback()
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 0


def test_parquet_format_access():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    parquet_path = Path(__file__).parent / "customer.parquet"
    nations_path = Path(__file__).parent / "nation.parquet"
    executor.parse_text(
        f"""

key id int;
property id.text_id string;
property id.last_name string;
property id.first_name string;
property id.salutation string;

property id.full_name <- concat(salutation, ' ', first_name, ' ', last_name);

key nation_id int;
property nation_id.nation_name string;

datasource customers (
    C_CUSTKEY: id,
    C_NAME: full_name,
    C_NATIONKEY: nation_id,
)
grain (id)
address `{parquet_path}`;

datasource nations (
    N_NATIONKEY: nation_id,
    N_NAME: nation_name,
)
grain(nation_id)
address `{nations_path}`;
"""
    )
    _ = executor.execute_raw_sql(f'select * from "{parquet_path}" limit 1;')
    r = executor.execute_query("select count(id) as customer_count;")

    assert r.fetchall()[0].customer_count == 1500

    r = executor.execute_query(
        "select nation_name,  count(id) as customer_count order by customer_count desc;"
    )

    assert r.fetchall()[0].customer_count == 72


def test_duckdb_date_add():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(
        r"""
auto today <- date_add(current_datetime(), day, -3);
select today;
"""
    )

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)


def test_duckdb_alias():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(
        r"""
auto today <- date_add(current_datetime(), day, -3);
select today as tomorrow, today;
"""
    )

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)
    assert results[0].tomorrow.date() == datetime.now().date() - timedelta(days=3)


def test_function_parsing():
    query = """
    import function_test as game;

where @game.is_team_game('pelican') = true
select game.home_team.name, count(game.id)->game_count;"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.parse_text(query)


def test_global_aggregate_inclusion():
    """check that including a global aggregate constant in output select doesn't force changed evaluation orde"""
    query = """
    key id int;
key date date;
property <id, date>.score int;

datasource raw (
    id: id,
    date: date,
    score: score
)
grain (id,date)
query '''
select 1 as id, '2023-01-01' as date, 10 as score
union all
select 2, '2023-01-02', 20
union all
select 3, '2023-01-03', 30
union all
select 4, '2023-01-03', 40
''';

auto max_date <- max(date) by *;

"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """where date = max_date and id >2
select date, avg(score) as avg_id;"""
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0

    results = executor.execute_text(
        """where date = max_date and id >2
select max_date, date, avg(score) as avg_id;"""
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0


def test_tuple_filtering():
    query = """
    key case_number int;
property case_number.primary_type string;
property case_number.ward string;

datasource crimes (
    case_number: case_number,
    primary_type: primary_type,
    ward: ward
)
grain (case_number)
query '''
select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
union all
select 2, 'ASSAULT', 'Ward 2'
union all
select 3, 'ROBBERY', 'Ward 1'
''';

"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """select local.ward, count_distinct(local.case_number) as violent_crime_count  
where local.primary_type in ("HOMICIDE"::string, "ASSAULT"::string, "ROBBERY"::string, "AGGRAVATED ASSAULT"::string)  
having violent_crime_count > 0
order by local.ward asc
                                    ; """
    )[0].fetchall()

    assert len(results) == 2
    assert results[0].violent_crime_count == 2


def test_multiple_string_filters():
    query = """
    key case_number int;
property case_number.primary_type string;
property case_number.ward string;

datasource crimes (
    case_number: case_number,
    primary_type: primary_type,
    ward: ward
)
grain (case_number)
query '''
select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
union all
select 2, 'ASSAULT', 'Ward 2'
union all
select 3, 'ROBBERY', 'Ward 1'
''';
"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """where ( local.primary_type = "HOMICIDE"::string or local.primary_type= "ASSAULT"::string)
        select 1 as test;
                                    """
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].test == 1

    results = executor.execute_text(
        """select local.ward, count_distinct(local.case_number) as violent_crime_count  
where ward='Ward 2' and ( local.primary_type = "HOMICIDE"::string or local.primary_type= "ASSAULT"::string)
having violent_crime_count > 0
order by local.ward asc
                                    ; """
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].violent_crime_count == 1


# def test_variable_plan_generation():
#     from trilogy.hooks import DebuggingHook
#     DebuggingHook()
#     query = """
#     key case_number int;
# property case_number.primary_type string;
# property case_number.ward string;

# datasource crimes (
#     case_number: case_number,
#     primary_type: primary_type,
#     ward: ward
# )
# grain (case_number)
# query '''
# select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
# union all
# select 2, 'ASSAULT', 'Ward 2'
# union all
# select 3, 'ROBBERY', 'Ward 1'
# ''';

# """

#     executor: Executor = Dialects.DUCK_DB.default_executor(
#         environment=Environment(working_path=Path(__file__).parent)
#     )
#     executor.parse_text(query)

#     base =  executor.generate_sql(
#         """select local.ward, count_distinct(local.case_number) as violent_crime_count
# where local.primary_type in ("HOMICIDE"::string, "ASSAULT"::string, "ROBBERY"::string, "AGGRAVATED ASSAULT"::string)
# having violent_crime_count > 0
# order by local.ward asc
#                                     ; """
#     )[0]
#     comp = executor.generate_sql(
#         """
# auto homicide <- "HOMICIDE"::string;
# auto assault <- "ASSAULT"::string;
# auto robbery <- "ROBBERY"::string;

# select local.ward, count_distinct(local.case_number) as violent_crime_count
# where local.primary_type in (homicide, assault, robbery)
# having violent_crime_count > 0
# order by local.ward asc
#                                     ; """
#     )[0]

#     assert base == comp


def test_null_filtering():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', null
union all
select 4, 'store3', 244
'''
where store = 'store2';

where store = 'store2' and customer IS NULL
select 
    avg(customer_orders) -> avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_customer_orders == 1


def test_recursive():
    from trilogy.hooks.query_debugger import DebuggingHook
    DebuggingHook()
    query = """
key id int;
property id.parent int;

# traverse parent-> id until you hit a null
auto first_parent <- recurse_edge(id, parent);

datasource edges (
    id: id,
    parent: parent
)
grain (id)
query '''
select 1 as id, null as parent
union all
select 2, 1
union all
select 3, 2
union all
select 4, 3
union all 
select 5, null
union all
select 6, 5
''';

"""
    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.parse_text(query)

    assert executor.environment.concepts["first_parent"].derivation == Derivation.RECURSIVE
    sql = executor.generate_sql(
        """where
first_parent = 1    
select id;
"""
    )[-1]
    assert sql == 'fun', sql
    results = executor.execute_text(
        """where
first_parent = 1
select id;
"""
    )[0].fetchall()
    assert len(results) == 4