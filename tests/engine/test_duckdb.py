from datetime import datetime
from pathlib import Path

import networkx as nx

from trilogy import Dialects
from trilogy.core.enums import FunctionType, Granularity, Purpose, PurposeLineage
from trilogy.core.env_processor import generate_graph
from trilogy.core.execute_models import (
    BoundConcept,
    BoundEnvironment,
    FilterItem,
    Grain,
    LooseConceptList,
    SubselectComparison,
)
from trilogy.core.author_models import SelectStatement, ShowStatement
from trilogy.core.processing.concept_strategies_v3 import get_upstream_concepts
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_function_parent_concepts,
)
from trilogy.executor import Executor
from trilogy.parser import parse_text


def test_basic_query(duckdb_engine: Executor, expected_results):
    graph = generate_graph(duckdb_engine.environment)

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
    assert scaled_metric.purpose == Purpose.METRIC
    assert (
        duckdb_engine.environment.concepts["usd_conversion"].granularity
        == Granularity.SINGLE_ROW
    )
    parent_arg: BoundConcept = [
        x for x in scaled_metric.lineage.arguments if x.name == "total_count"
    ][0]
    assert len(parent_arg.lineage.arguments[0].grain.components) == 2
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
    assert 'fact_items."item" as "item"' in results[0]["__preql_internal_query_text"]


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
    assert z.grain == Grain(components=[x])
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
    assert half.derivation == PurposeLineage.BASIC
    assert half.granularity == Granularity.MULTI_ROW
    assert len(results) == 4


def test_array_inclusion(default_duckdb_engine: Executor):
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
const orid <- unnest(list);
const orid_2 <-unnest(list_2);
const even_orders <- filter orid where (orid % 2) = 0;
const filtered_even_orders <- filter orid_2 where orid_2 in even_orders;

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
    env = default_duckdb_engine.environment
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, FilterItem)
    assert isinstance(agg_parent.lineage.where.conditional, SubselectComparison)
    _, _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
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
    env = default_duckdb_engine.environment
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, FilterItem)
    _, _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
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

    customer_orders = default_duckdb_engine.environment.concepts["customer_orders"]
    assert set([x for x in customer_orders.keys]) == {"local.customer"}
    assert set([x for x in customer_orders.grain.components]) == {"local.customer"}

    customer_orders_2 = customer_orders.with_select_context(
        {},
        Grain(
            components=[default_duckdb_engine.environment.concepts["local.customer"]]
        ),
        default_duckdb_engine.environment,
    )
    assert set([x for x in customer_orders_2.keys]) == {"local.customer"}
    assert set([x for x in customer_orders_2.grain.components]) == {"local.customer"}

    count_by_customer = default_duckdb_engine.environment.concepts[
        "avg_customer_orders"
    ].lineage.arguments[0]
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

key constan_group_orid <- unnest([1,2,3]);
property constan_group_orid.mod_two <- constan_group_orid % 2;

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
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    default_duckdb_engine = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    test = """
const x <- 1;
const x2 <- x+1;

key orid <- unnest([1,2,3,6,10]);
property orid.mod_two <- orid % 2;

property orid.cased <-CASE WHEN mod_two = 0 THEN 1 ELSE 0 END;

auto total_mod_two <- sum(cased);

select 
    total_mod_two
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    cased = default_duckdb_engine.environment.concepts["cased"]
    total = default_duckdb_engine.environment.concepts["total_mod_two"]
    assert cased.purpose == Purpose.PROPERTY
    assert cased.keys == {"local.orid"}
    assert total.derivation == PurposeLineage.AGGREGATE
    x = resolve_function_parent_concepts(
        total, environment=default_duckdb_engine.environment
    )
    assert len(cased.concept_arguments) == 1
    assert "local.orid" in get_upstream_concepts(cased)

    # for x in total.lineage.concept_arguments:
    #     if isinstance(x, Concept) and x.purpose == Purpose.PROPERTY and x.keys:
    #         raise SyntaxError(x.keys)
    assert "local.cased" in LooseConceptList(concepts=x)
    assert "local.orid" in LooseConceptList(concepts=x)
    # function_to_concept(
    #     parent=function_to_concept()
    # )
    assert results[0] == (3,)


def test_demo_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([1,2,2,3]);

const even_x <- filter x where (x % 2) = 0;

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

const y <- x+1;

const odd_y <- filter x where (x % 2) = 0;

select 
    count(odd_y) -> odd_y_count,
    count(x) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2, 4)
    assert len(results) == 1


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
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=BoundEnvironment())

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
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=BoundEnvironment())
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

    parsed: SelectStatement = duckdb_engine.parse_text(test)[0]
    row_args = parsed.where_clause.row_arguments
    assert parsed.having_clause
    assert parsed.grain == Grain(
        components=[duckdb_engine.environment.concepts["item"]]
    )
    assert len(row_args) == 1
    # assert target.grain.components == [duckdb_engine.environment.concepts["item"]]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    # derived = parsed.local_concepts["local.all_store_count"]
    # assert isinstance(derived.lineage, AggregateWrapper)
    # assert derived.lineage.by == [duckdb_engine.environment.concepts["item"]]
    assert len(results) == 1
    assert results[0] == ("hammer", 2)


def test_duckdb_load():
    env = BoundEnvironment(working_path=Path(__file__).parent)
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
property space_all.name <- union(one_name, two_name);

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
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
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
