from datetime import datetime
import networkx as nx
from trilogy.core.env_processor import generate_graph
from trilogy.executor import Executor
from trilogy.core.models import ShowStatement, Concept, Grain
from trilogy.core.enums import Purpose, Granularity, PurposeLineage, FunctionType
from trilogy.parser import parse_text
from trilogy.core.processing.concept_strategies_v3 import get_upstream_concepts
from trilogy.core.processing.node_generators.group_node import (
    resolve_function_parent_concepts,
)

from trilogy import Dialects

from trilogy.core.models import LooseConceptList


def test_basic_query(duckdb_engine: Executor, expected_results):
    graph = generate_graph(duckdb_engine.environment)

    list(nx.neighbors(graph, "c~local.count@Grain<local.item,local.store_id>"))
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_concept_derivation(duckdb_engine: Executor):
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
        results = duckdb_engine.execute_text(
            f""" 


        select test.{property};
        
        """
        )[0].fetchall()
        assert results[0][0] == check


def test_render_query(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results


def test_aggregate_at_grain(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select avg_count_per_product;""")[
        0
    ].fetchall()
    assert results[0].avg_count_per_product == expected_results["avg_count_per_product"]


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
    parent_arg: Concept = [
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
    assert (
        'local_fact_items."item" as "item"' in results[0]["__preql_internal_query_text"]
    )


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
  
  select 
    date_add(today, day, 1)->tomorrow,
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
    assert str(z) == "local.z<local.x>"
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
    assert str(z) == "local.z<local.x>"
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3


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

select 
    orid,
    ((orid+17)/2) -> half_orid,
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    listc = default_duckdb_engine.environment.concepts["list"]
    assert listc.purpose == Purpose.CONSTANT
    orid = default_duckdb_engine.environment.concepts["orid"]
    half = default_duckdb_engine.environment.concepts["half_orid"]
    assert orid.address in [x.address for x in half.concept_arguments]
    assert set([x.address for x in half.keys]) == {
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

def test_array_inclusion_aggregate(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook
    from trilogy.core.models import FilterItem, SubselectComparison
    from trilogy.core.processing.node_generators.common import (
        resolve_filter_parent_concepts,
        resolve_function_parent_concepts
    )

    default_duckdb_engine.hooks = [DebuggingHook()]
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where orid_2 in even_orders;

select 
    count(filtered_even_orders)->f_ord_count
;
    """
    parsed = default_duckdb_engine.parse_text(test)[-1]
    env = default_duckdb_engine.environment
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, FilterItem)
    assert isinstance(agg_parent.lineage.where.conditional, SubselectComparison)
    base, row, existence = resolve_filter_parent_concepts(agg_parent)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3


    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where 1=1 and orid_2 in even_orders;

select 
    count(filtered_even_orders)->f_ord_count
;
    """
    parsed = default_duckdb_engine.parse_text(test)[-1]
    env = default_duckdb_engine.environment
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, FilterItem)
    base, row, existence = resolve_filter_parent_concepts(agg_parent)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3


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

select 
    avg(customer_orders) -> avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    customer_orders = default_duckdb_engine.environment.concepts["customer_orders"]
    assert set([x.address for x in customer_orders.keys]) == {"local.customer"}
    assert set([x.address for x in customer_orders.grain.components]) == {
        "local.customer"
    }

    customer_orders_2 = customer_orders.with_select_grain(Grain())
    assert set([x.address for x in customer_orders_2.keys]) == {"local.customer"}
    assert set([x.address for x in customer_orders_2.grain.components]) == {
        "local.customer"
    }

    count_by_customer = default_duckdb_engine.environment.concepts[
        "avg_customer_orders"
    ].lineage.arguments[0]
    # assert isinstance(count_by_customer, AggregateWrapper)
    assert set([x.address for x in count_by_customer.keys]) == {"local.customer"}
    assert set([x.address for x in count_by_customer.grain.components]) == {
        "local.customer"
    }
    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 1.33


def test_constant_group(default_duckdb_engine: Executor):
    test = """
const x <- 1;
const x2 <- x+1;

key orid <- unnest([1,2,3]);
property orid.mod_two <- orid % 2;

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

    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    test = """
const x <- 1;
const x2 <- x+1;

key orid <- unnest([1,2,3,6,10]);
property orid.mod_two <- orid % 2;

property orid.cased <-CASE WHEN mod_two = 0 THEN 1 ELSE 0 END;

select 
    SUM(cased) -> total_mod_two
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    cased = default_duckdb_engine.environment.concepts["cased"]
    total = default_duckdb_engine.environment.concepts["total_mod_two"]
    assert cased.purpose == Purpose.PROPERTY
    assert LooseConceptList(concepts=cased.keys) == LooseConceptList(
        concepts=[default_duckdb_engine.environment.concepts["orid"]]
    )

    assert total.derivation == PurposeLineage.AGGREGATE
    x = resolve_function_parent_concepts(total)
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
where (x_squared %2) = 0;

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
where x_squared %2  = 0;

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
