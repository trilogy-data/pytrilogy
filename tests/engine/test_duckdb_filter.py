from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.build import BuildFilterItem, BuildSubselectComparison
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_function_parent_concepts,
)
from trilogy.core.processing.utility import get_disconnected_components
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook


def test_array_filtering():
    DebuggingHook()
    engine = Dialects.DUCK_DB.default_executor()
    test = """
key id int;

datasource numbers (
    id
)
grain (id)
query '''
select 1 as id union all select 2 union all select 3 union all select 4 union all select 5 union all select 6
'''
;
where id in [1,2,3]
select id
;
    """
    results = engine.execute_text(test)[0].fetchall()
    assert len(results) == 3


def test_array_inclusion(default_duckdb_engine: Executor):
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


def test_demo_filter():
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
    test = """const x <- unnest([0, 1,2,2,3]);

select
    count(x ? x ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)


def test_nullif_filter():
    test = """const x <- unnest([0, 1,2,2,3]);

select
    count(x ? nullif(x, 2) ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2,)


def test_filter_promotion(duckdb_engine: Executor):
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

    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0] == ("hammer", 2)


def test_filter_scalar_aggregate_not_restricted_by_staging():
    """A by-aggregate used as a filter scalar must compute over its own
    dimension scope, not be silently restricted by a staging datasource whose
    `complete where` matches the outer filter.

    Covers four permutations against the same items/sales model:
      1. Baseline (no staging): filter-scalar avg(price) by category ranges
         over the full items table.
      2. With staging whose non_partial_for matches the outer WHERE: the filter
         scalar must still range over the full items table (the bug).
      3. Same aggregate used BOTH as SELECT output AND as filter scalar: the
         filter-scalar sourcing is unfiltered, the SELECT-output sourcing is
         filtered.
      4. SELECT-level aggregate with matching outer WHERE: the filter *does*
         apply (SELECT-level aggregates are not filter scalars).
    """
    DebuggingHook()

    model = """
key item_id int;
property item_id.category string;
property item_id.price int;

key sale_id int;
property sale_id.sale_year int;
property sale_id.sale_item_id int;
property sale_id.sale_item_price int;

datasource items (
    id: item_id,
    category: category,
    price: price
) grain (item_id) address items_tbl;

datasource sales (
    id: sale_id,
    year: sale_year,
    item_id: sale_item_id,
    item_price: sale_item_price
) grain (sale_id) address sales_tbl;

merge sale_item_id into ~item_id;
"""

    staging = """
datasource staged_sales (
    sale_id: ~sale_id,
    sale_year: ~sale_year,
    item_id: ~item_id,
    category: ~category,
    sale_item_price: ~sale_item_price
) grain (sale_id) complete where sale_year = 2023
address staged_sales_tbl;
"""

    def build_executor(include_staging: bool) -> Executor:
        executor = Dialects.DUCK_DB.default_executor(environment=Environment())
        executor.execute_raw_sql(
            "CREATE TABLE items_tbl (id INT, category VARCHAR, price INT)"
        )
        executor.execute_raw_sql(
            "INSERT INTO items_tbl VALUES "
            "(1,'A',10),(2,'A',20),(3,'A',100),(4,'B',30),(5,'B',40)"
        )
        executor.execute_raw_sql(
            "CREATE TABLE sales_tbl (id INT, year INT, item_id INT, item_price INT)"
        )
        executor.execute_raw_sql(
            "INSERT INTO sales_tbl VALUES "
            "(1,2023,1,10),(2,2023,2,20),(3,2023,4,30),"
            "(4,2023,5,40),(5,2022,3,100)"
        )
        executor.parse_text(model)
        if include_staging:
            executor.execute_raw_sql(
                "CREATE TABLE staged_sales_tbl AS "
                "SELECT s.id AS sale_id, s.year AS sale_year, "
                "i.id AS item_id, i.category AS category, "
                "s.item_price AS sale_item_price "
                "FROM sales_tbl s JOIN items_tbl i ON s.item_id = i.id "
                "WHERE s.year = 2023"
            )
            executor.parse_text(staging)
        return executor

    # Thresholds for avg(price) by category over the full items table:
    #   A -> (10 + 20 + 100) / 3 = 43.33
    #   B -> (30 + 40) / 2 = 35.0
    # Only sale 4 (B, price 40) exceeds its threshold => count = 1.
    # If the aggregate is incorrectly restricted to 2023 sales only, item 3
    # (price 100) drops out and A's threshold falls to 15, pulling in sale 2
    # (A, price 20) as well => count = 2 (bug).
    filter_scalar_query = """
where sale_year = 2023
  and sale_item_price > avg(price) by category
select count(sale_id) as sale_count;
"""

    def gen_sql(executor: Executor, query: str) -> str:
        return executor.generate_sql(executor.parse_text(query)[-1])[0]

    # 1. Baseline: no staging in scope. Must source from items_tbl + sales_tbl.
    baseline = build_executor(include_staging=False)
    sql = gen_sql(baseline, filter_scalar_query)
    assert "items_tbl" in sql
    assert "sales_tbl" in sql
    assert "staged_sales_tbl" not in sql
    result = baseline.execute_text(filter_scalar_query)[0].fetchall()
    assert result[0].sale_count == 1

    # 2. With staging in scope: aggregate must source items_tbl (full dim),
    # not staged_sales_tbl (which would restrict it to year=2023).
    staged = build_executor(include_staging=True)
    sql = gen_sql(staged, filter_scalar_query)
    assert "items_tbl" in sql, sql
    assert "staged_sales_tbl" not in sql, sql
    result = staged.execute_text(filter_scalar_query)[0].fetchall()
    assert result[0].sale_count == 1

    # 3. Same by-aggregate used as SELECT output AND filter scalar.
    # Threshold for B = 35; sale 4 is the only one above that threshold.
    mixed_query = """
where sale_item_price > avg(price) by category
  and category = 'B'
select category, count(sale_id) as sale_count
order by category asc;
"""
    sql = gen_sql(staged, mixed_query)
    assert "items_tbl" in sql, sql
    assert "staged_sales_tbl" not in sql, sql
    result = staged.execute_text(mixed_query)[0].fetchall()
    assert len(result) == 1
    assert result[0].category == "B"
    assert result[0].sale_count == 1

    # 4. SELECT-level aggregate with outer WHERE — filter DOES apply, so the
    # planner is free to source from the pre-filtered staging datasource.
    select_level_query = """
where sale_year = 2023
select count(sale_id) as sale_count;
"""
    sql = gen_sql(staged, select_level_query)
    assert "staged_sales_tbl" in sql, sql
    result = staged.execute_text(select_level_query)[0].fetchall()
    assert result[0].sale_count == 4


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
    DebuggingHook()
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


def test_filter_constant_unrelated():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;

"""
    default_duckdb_engine.parse_text(test)
    env = default_duckdb_engine.environment
    graph_count, graphs = get_disconnected_components(
        concept_map={
            "example": [env.concepts["value"]],
            "other": [env.concepts["dim"], env.concepts["x"]],
        }
    )
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
