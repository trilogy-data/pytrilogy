from datetime import datetime
import networkx as nx
from preql.core.env_processor import generate_graph
from preql.executor import Executor
from preql.core.models import ShowStatement, Concept, Grain
from preql.core.enums import Purpose, Granularity, PurposeLineage, FunctionType
from preql.parser import parse_text


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
    from preql.hooks.query_debugger import DebuggingHook

    test = """
const list <- [1,2,2,3];
const orid <- unnest(list);

select 
    orid,
    ((orid+17)/2) -> half_orid,

    
  ;
    """
    default_duckdb_engine.hooks = [DebuggingHook()]
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
