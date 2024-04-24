from datetime import datetime
import networkx as nx
from preql.core.env_processor import generate_graph
from preql.executor import Executor
from preql.core.models import ShowStatement
from preql.core.enums import PurposeLineage


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
    
    select total_count * usd_conversion as converted_total_count, total_count ;
    """
    )[0].fetchall()
    print(results[0])
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

    from preql.parser import parse_text

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
