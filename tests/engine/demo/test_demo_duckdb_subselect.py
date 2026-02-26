from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.hooks.query_debugger import DebuggingHook


def test_subselect_non_correlated():
    """Non-correlated subselect: constant array output."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query(
        """
key id int;
property id.val int;
auto const <- unnest([1,2]);
datasource nums(
    id: id,
    val: val
)
grain (id)
query '''
select 1 id, 10 val
union all select 2, 20
union all select 3, 30
union all select 4, 40
union all select 5, 50
''';

auto top3 <- subselect(val order by val desc limit 3);
select const,top3;
"""
    ).fetchall()
    arr = results[0].top3
    assert sorted(arr, reverse=True) == [50, 40, 30]


def test_subselect_correlated():
    """Correlated subselect: array per group with join key."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query(
        """
key id int;
property id.category string;
property id.score int;
datasource items(
    id: id,
    category: category,
    score: score
)
grain (id)
query '''
select 1 id, 'a' category, 10 score
union all select 2, 'a', 20
union all select 3, 'a', 30
union all select 4, 'b', 40
union all select 5, 'b', 50
''';

def table top_category_scores() -> select score where category order by score desc limit 2;
select
    category,
    @top_category_scores() as top_scores
order by
    category asc
;
"""
    ).fetchall()
    assert len(results) == 2
    assert sorted(results[0].top_scores, reverse=True) == [30, 20]
    assert sorted(results[1].top_scores, reverse=True) == [50, 40]


def test_subselect_with_filter():
    """Subselect with WHERE filter, no correlation."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query(
        """
key id int;
property id.val int;
datasource nums(
    id: id,
    val: val
)
grain (id)
query '''
select 1 id, 10 val
union all select 2, 20
union all select 3, 30
union all select 4, 40
union all select 5, 50
''';

def table filtered() -> select val where val > 20 order by val asc limit 2;
select @filtered() as filtered;
"""
    ).fetchall()
    arr = results[0].filtered
    assert sorted(arr) == [30, 40]


def test_subselect_closest_warehouse():
    """Correlated subselect across unconnected datasources."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query(
        """
key customer_id int;
property customer_id.customer_name string;
property customer_id.customer_lat float;
property customer_id.customer_lon float;

key warehouse_id int;
property warehouse_id.warehouse_name string;
property warehouse_id.warehouse_lat float;
property warehouse_id.warehouse_lon float;

datasource customers(
    customer_id: customer_id,
    customer_name: customer_name,
    customer_lat: customer_lat,
    customer_lon: customer_lon
)
grain (customer_id)
query '''
select 1 as customer_id, 'alice' as customer_name, 40.0 as customer_lat, -74.0 as customer_lon
union all select 2, 'bob', 34.0, -118.0
''';

datasource warehouses(
    warehouse_id: warehouse_id,
    warehouse_name: warehouse_name,
    warehouse_lat: warehouse_lat,
    warehouse_lon: warehouse_lon
)
grain (warehouse_id)
query '''
select 10 as warehouse_id, 'east' as warehouse_name, 40.1 as warehouse_lat, -74.1 as warehouse_lon
union all select 20, 'west', 34.2, -118.2
''';

def table close_warehouses(lat, long) -> select warehouse_name order by sqrt(
        (lat - warehouse_lat) * (lat - warehouse_lat)
        + (long - warehouse_lon) * (long - warehouse_lon)
    ) asc limit 1;

select
    customer_name,
    @close_warehouses(customer_lat, customer_lon) as nearest_warehouses
order by
    customer_name asc
;
"""
    ).fetchall()
    by_customer = {row.customer_name: row.nearest_warehouses for row in results}
    assert by_customer == {"alice": ["east"], "bob": ["west"]}


def test_subselect_with_merge():
    """Subselect result merged with data from a separate datasource."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query(
        """
key item_id int;
property item_id.category string;
property item_id.score int;
datasource items(
    item_id: item_id,
    category: category,
    score: score
)
grain (item_id)
query '''
select 1 item_id, 'a' category, 10 score
union all select 2, 'a', 20
union all select 3, 'b', 30
''';

key label_id int;
property label_id.label_cat string;
property label_id.label_text string;
datasource labels(
    label_id: label_id,
    label_cat: label_cat,
    label_text: label_text
)
grain (label_id)
query '''
select 1 label_id, 'a' label_cat, 'Alpha' label_text
union all select 2, 'b', 'Beta'
''';

def table top_scores() -> select score where category order by score desc limit 2;

SELECT
    category,
    @top_scores() as top_scores
MERGE
SELECT
    label_cat,
    label_text
ALIGN merged_cat: category, label_cat
;
"""
    ).fetchall()
    by_cat = {
        row.merged_cat: (sorted(row.top_scores, reverse=True), row.label_text)
        for row in results
    }
    assert by_cat["a"] == ([20, 10], "Alpha")
    assert by_cat["b"] == ([30], "Beta")


def test_subselect_imported_namespace():
    """Subselect function imported from another namespace."""
    env = Environment(working_path=Path(__file__).parent)
    executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook()]
    )
    results = executor.execute_query(
        """
import subselect_helpers as sh;

select
    sh.category,
    @sh.top_scores() as top_scores
order by
    sh.category asc
;
"""
    ).fetchall()
    assert len(results) == 2
    assert sorted(results[0].top_scores, reverse=True) == [30, 20]
    assert sorted(results[1].top_scores, reverse=True) == [50, 40]
