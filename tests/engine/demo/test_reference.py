from trilogy import Dialects
from trilogy.hooks.query_debugger import DebuggingHook


def test_basic_agg():
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_query(
        """const x <- unnest([1,2,2,3]);

select 
    max(x) as max_x, 
    min(x) as min_x, 
    avg(x) as avg_x, 
    count(x) as count_x;"""
    ).fetchall()

    assert results[0].max_x == 3
    assert results[0].min_x == 1
    assert results[0].avg_x == 2


def test_agg_to_grain():
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    results = executor.execute_query(
        """
key idx int;
property idx.x int;
datasource numbers(
    idx: idx,
    x: x
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
  x,
  count(x) as number_count
order by
    x asc
;"""
    ).fetchall()

    assert results[1].number_count == 2
