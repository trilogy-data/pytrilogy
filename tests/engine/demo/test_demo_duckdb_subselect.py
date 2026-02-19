from trilogy import Dialects
from trilogy.hooks.query_debugger import DebuggingHook


def test_subselect_non_correlated():
    """Non-correlated subselect: constant array output."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query("""
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

auto top3 <- subselect(val order by val desc limit 3);
select top3;
""").fetchall()
    arr = results[0].top3
    assert sorted(arr, reverse=True) == [50, 40, 30]


def test_subselect_correlated():
    """Correlated subselect: array per group with join key."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query("""
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

auto top_scores <- subselect(score where category = category order by score desc limit 2);
select
    category,
    top_scores
order by
    category asc
;
""").fetchall()
    assert len(results) == 2
    assert sorted(results[0].top_scores, reverse=True) == [30, 20]
    assert sorted(results[1].top_scores, reverse=True) == [50, 40]


def test_subselect_with_filter():
    """Subselect with WHERE filter, no correlation."""
    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    results = executor.execute_query("""
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

auto filtered <- subselect(val where val > 20 order by val asc limit 2);
select filtered;
""").fetchall()
    arr = results[0].filtered
    assert sorted(arr) == [30, 40]
