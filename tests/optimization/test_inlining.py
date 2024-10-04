from trilogy import Dialects


def test_safe_cases():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    raw = """
key upper_word string;
key lower_word string;
property upper_word.left string;
property lower_word.right string;
          
datasource lefts (
    raw:lower_word,
    upper(lower_word): upper_word,
    left:left 
          )
grain (upper_word)
query '''
select 'abc' raw, 'fun' "left"
union all
select 'def' raw, 'brave' "left"
          ''';
          
datasource rights (
    gaw:lower_word,
    raw( '''upper(gaw)''' ): upper_word,
    right:right
    )
grain (upper_word)
    query '''
select 'abc' gaw, 'monkey' "right"
union all 
select 'def' gaw, 'ant' "right"
          '''; 

select
    upper_word,
    count(left)->left_count,
    count(right)->right_count   
order by upper_word asc;
"""

    executor = Dialects.DUCK_DB.default_executor()
    results = executor.execute_query(raw)

    assert results.fetchall() == [("ABC", 1, 1), ("DEF", 1, 1)]


def test_raw_assignment():
    raw = """
key upper_word string;
key lower_word string;
property upper_word.left string;
property lower_word.right string;
          
datasource rights (
    raw('''haw''') : upper_word,
    raw:lower_word,
    right:right
    )
grain (upper_word)
    query '''
select 'abc' raw, 'monkey' "right"
union all 
select 'def' raw, 'ant' "right"
          '''; 

          select 1 as test;
"""

    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_query(raw)
