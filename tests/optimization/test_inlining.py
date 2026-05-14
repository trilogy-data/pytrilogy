from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource
from trilogy.core.optimizations.inline_datasource import InlineDatasource


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


def test_inline_datasource_respects_cutoff(test_environment):
    env = test_environment.materialize_for_select()
    datasource = env.datasources["products"]
    product_id = env.concepts["product_id"]
    parent = CTE.from_datasource(datasource)
    parent.name = "parent_products"
    child = CTE(
        name="child",
        source=QueryDatasource(
            input_concepts=[product_id],
            output_concepts=[product_id],
            datasources=[parent.source],
            grain=BuildGrain(),
            joins=[],
            source_map={product_id.address: {parent.source}},
        ),
        output_columns=[product_id],
        parent_ctes=[parent],
        grain=BuildGrain(),
        source_map={product_id.address: [parent.name]},
        existence_source_map={},
    )

    original = CONFIG.optimizations.constant_inline_cutoff
    CONFIG.optimizations.constant_inline_cutoff = 0
    try:
        rule = InlineDatasource()
        assert rule.optimize(child, {"parent_products": [child]})[0] is True
        assert rule.optimize(child, {"parent_products": [child]})[0] is False
        assert child.parent_ctes == [parent]
    finally:
        CONFIG.optimizations.constant_inline_cutoff = original


def test_select_literal_is_rendered_in_projection():
    raw = """
key x int;

datasource nums (
    x:x
)
grain (x)
query '''select 1 as x union all select 2 as x''';

where x = 1
select
    x,
    'abc' as label;
"""
    executor = Dialects.DUCK_DB.default_executor()
    query = executor.generate_sql(raw)[-1]

    assert query.count(":label") == 1
    assert 'SELECT\n    :label as "label"\n)' not in query
    assert '    :label as "label"' in query
