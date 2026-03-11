import pytest

from trilogy import Dialects
from trilogy.core.models.core import EnumType

PREQL = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 10 as sales
''';

datasource ds_b (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 20 as sales
''';
"""


def test_enum_type_parsed():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    concept = executor.environment.concepts["local.category"]
    assert isinstance(concept.datatype, EnumType)
    assert set(concept.datatype.values) == {"A", "B"}


def test_enum_comparison_invalid():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    with pytest.raises(Exception):
        executor.execute_text("select sales where category = 'C';")


def test_enum_union_injection():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select sum(sales) as total_sales;")[-1].fetchall()
    assert len(results) == 1
    assert results[0].total_sales == 30


def test_enum_union_by_category():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select category, sales order by category asc;")[
        -1
    ].fetchall()
    assert len(results) == 2
    assert results[0].category == "A"
    assert results[0].sales == 10
    assert results[1].category == "B"
    assert results[1].sales == 20
