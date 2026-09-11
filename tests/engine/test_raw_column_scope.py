"""A `raw()` column binding is evaluated in its datasource's own scope.

The raw text renders verbatim. When the datasource was inlined into a consumer
that joins another table, the text landed unqualified in the consumer's
scope: ambiguous when the other table shares the column name, and a literal
(`raw('''true''')`) read as true for every row of the other side. A datasource
with a raw column now keeps its own CTE, so the text only ever sees its own
table.
"""

from trilogy import Dialects

MODEL = """
key product_id int;
property product_id.name string;
property product_id.featured bool;

datasource products (
    id: product_id,
    name: name,
)
grain (product_id)
query '''
select 1 as id, 'a' as name union all
select 2 as id, 'b' as name union all
select 3 as id, 'c' as name
''';

# A marker table: only featured products have a row.
datasource featured (
    id: ~product_id,
    raw('''true'''): featured,
)
grain (product_id)
query '''
select 1 as id
''';

key sale_id int;
property sale_id.amount int;
property sale_id.even_sale bool;

datasource sales (
    id: sale_id,
    product_id: ~product_id,
    amount: amount,
    raw('''"id" % 2 = 0'''): even_sale,
)
grain (sale_id)
query '''
select 1 as id, 1 as product_id, 10 as amount union all
select 2 as id, 1 as product_id, 20 as amount union all
select 3 as id, 2 as product_id, 30 as amount
''';
"""


def _rows(query: str) -> dict:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    return {row[0]: row[1] for row in executor.execute_text(query)[-1].fetchall()}


def test_raw_literal_is_scoped_to_its_marker_table():
    rows = _rows("select name, featured;")
    assert rows["a"] is True
    assert rows["b"] is None
    assert rows["c"] is None


def test_raw_column_reference_is_not_ambiguous_under_a_join():
    # `"id"` exists on both tables; the raw text must bind to sales.id.
    rows = _rows("select name, sum(amount ? even_sale) as even_amount;")
    assert rows["a"] == 20
    assert rows["b"] is None
