from os.path import dirname

from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.parser import parse

QUERY = """
import recursion_concepts.sales_a as a;
import recursion_concepts.sales_b as b;

merge a.sold_date.date_sk into ~b.sold_date.date_sk;

select
    a.sold_date.week_seq,
    sum(a.ext_sales_price ? a.sold_date.dow=0)
      + sum(b.ext_sales_price ? b.sold_date.dow=0)
      by a.sold_date.week_seq, a.sold_date.dow as combined
limit 20;
"""


def test_cross_model_additive_multikey_grain() -> None:
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    select: SelectStatement = parsed[-1]
    combined = select.local_concepts["local.combined"]
    # The combination's grain is the union of its aggregates' `by` keys, not the
    # surrogate primary key of either source datasource.
    assert combined.grain.components == {"a.sold_date.week_seq", "a.sold_date.dow"}
    # A selected aggregate dedupes to its own values, so it is a grain component.
    assert "local.combined" in select.grain.components
    # Must build without the spurious-grain recursion this query used to hit.
    process_query(statement=select, environment=env)
