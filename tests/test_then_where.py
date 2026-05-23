from trilogy import Dialects, parse
from trilogy.core.models.environment import Environment

BASE = """
key id int;
property id.cat string;
property id.amount int;
datasource rows (id: id, cat: cat, amount: amount)
grain (id)
address rows_tbl;

auto cat_total <- sum(amount) by cat;
"""


def test_then_where_preserves_ordered_stages():
    _, parsed = parse(BASE + """
where cat = 'a'
then where cat_total > 10
select id;
""")
    select = parsed[-1]
    assert len(select.where_clauses) == 2
    assert "cat" in str(select.where_clauses[0])
    assert "cat_total" in str(select.where_clauses[1])
    # combined view is the AND of every stage
    assert select.where_clause is not None
    assert "cat_total" in str(select.where_clause)


def test_then_where_after_select():
    _, parsed = parse(BASE + """
select id
where cat = 'a'
then where cat_total > 10;
""")
    assert len(parsed[-1].where_clauses) == 2


def test_then_where_three_stages():
    _, parsed = parse(BASE + """
where cat = 'a'
then where amount > 0
then where cat_total > 10
select id;
""")
    assert len(parsed[-1].where_clauses) == 3


def test_single_where_before_select():
    _, parsed = parse(BASE + "where cat = 'a' select id;")
    assert len(parsed[-1].where_clauses) == 1


def test_single_where_after_select():
    _, parsed = parse(BASE + "select id where cat = 'a';")
    assert len(parsed[-1].where_clauses) == 1


def test_then_where_render_round_trip():
    _, parsed = parse(BASE + """
where cat = 'a'
then where cat_total > 10
select id;
""")
    rendered = str(parsed[-1])
    assert "then WHERE" in rendered
    # the rendered text re-parses to the same staged shape
    _, reparsed = parse(BASE + rendered)
    assert len(reparsed[-1].where_clauses) == 2


def test_then_where_generates_aggregate_semijoin():
    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    sql = engine.generate_sql(BASE + """
where amount > 5
then where cat_total > 10
select id;
""")[-1]
    # stage 1 (amount > 5) is pushed into the aggregate scan as a semijoin
    assert "in (select" in sql, sql
