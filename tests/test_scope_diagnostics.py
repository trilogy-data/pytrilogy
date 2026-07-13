"""Derived-value scope diagnostics (docs/SPEC_query_derived_value_scopes.md).

Extraction runs on the post-normalization build lineage inside process_query;
these tests parse against a duckdb executor and read the typed records off the
resulting ProcessedQuery.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.scope_diagnostics import (
    DerivedValueScope,
    render_derived_value_scopes,
)
from trilogy.core.statements.execute import ProcessedQuery

MODEL = """
key sale_id int;
property sale_id.year int;
property sale_id.state string;
property sale_id.customer_id int;
property sale_id.amount float;
property sale_id.is_web bool;

datasource sales (
    id: sale_id,
    yr: year,
    st: state,
    cust: customer_id,
    amt: amount,
    web: is_web,
)
grain (sale_id)
address sales;
"""


def _scopes(body: str) -> list[DerivedValueScope]:
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MODEL)
    queries = [q for q in exec.parse_text(body) if isinstance(q, ProcessedQuery)]
    assert queries, "body produced no ProcessedQuery"
    return queries[-1].derived_value_scopes


def _by_name(scopes: list[DerivedValueScope], name: str) -> DerivedValueScope:
    match = [s for s in scopes if s.name == name]
    assert match, f"no scope named {name}; have {[s.name for s in scopes]}"
    return match[0]


def test_keyed_aggregate_with_where():
    scopes = _scopes("""
select
    state,
    sum(amount) as total_amount
where year = 2002;
""")
    scope = _by_name(scopes, "total_amount")
    assert scope.kind == "aggregate"
    assert scope.expression == "sum(amount)"
    assert scope.input_filters == ["year = 2002"]
    assert scope.group_by == ["state"]
    assert scope.output_filters == []


def test_scalar_aggregate_groups_by_star():
    scopes = _scopes("select sum(amount) by * as grand_total;")
    scope = _by_name(scopes, "grand_total")
    assert scope.group_by == ["*"]
    assert scope.input_filters == []


def test_filtered_argument_is_input_filter_not_statement_where():
    scopes = _scopes("""
select
    state,
    sum(amount ? is_web) as web_amount
where year = 2002;
""")
    scope = _by_name(scopes, "web_amount")
    assert "year = 2002" in scope.input_filters
    assert any("is_web" in f for f in scope.input_filters)


def test_nested_average_reports_inner_grain_and_rowset_filter():
    scopes = _scopes("""
with customer_totals as
select
    customer_id,
    state,
    sum(amount) as customer_total
where year = 2002;

select
    customer_totals.state,
    avg(customer_totals.customer_total) as state_average;
""")
    outer = _by_name(scopes, "state_average")
    assert outer.input_values == ["customer_totals.customer_total"]
    assert outer.input_filters == ["year = 2002"]
    assert set(outer.input_grain) == {"customer_id", "state"}
    assert outer.group_by == ["customer_totals.state"]
    inner = _by_name(scopes, "customer_totals.customer_total")
    assert inner.expression == "sum(amount)"
    assert inner.input_filters == ["year = 2002"]
    assert set(inner.group_by) == {"customer_id", "state"}


def test_outer_filter_does_not_leak_into_rowset_scope():
    scopes = _scopes("""
with customer_totals as
select
    customer_id,
    state,
    sum(amount) as customer_total
where year = 2002;

select
    customer_totals.state,
    avg(customer_totals.customer_total) as state_average
where customer_totals.state = 'GA';
""")
    # The outer restriction filters rows entering the average...
    outer = _by_name(scopes, "state_average")
    assert "customer_totals.state = 'GA'" in outer.input_filters
    # ...but never the already-computed per-customer totals.
    inner = _by_name(scopes, "customer_totals.customer_total")
    assert inner.input_filters == ["year = 2002"]


def test_where_dual_scope_emits_two_entries():
    scopes = _scopes("""
select
    state,
    count(sale_id) as sale_count
where count(sale_id) > 10;
""")
    filter_scope = _by_name(scopes, "sale_count (filter scope)")
    output_scope = _by_name(scopes, "sale_count (output scope)")
    # population gate: unfiltered inputs, condition applies to its output
    assert filter_scope.input_filters == []
    assert filter_scope.output_filters == ["count(sale_id) > 10"]
    # select output: recomputes over the WHERE-filtered rows
    assert output_scope.input_filters == ["count(sale_id) > 10"]


def test_window_partition_order_and_where():
    scopes = _scopes("""
select
    state,
    year,
    sum(amount) as total_amount,
    rank total_amount over state order by total_amount desc as amount_rank
where year = 2002;
""")
    rank = _by_name(scopes, "amount_rank")
    assert rank.kind == "window"
    assert rank.partition_by == ["state"]
    assert [o.render() for o in rank.order_by] == ["total_amount desc"]
    assert rank.input_values == ["total_amount"]
    assert rank.input_filters == ["year = 2002"]
    assert set(rank.input_grain) == {"state", "year"}


def test_lag_window_reports_offset_and_input_grain():
    scopes = _scopes("""
select
    year,
    sum(amount) as yearly_total,
    lag 1 yearly_total order by year asc as prior_year_total;
""")
    lag = _by_name(scopes, "prior_year_total")
    assert lag.expression == "lag(yearly_total, 1)"
    assert lag.input_values == ["yearly_total"]
    assert lag.input_grain == ["year"]
    assert [o.render() for o in lag.order_by] == ["year asc"]


def test_having_is_output_filter():
    scopes = _scopes("""
select
    state,
    sum(amount) as total_amount
having total_amount > 100;
""")
    scope = _by_name(scopes, "total_amount")
    assert scope.input_filters == []
    assert scope.output_filters == ["total_amount > 100"]


def test_hidden_having_aggregate_is_reported():
    scopes = _scopes("""
select
    state
having sum(amount) > 100;
""")
    assert any(s.expression == "sum(amount)" for s in scopes)


def test_multiple_statements_have_independent_scopes():
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MODEL)
    queries = [q for q in exec.parse_text("""
select state, sum(amount) as total_amount where year = 2001;
select year, count(sale_id) as sale_count;
""") if isinstance(q, ProcessedQuery)]
    assert len(queries) == 2
    first = _by_name(queries[0].derived_value_scopes, "total_amount")
    second = _by_name(queries[1].derived_value_scopes, "sale_count")
    assert first.input_filters == ["year = 2001"]
    assert second.input_filters == []
    assert second.group_by == ["year"]


def test_no_derived_values_yields_empty_report():
    scopes = _scopes("select state, year;")
    assert scopes == []
    assert render_derived_value_scopes(scopes) == ""


def test_to_dict_omits_empty_fields_kind_and_ordering():
    scopes = _scopes("select state, sum(amount) as total_amount;")
    payload = _by_name(scopes, "total_amount").to_dict()
    assert payload == {
        "name": "total_amount",
        "expression": "sum(amount)",
        "group_by": ["state"],
    }
    windowed = _scopes("""
select
    year,
    sum(amount) as yearly_total,
    lag 1 yearly_total order by year asc as prior_year_total;
""")
    payload = _by_name(windowed, "prior_year_total").to_dict()
    assert "kind" not in payload
    assert "order_by" not in payload
    assert payload["input_values"] == ["yearly_total"]


def test_render_block_shape():
    scopes = _scopes("""
select
    state,
    sum(amount) as total_amount
where year = 2002;
""")
    block = render_derived_value_scopes(scopes)
    lines = block.splitlines()
    assert lines[0] == "Derived value scopes"
    assert "total_amount" in lines
    assert "  input filters: year = 2002" in lines
    assert "  group by: state" in lines
    assert not any(line.startswith("  kind:") for line in lines)


def test_render_block_omits_empty_input_filters():
    scopes = _scopes("select state, sum(amount) as total_amount;")
    block = render_derived_value_scopes(scopes)
    assert "input filters" not in block


def test_generated_sql_unchanged_by_diagnostics():
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MODEL)
    body = "select state, sum(amount) as total_amount where year = 2002;"
    sql = exec.generate_sql(body)
    assert sql
    assert "wscope" not in "\n".join(sql)
