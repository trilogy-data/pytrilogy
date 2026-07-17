"""Derived-value scope diagnostics (docs/SPEC_query_derived_value_scopes.md).

Extraction runs on the post-normalization build lineage inside process_query;
these tests parse against a duckdb executor and read the typed records off the
resulting ProcessedQuery.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.scope_diagnostics import (
    DerivedValueScope,
    derived_value_warnings,
    render_derived_value_scopes,
)
from trilogy.core.statements.execute import ProcessedQuery

MODEL = """
key sale_id int;
property sale_id.year int;
property sale_id.state string;
property sale_id.order_id int;
property sale_id.customer_id int;
property sale_id.amount float;
property sale_id.is_web bool;
property sale_id.is_returned bool;

datasource sales (
    id: sale_id,
    yr: year,
    st: state,
    ord: order_id,
    cust: customer_id,
    amt: amount,
    web: is_web,
    ret: is_returned,
)
grain (sale_id)
address sales;
"""


# Two/three fact tables whose customer keys can be joined into one scoped-join
# equivalence group. Property names are globally distinct so no import
# namespacing is needed.
MULTI_FACT_MODEL = """
key ss_id int;
property ss_id.ss_customer int;
property ss_id.ss_return_customer int;
property ss_id.ss_item int;
property ss_id.ss_year int;
property ss_id.ss_qty int;
datasource store_sales (
    id: ss_id, cust: ss_customer, rcust: ss_return_customer,
    itm: ss_item, yr: ss_year, qty: ss_qty,
) grain (ss_id) address store_sales;

key cs_id int;
property cs_id.cs_billing_customer int;
property cs_id.cs_item int;
property cs_id.cs_year int;
datasource catalog_sales (
    id: cs_id, bcust: cs_billing_customer, itm: cs_item, yr: cs_year,
) grain (cs_id) address catalog_sales;

key ws_id int;
property ws_id.ws_customer int;
property ws_id.ws_year int;
datasource web_sales (
    id: ws_id, cust: ws_customer, yr: ws_year,
) grain (ws_id) address web_sales;
"""


def _scopes(body: str) -> list[DerivedValueScope]:
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MODEL)
    queries = [q for q in exec.parse_text(body) if isinstance(q, ProcessedQuery)]
    assert queries, "body produced no ProcessedQuery"
    return queries[-1].derived_value_scopes


def _multi_scopes(body: str) -> list[DerivedValueScope]:
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MULTI_FACT_MODEL)
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
    assert scope.input_row_filters == ["year = 2002"]
    assert scope.group_by == ["state"]
    assert scope.output_row_filters == []


def test_scalar_aggregate_groups_by_star():
    scopes = _scopes("select sum(amount) by * as grand_total;")
    scope = _by_name(scopes, "grand_total")
    assert scope.group_by == ["*"]
    assert scope.input_row_filters == []


def test_filtered_argument_is_input_filter_not_statement_where():
    scopes = _scopes("""
select
    state,
    sum(amount ? is_web) as web_amount
where year = 2002;
""")
    scope = _by_name(scopes, "web_amount")
    assert "year = 2002" in scope.input_row_filters
    assert any("is_web" in f for f in scope.input_row_filters)


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
    assert outer.input_row_filters == ["year = 2002"]
    assert set(outer.input_grain) == {"customer_id", "state"}
    assert outer.group_by == ["customer_totals.state"]
    inner = _by_name(scopes, "customer_totals.customer_total")
    assert inner.expression == "sum(amount)"
    assert inner.input_row_filters == ["year = 2002"]
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
    assert "customer_totals.state = 'GA'" in outer.input_row_filters
    # ...but never the already-computed per-customer totals.
    inner = _by_name(scopes, "customer_totals.customer_total")
    assert inner.input_row_filters == ["year = 2002"]


def test_where_dual_scope_emits_two_entries_with_roles():
    scopes = _scopes("""
select
    state,
    count(sale_id) as sale_count
where count(sale_id) > 10 and year = 2001;
""")
    gate = _by_name(scopes, "sale_count — used by WHERE comparison")
    output = _by_name(scopes, "sale_count — selected output after row admission")
    assert gate.role == "where_gate"
    assert output.role == "selected_output"
    # population gate: unfiltered inputs, condition applies to its output
    assert gate.input_row_filters == []
    assert gate.output_row_filters == ["count(sale_id) > 10"]
    # select output: row filters and the computed-value admission gate are
    # reported separately, never mixed
    assert output.input_row_filters == ["year = 2001"]
    assert output.admitted_by == ["count(sale_id) > 10"]
    # schema-stable JSON: empty input_row_filters serialize rather than vanish
    gate_payload = gate.to_dict()
    assert gate_payload["input_row_filters"] == []
    assert gate_payload["role"] == "where_gate"
    # unrestricted population renders loudly
    block = render_derived_value_scopes(scopes)
    assert "input row filters: NONE — unrestricted population" in block
    assert "admitted by: count(sale_id) > 10" in block


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
    assert rank.input_row_filters == ["year = 2002"]
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
    assert scope.input_row_filters == []
    assert scope.output_row_filters == ["total_amount > 100"]


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
    assert first.input_row_filters == ["year = 2001"]
    assert second.input_row_filters == []
    assert second.group_by == ["year"]


def test_no_derived_values_yields_empty_report():
    scopes = _scopes("select state, year;")
    assert scopes == []
    assert render_derived_value_scopes(scopes) == ""


def test_to_dict_schema_stable_without_kind_or_ordering():
    scopes = _scopes("select state, sum(amount) as total_amount;")
    payload = _by_name(scopes, "total_amount").to_dict()
    assert payload == {
        "name": "total_amount",
        "expression": "sum(amount)",
        "role": "selected_output",
        "input_row_filters": [],
        "group_by": ["state"],
        "output_row_filters": [],
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
    assert "  input row filters: year = 2002" in lines
    assert "  group by: state" in lines
    assert not any(line.startswith("  kind:") for line in lines)


def test_render_block_flags_unrestricted_population():
    scopes = _scopes("select state, sum(amount) as total_amount;")
    block = render_derived_value_scopes(scopes)
    assert "input row filters: NONE — unrestricted population" in block


def test_membership_rowset_eligibility_aggregates_reported():
    """q95 shape: aggregates gating a membership rowset's HAVING must be
    reported even though the outer statement consumes only a plain key."""
    scopes = _scopes("""
with eligible_orders as
select
    order_id,
    count(customer_id) as buyer_count,
    bool_or(is_returned) as has_return
having
    buyer_count > 1
    and has_return = true;

select
    count(order_id ? order_id in eligible_orders.order_id) as eligible_order_count
where year = 2000;
""")
    buyer = _by_name(scopes, "eligible_orders.buyer_count")
    returned = _by_name(scopes, "eligible_orders.has_return")
    for scope in (buyer, returned):
        assert scope.role == "upstream"
        assert scope.input_row_filters == []
        assert scope.group_by == ["order_id"]
        assert "buyer_count > 1" in scope.output_row_filters
    assert buyer.expression == "count(customer_id)"
    assert returned.expression == "bool_or(is_returned)"
    # outer statement's WHERE never leaks into the eligibility population
    block = render_derived_value_scopes(scopes)
    assert "<Subselect" not in block and "<Rowset" not in block


def test_scalar_benchmark_rowset_reported_from_having():
    """q14 shape: a scalar benchmark consumed only by HAVING keeps its own
    population (1999-2001), distinct from the statement's."""
    scopes = _scopes("""
with overall_avg as
select
    avg(amount) by * as avg_amount
where year between 1999 and 2001;

select
    state,
    sum(amount) as state_total
where year = 2002
having state_total > overall_avg.avg_amount;
""")
    benchmark = _by_name(scopes, "overall_avg.avg_amount")
    assert benchmark.role == "upstream"
    assert benchmark.input_row_filters == ["year between 1999 and 2001"]
    assert benchmark.group_by == ["*"]
    outer = _by_name(scopes, "state_total")
    assert outer.input_row_filters == ["year = 2002"]
    assert outer.output_row_filters == ["state_total > overall_avg.avg_amount"]


def test_having_subselect_benchmark_owned_by_its_rowset():
    """q14 blocker shape: the final statement consumes `filtered_groups`,
    whose HAVING consumes `overall_avg` through an expression subselect. The
    benchmark must be owned by overall_avg with ITS filters and scalar grain —
    never stamped with the consumer's name, filters, or grain."""
    scopes = _scopes("""
with overall_avg as
where year between 1999 and 2001
select avg(amount) as overall_avg_val;

with filtered_groups as
where year = 2001
select
    state,
    order_id,
    sum(amount) as total_sales
having total_sales > (select overall_avg.overall_avg_val);

select
    filtered_groups.state,
    sum(filtered_groups.total_sales) as rollup_total;
""")
    benchmark = _by_name(scopes, "overall_avg.overall_avg_val")
    assert benchmark.role == "upstream"
    assert benchmark.input_row_filters == ["year between 1999 and 2001"]
    assert benchmark.group_by == ["*"]
    leaf = _by_name(scopes, "filtered_groups.total_sales")
    assert leaf.input_row_filters == ["year = 2001"]
    assert leaf.output_row_filters == [
        "total_sales > (select overall_avg.overall_avg_val)"
    ]
    # no consumer-stamped copy of the benchmark, no opaque planner tokens
    assert not any("_overall_avg" in s.name for s in scopes)
    block = render_derived_value_scopes(scopes)
    assert "<Subquery" not in block and "_subquery" not in block


def test_nested_rowset_chain_reports_benchmark():
    """reported aggregate -> membership rowset -> aggregate benchmark rowset:
    every upstream computation appears with its own scope."""
    scopes = _scopes("""
with high_orders as
select
    order_id,
    sum(amount) as order_total
having order_total > 5;

with eligible_customers as
select
    customer_id
where order_id in high_orders.order_id;

select
    count(customer_id ? customer_id in eligible_customers.customer_id) as buyer_count;
""")
    benchmark = _by_name(scopes, "high_orders.order_total")
    assert benchmark.role == "upstream"
    assert benchmark.expression == "sum(amount)"
    assert benchmark.group_by == ["order_id"]
    assert benchmark.output_row_filters == ["order_total > 5"]


def test_count_family_reports_argument_grain():
    """count identity evidence: each count-family aggregate reports the
    counted ARGUMENT's own grain under `argument_grain` — a distinct field so
    it cannot be read as the input relation's row grain — with no correctness
    claim. Here order_id repeats across sale lines: its identity resolves to
    the sale_id key it is a property of."""
    scopes = _scopes("""
select
    count(order_id) as order_count,
    count(customer_id) as buyer_count,
    sum(amount) as total_amount;
""")
    order_count = _by_name(scopes, "order_count")
    assert order_count.argument_grain == ["sale_id"]
    assert order_count.input_grain == []
    assert _by_name(scopes, "buyer_count").argument_grain == ["sale_id"]
    # non-count aggregates make no argument-identity claim
    total = _by_name(scopes, "total_amount")
    assert total.argument_grain == []
    assert "argument_grain" not in total.to_dict()
    assert order_count.to_dict()["argument_grain"] == ["sale_id"]


def test_scoped_join_preserves_authored_where_and_reports_join():
    """q17 shape: a WHERE equality whose endpoint joins into a scoped-join
    equivalence group keeps its AUTHORED spelling as the input row filter; the
    canonicalized form and the join declarations are reported separately so the
    rewritten endpoint never reads as a different business filter."""
    scopes = _multi_scopes("""
where
    ss_year = 2001
    and ss_return_customer = ss_customer
    and cs_year in (2001, 2002)
select
    count(ss_qty) as store_qty_count
union join ss_customer = cs_billing_customer
union join ss_item = cs_item;
""")
    scope = _by_name(scopes, "store_qty_count")
    # 1. authored equality remains visible, un-rewritten
    assert "ss_return_customer = ss_customer" in scope.input_row_filters
    assert "ss_return_customer = cs_billing_customer" not in scope.input_row_filters
    # 2. scoped joins are distinguishable from row filters
    assert scope.scoped_joins == [
        "union join ss_customer = cs_billing_customer",
        "union join ss_item = cs_item",
    ]
    assert not any("join" in f for f in scope.input_row_filters)
    # 3. the normalized/effective form is exposed and matches the planner's
    #    endpoint canonicalization
    assert "ss_return_customer = cs_billing_customer" in (
        scope.normalized_input_row_filters
    )
    payload = scope.to_dict()
    assert payload["input_row_filters"] == scope.input_row_filters
    assert payload["scoped_joins"] == scope.scoped_joins
    assert payload["normalized_input_row_filters"] == (
        scope.normalized_input_row_filters
    )
    block = render_derived_value_scopes(scopes)
    assert "scoped joins: union join ss_customer = cs_billing_customer" in block
    assert "input row filters: ss_year = 2001; ss_return_customer = ss_customer" in (
        block
    )


def test_scoped_join_removed_restores_authored_presentation():
    """4. removing the scoped join restores the plain authored-filter
    presentation — no normalized twin, no scoped-join field."""
    scopes = _multi_scopes("""
where
    ss_year = 2001
    and ss_return_customer = ss_customer
select
    count(ss_qty) as store_qty_count;
""")
    scope = _by_name(scopes, "store_qty_count")
    assert scope.input_row_filters == [
        "ss_year = 2001",
        "ss_return_customer = ss_customer",
    ]
    assert scope.normalized_input_row_filters == []
    assert scope.scoped_joins == []
    payload = scope.to_dict()
    assert "normalized_input_row_filters" not in payload
    assert "scoped_joins" not in payload


def test_three_member_join_group_keeps_authored_predicate():
    """5. a three-member join group must not change the displayed business
    predicate based on which endpoint the planner picks as canonical."""
    scopes = _multi_scopes("""
where
    ss_year = 2001
    and ss_return_customer = ss_customer
select
    count(ss_qty) as store_qty_count
union join ss_customer = cs_billing_customer = ws_customer;
""")
    scope = _by_name(scopes, "store_qty_count")
    assert "ss_return_customer = ss_customer" in scope.input_row_filters
    # whichever endpoint became canonical, the authored spelling is untouched
    assert not any(
        "ss_return_customer = cs_billing_customer" in f
        or "ss_return_customer = ws_customer" in f
        for f in scope.input_row_filters
    )
    assert scope.scoped_joins == [
        "union join ss_customer = cs_billing_customer",
        "union join cs_billing_customer = ws_customer",
    ]


def test_generated_sql_unchanged_by_diagnostics():
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(MODEL)
    body = "select state, sum(amount) as total_amount where year = 2002;"
    sql = exec.generate_sql(body)
    assert sql
    assert "wscope" not in "\n".join(sql)


def _warning_kinds(body: str) -> list[str]:
    return [w["kind"] for w in derived_value_warnings(_scopes(body))]


def test_warns_window_in_select_with_where():
    warnings = derived_value_warnings(_scopes("""
select
    state,
    year,
    sum(amount) as total_amount,
    rank total_amount over state order by total_amount desc as amount_rank
where year = 2002;
"""))
    match = [w for w in warnings if w["kind"] == "window_filter_needs_having"]
    assert len(match) == 1
    assert match[0]["name"] == "amount_rank"
    assert "HAVING" in match[0]["message"]


def test_no_window_warning_without_where():
    assert "window_filter_needs_having" not in _warning_kinds("""
select
    state,
    sum(amount) as total_amount,
    rank total_amount over state order by total_amount desc as amount_rank;
""")


def test_warns_where_aggregate_inherited_select_grain():
    warnings = derived_value_warnings(_scopes("""
select
    state,
    count(sale_id) as sale_count
where count(sale_id) > 10 and year = 2001;
"""))
    match = [w for w in warnings if w["kind"] == "where_aggregate_inherited_grain"]
    assert len(match) == 1
    assert match[0]["expression"] == "count(sale_id)"
    assert match[0]["group_by"] == ["state"]
    assert "by *" in match[0]["message"]


def test_no_grain_warning_for_explicit_by_in_where():
    assert "where_aggregate_inherited_grain" not in _warning_kinds("""
select
    state,
    count(sale_id) by state as sale_count
where count(sale_id) by state > 10;
""")


def test_no_grain_warning_for_global_aggregate_in_where():
    assert "where_aggregate_inherited_grain" not in _warning_kinds("""
select
    state,
    sum(amount) as total_amount
where sum(amount) by * > 10;
""")
