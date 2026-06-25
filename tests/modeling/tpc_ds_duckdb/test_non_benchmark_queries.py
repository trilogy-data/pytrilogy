import re
from datetime import datetime
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.build import BuildAggregateWrapper, BuildGrain
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import (
    History,
    generate_graph,
    search_concepts,
)
from trilogy.core.processing.discovery_utility import (
    calculate_effective_parent_grain,
    check_if_group_required,
)
from trilogy.core.query_processor import process_query
from trilogy.parser import parse_text

working_path = Path(__file__).parent


def test_demo(engine):
    query = """
import physical_sales as physical_sales;
with ranked_states as
select 
    physical_sales.customer.first_name,
    physical_sales.customer.address.state,
    rank physical_sales.customer.first_name 
        over physical_sales.customer.address.state 
        order by  sum(physical_sales.sales_price) by physical_sales.customer.first_name, physical_sales.customer.address.state desc 
    -> sales_rank;

select 
    ranked_states.physical_sales.customer.first_name,
    avg(cast(ranked_states.sales_rank as int))-> avg_sales_rank
order by 
    avg_sales_rank desc
limit 10
;"""

    results = engine.execute_text(query)[0].fetchall()

    assert results[0].avg_sales_rank != 1.0


def test_copy_perf():
    env, imports = Environment(working_path=working_path).parse("""
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import physical_returns as physical_returns;
import physical_sales as physical_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
""")

    start = datetime.now()
    _ = env.duplicate()
    end = datetime.now()
    duration = end - start
    dumped = env.to_dict()
    assert duration.total_seconds() < 2, f"{len(dumped)}, {duration}"


def test_generate_queries_perf():

    env, _ = Environment(working_path=working_path).parse("""
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import physical_returns as physical_returns;
import physical_sales as physical_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
""")

    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    test_queries = """
select
    physical_sales.date.year,
    count(physical_sales.ticket_number) as store_order_count;

select
    web_sales.date.year,
    count(web_sales.order_number) as web_order_count;

select
    catalog_sales.date.year,
    count(catalog_sales.order_number) as catalog_order_count;


"""
    durations = []
    for _ in range(5):
        start = datetime.now()
        dialect.parse_text(test_queries)
        end = datetime.now()
        durations.append((end - start).total_seconds())
    # 0.4037
    avg_duration = sum(durations) / len(durations)
    print(f"Parse times: {durations}")
    print(f"Average parse time: {avg_duration:.4f}s")
    assert avg_duration < 2.0, f"Average duration: {avg_duration:.4f}s"


def _self_referential_joins(sql: str) -> list[str]:
    """CTE names whose own join ON clause references the CTE itself — invalid
    SQL (a CTE can't alias itself in its own body)."""
    bad = []
    for m in re.finditer(r"(\w+) as \(\n(.*?)(?=\n\w+ as \(|\Z)", sql, re.DOTALL):
        name, body = m.group(1), m.group(2)
        for jref in re.findall(
            r'JOIN\s+"?[\w".]+"?\s+(?:as\s+"\w+"\s+)?on "(\w+)"', body
        ):
            if jref == name:
                bad.append(name)
    return bad


def _out_of_scope_join_aliases(sql: str) -> list[tuple[str, str]]:
    """(cte, alias) pairs where a join ON references a table alias not introduced
    by that CTE's own FROM/JOIN — invalid SQL (alias bound in a sibling CTE)."""
    bad: list[tuple[str, str]] = []
    for m in re.finditer(r"(\w+) as \(\n(.*?)(?=\n\w+ as \(|\Z)", sql, re.DOTALL):
        name, body = m.group(1), m.group(2)
        in_scope = set(re.findall(r'(?:FROM|JOIN)\s+"?[\w".]+"?\s+as\s+"(\w+)"', body))
        in_scope |= set(re.findall(r'(?:FROM|JOIN)\s+"(\w+)"(?!\."|\s+as)', body))
        for ref in re.findall(r"\bon\b.*", body):
            for alias in re.findall(r'"(\w+)"\."', ref):
                if alias not in in_scope:
                    bad.append((name, alias))
    return bad


_AGG_FNS = ("sum", "avg", "count", "min", "max", "stddev", "median")


def _has_nested_aggregate(sql: str) -> bool:
    """True if any aggregate call's argument span contains another aggregate call
    — e.g. `sum(CASE WHEN ... THEN sum(x) END)`. DuckDB rejects this with
    'aggregate function calls cannot be nested'."""
    low = sql.lower()
    agg_open = re.compile(r"\b(?:%s)\(" % "|".join(_AGG_FNS))
    for m in agg_open.finditer(low):
        depth, i = 1, m.end()
        while i < len(low) and depth > 0:
            if low[i] == "(":
                depth += 1
            elif low[i] == ")":
                depth -= 1
            i += 1
        if agg_open.search(low, m.end(), i):
            return True
    return False


def test_or_membership_with_projected_aggregate(engine):
    """Regression for bug B2 + its two co-sourcing siblings, using the full
    documented repro (feeder filters include a ``date.year`` join):

    * B2: ``A in X or A in Y and <agg> is not null`` where ``<agg>`` is also a
      projected output. The aggregate makes the condition non-scalar, so the
      group node lifts it onto a wrapper SELECT — which used to strip the
      membership feeder sources, leaving ``in (select ...)`` pointing at a CTE
      that was never emitted (INVALID_REFERENCE_BUG).
    * Feeder-CTE join self-reference: the feeder's date join rendered its own
      CTE name as the left alias (``on "feeder"."s_date_id"``) → BinderException.
    * Predicate-pushdown over-pruning: the feeders' ``channel`` filters pruned
      the union scan they share with the unfiltered STORE aggregate down to one
      channel, so the STORE sum read no STORE rows.
    """
    query = """
import all_sales as s;

auto cat_qual <- s.billing_customer.id ? s.channel = 'CATALOG' and s.date.year = 1998;
auto web_qual <- s.billing_customer.id ? s.channel = 'WEB'     and s.date.year = 1998;
auto cust_total <- sum(s.ext_sales_price ? s.channel = 'STORE') by s.billing_customer.id;

where s.billing_customer.id in cat_qual
   or s.billing_customer.id in web_qual
  and cust_total is not null
select
    round(cust_total / 50) as segment,
    count(s.billing_customer.id) as customer_count
limit 100;
"""
    sql = engine.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    # the membership feeders must resolve to real, declared CTEs
    referenced = set(re.findall(r"in \(select (\w+)\.", sql))
    declared = set(re.findall(r"(\w+) as \(", sql))
    assert referenced, sql
    assert referenced <= declared, f"dangling membership CTEs: {referenced - declared}"
    # no feeder CTE may reference itself in its own join ON
    assert not _self_referential_joins(sql), sql
    # the STORE aggregate's union scan must keep all channels (not pruned to one)
    assert "store_sales" in sql and "web_sales" in sql and "catalog_sales" in sql, sql
    # and the query executes end-to-end against real data
    engine.execute_text(query)[0].fetchall()


def test_merge_comparison(engine):

    x = """
import physical_sales as physical_sales;
import web_sales as web_sales;




    SELECT
    physical_sales.date.year,
    count(physical_sales.ticket_number) as store_order_count
HAVING
    store_order_count>0
MERGE
SELECT
    web_sales.date.year,
    count(web_sales.order_number) as web_order_count
HAVING
    web_order_count>0 
ALIGN 
    report_date: physical_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    r1 = list(engine.execute_text(x)[0].fetchall())
    assert r1[0].web_order_count == 11951

    y = """
import physical_sales as physical_sales;
import web_sales as web_sales;
import date as date; 

MERGE physical_sales.date.* into ~date.*;
MERGE web_sales.date.* into ~date.*;

datasource filtered_cache (
    store_sales_date_month_of_year: ~physical_sales.date.month_of_year,
    store_sales_date_year: ~physical_sales.date.year,
    store_sales_ext_sales_price: ~physical_sales.ext_sales_price,
    store_sales_item_brand_id: ~physical_sales.item.brand_id,
    store_sales_item_brand_name: ~physical_sales.item.brand_name,
    store_sales_item_id: ~physical_sales.item.id,
    store_sales_item_manufacturer_id: ~physical_sales.item.manufacturer_id,
    store_sales_ticket_number: ~physical_sales.ticket_number
)
complete where physical_sales.date.month_of_year = 11 and physical_sales.item.manufacturer_id = 128
address filtered_cache;


SELECT
    date.year,
    count(web_sales.order_number) as web_order_count,
    count(physical_sales.ticket_number) as store_order_count
HAVING
    web_order_count>0 or store_order_count>0
ORDER BY 
    date.year asc
LIMIT 100;"""

    r2 = list(engine.execute_text(y)[0].fetchall())
    # assert 1 == 0
    for idx, row in enumerate(r1):
        r2_row = r2[idx]
        assert row.web_order_count == r2_row.web_order_count
        assert row.store_order_count == r2_row.store_order_count


def test_having_nested(engine):

    y = """
import physical_sales as physical_sales;
import web_sales as web_sales;

SELECT
    physical_sales.date.year,
    count_distinct(physical_sales.ticket_number) as store_order_count
having
    store_order_count > 1000
MERGE
SELECT
    web_sales.date.year,
    count_distinct(web_sales.order_number) as web_order_count

ALIGN 
    report_date: physical_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    r1 = engine.execute_query(y).fetchall()
    for row in r1:
        assert row.store_order_count is None or row.store_order_count > 1000, row


def test_website_demo(engine):
    query = """import physical_sales as physical_sales;
select 
    physical_sales.customer.id, 
    physical_sales.customer.full_name,
    physical_sales.ticket_number, 
limit 5;    
"""
    engine.execute_query(query).fetchall()


def test_where_clause_inputs(engine):
    y = """import physical_sales as physical_sales;
import catalog_sales as catalog_sales;

merge catalog_sales.bill_customer.id into physical_sales.customer.id;
merge catalog_sales.item.id into physical_sales.item.id;

SELECT 
    physical_sales.item.product_name,
    physical_sales.item.desc,
    physical_sales.store.text_id,
    physical_sales.store.name,
    sum(physical_sales.net_profit) AS store_sales_profit ,
    sum(physical_sales.return_net_loss) AS store_returns_loss ,
    sum(catalog_sales.net_profit) AS catalog_sales_profit
WHERE 
    physical_sales.is_returned and physical_sales.date.year=2001 and physical_sales.date.month_of_year=4
    and physical_sales.return_date.year=2001 and physical_sales.return_date.month_of_year between 4 and 10
    and catalog_sales.date.year=2001 and catalog_sales.date.month_of_year between 4 and 10
    and physical_sales.return_customer.id = physical_sales.customer.id
ORDER BY 
    physical_sales.item.product_name asc,
    physical_sales.item.desc asc,
    physical_sales.store.text_id asc,
    physical_sales.store.name asc
LIMIT 100;"""
    r1 = engine.parse_text(y)[-1]
    found = False
    for cte in r1.ctes:
        if cte.condition and "physical_sales.is_returned" in [
            x.address for x in cte.condition.row_arguments
        ]:
            found = True
            break
    assert found


def test_constant_extra(engine):
    query = """import physical_sales as physical_sales;

where physical_sales.date.year = 2001
select 
    count(physical_sales.customer.id)->ccount, 
    1 as test,
limit 5;    
"""
    engine.execute_query(query).fetchall()


def test_merge_grain_discovery(engine: Executor):

    engine.parse_text("""import physical_sales as physical_sales;""")
    environment = engine.environment
    build_environment = environment.materialize_for_select()
    graph = generate_graph(build_environment)

    target_concepts = [
        build_environment.concepts["physical_sales.ticket_number"],
        build_environment.concepts["physical_sales.date.year"],
        build_environment.concepts["physical_sales.item.id"],
    ]
    node = search_concepts(
        mandatory_list=target_concepts,
        history=History(base_environment=environment),
        environment=build_environment,
        g=graph,
        depth=0,
        accept_partial=False,
    )
    grain = calculate_effective_parent_grain(node.resolve())
    assert (
        grain.components
        == BuildGrain(
            components={"physical_sales.ticket_number", "physical_sales.item.id"}
        ).components
    ), grain.components

    assert not check_if_group_required(
        downstream_concepts=target_concepts,
        parents=[node.resolve()],
        environment=build_environment,
    ).required


def test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate():
    query = """
    import all_sales as sales;

    def weekday_sum(weekday) -> sum(
        sales.ext_sales_price ? sales.date.day_of_week = weekday
    ) by sales.date.week_seq;

    SELECT
        sales.date.week_seq,
        @weekday_sum(0) / @weekday_sum(1) as sun_over_mon
    ORDER BY sales.date.week_seq asc
    LIMIT 5;
    """

    env = Environment(working_path=working_path)
    _, statements = parse_text(query, env)
    processed = process_query(env, statements[-1])
    grouped_cte = processed.ctes[-1]

    aggregate_outputs = [
        c
        for c in grouped_cte.output_columns
        if isinstance(c.lineage, BuildAggregateWrapper)
    ]
    assert len(aggregate_outputs) == 2
    assert [c.address for c in grouped_cte.group_concepts] == ["sales.date.week_seq"]


def test_def_body_can_call_another_custom_function():
    """Regression: FunctionCallWrapper.with_reference_replacement used to raise
    NotImplementedError, breaking any `def` whose body wrapped another @-call."""
    query = """
    import all_sales as sales;

    def weekday_sum(weekday) -> sum(
        sales.ext_sales_price ? sales.date.day_of_week = weekday
    ) by sales.date.week_seq;

    def doubled_weekday_sum(weekday) -> @weekday_sum(weekday) * 2;

    SELECT
        sales.date.week_seq,
        @doubled_weekday_sum(0) as sun_doubled
    ORDER BY sales.date.week_seq asc
    LIMIT 5;
    """

    env = Environment(working_path=working_path)
    _, statements = parse_text(query, env)
    process_query(env, statements[-1])


def test_two_merge_aggregate_compacts_inline_window_query():
    query = """
    import catalog_sales as catalog_sales;
    import web_sales as web_sales;
    import date as date;

    merge catalog_sales.date.* into ~date.*;
    merge web_sales.date.* into ~date.*;

    auto relevent_week_seq <- filter date.week_seq where date.year in (2001, 2002);

    def weekday_sales(weekday) ->
        (SUM(CASE WHEN date.day_of_week = weekday THEN web_sales.ext_sales_price ELSE 0.0 END) by date.week_seq +
        SUM(CASE WHEN date.day_of_week = weekday THEN catalog_sales.ext_sales_price ELSE 0.0 END) by date.week_seq)
    ;

    def round_lag(sales)-> round(sales / (lead 53 sales by date.week_seq asc), 2);

    WHERE
        date.week_seq in relevent_week_seq
    SELECT
        date.week_seq,
        @round_lag(@weekday_sales(0)) as sunday_increase,
        @round_lag(@weekday_sales(1)) as monday_increase,
        @round_lag(@weekday_sales(2)) as tuesday_increase,
        @round_lag(@weekday_sales(3)) as wednesday_increase,
        @round_lag(@weekday_sales(4)) as thursday_increase,
        @round_lag(@weekday_sales(5)) as friday_increase,
        @round_lag(@weekday_sales(6)) as saturday_increase
    having sunday_increase is not null
    ORDER BY date.week_seq asc NULLS FIRST
    LIMIT 100;
    """

    original = CONFIG.optimizations.merge_aggregate
    try:
        CONFIG.optimizations.merge_aggregate = False
        off_env = Environment(working_path=working_path)
        _, off_statements = parse_text(query, off_env)
        off_processed = process_query(off_env, off_statements[-1])
        off_generated = Dialects.DUCK_DB.default_executor(
            environment=off_env
        ).generate_sql(query)[-1]
        CONFIG.optimizations.merge_aggregate = True
        on_env = Environment(working_path=working_path)
        _, on_statements = parse_text(query, on_env)
        on_processed = process_query(on_env, on_statements[-1])
    finally:
        CONFIG.optimizations.merge_aggregate = original

    assert len(off_processed.ctes) == 9, off_generated
    assert len(on_processed.ctes) == 5


def test_rowset_arithmetic_argument_keeps_precedence():
    query = (working_path / "query02-one.preql").read_text()
    env = Environment(working_path=working_path)
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]

    assert re.search(r"round\(\( .*? \+ .*? \) / \(lead", sql, re.S), sql


def _assert_having_membership_subselect_valid(query: str) -> None:
    """A HAVING `x in <set>` must source its existence subselect from a real
    producer CTE present in the WITH list — not a dangling reference. The HAVING
    application routes through `append_existence_check` (mirroring the WHERE
    path), so the predicate's subselect is wired regardless of node shape."""
    env = Environment(working_path=working_path)
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    defined_ctes = set(re.findall(r"\n(\w+) as \(", sql))
    referenced = re.search(r"in \(select (\w+)\.", sql)
    assert referenced is not None, sql
    assert referenced.group(1) in defined_ctes, sql


def test_membership_in_having_over_window_renders_valid_subselect():
    # CTE-handle form: the HAVING-carrying node is a WhereSafetyNode padding the
    # window output.
    _assert_having_membership_subselect_valid("""
import all_sales as all_sales;

auto ws_2001 <- all_sales.date.week_seq ? all_sales.date.year = 2001;

with weekly_dow as
where all_sales.channel in ('WEB', 'CATALOG')
select
    all_sales.date.week_seq as ws,
    sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0) as sun
;

select
    weekly_dow.ws as week_sequence,
    round(weekly_dow.sun / (lead(weekly_dow.sun, 53) over (order by weekly_dow.ws asc)), 2) as sun_ratio,
    --weekly_dow.ws in ws_2001 as in_2001
having
    weekly_dow.ws in ws_2001
order by weekly_dow.ws asc nulls first
;
""")


def test_membership_in_having_auto_concept_renders_valid_subselect():
    # Auto-concept form: the HAVING-carrying node is a GroupNode that does NOT
    # itself source the set (it lands on the inner flag node), so the existence
    # must be sourced at the HAVING site. The narrow WhereSafetyNode fold missed
    # this; the general append_existence_check covers it.
    _assert_having_membership_subselect_valid("""
import all_sales as all_sales;

auto ws_2001 <- all_sales.date.week_seq ? all_sales.date.year = 2001;

select
    all_sales.date.week_seq as ws,
    --ws in ws_2001 as in_2001,
    sum(all_sales.ext_sales_price) as sun
having ws in ws_2001
;
""")


def test_membership_in_having_no_projected_flag_renders_valid_subselect():
    # The membership's set need not be projected as a hidden flag: the validator
    # uses row_arguments (existence RHS exempt), and the HAVING site sources the
    # set directly. This rowset + window + filtered-aggregate shape used to crash
    # in DISCOVERY building the forced hidden flag's output node ('local.ws'
    # missing parent); with no flag required, that node is never built.
    _assert_having_membership_subselect_valid("""
import all_sales as all_sales;

rowset ws_2001 <- select all_sales.date.week_seq
    where all_sales.channel != 'STORE' and all_sales.date.year = 2001;
auto wk_sun <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0);

select
    all_sales.date.week_seq as ws,
    wk_sun as sun,
    lead(wk_sun, 53) over (order by all_sales.date.week_seq) as next_sun
having ws in ws_2001.all_sales.date.week_seq
;
""")


def test_rank_over_projected_aggregate_ratio_no_recursion():
    # Bug B1: a rank() whose order_by is the same sum(a)/sum(b) ratio that is
    # also projected, with a partition key not in the projection, used to push
    # the window output into the select grain. The abstract sums then resolved
    # their `by` against that grain — grouping by the rank, whose order_by
    # rebuilt the sums — a build-time RecursionError. The grain must instead use
    # the window's keys (item, channel).
    query = """
    import all_sales as s;
    where s.return_amount > 10000
    select
        s.item.id as item,
        sum(s.return_quantity) / sum(s.quantity) as return_quantity_ratio,
        rank(s.item.id) over (
            partition by s.channel
            order by sum(s.return_quantity) / sum(s.quantity) asc
        ) as rank_a
    limit 100;
    """
    env = Environment(working_path=working_path)
    _, statements = parse_text(query, env)
    select_grain = statements[-1].grain
    assert select_grain.components == {"s.item.id", "s.channel"}, select_grain
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]
    assert re.search(r"rank\(\) over \(partition by", sql), sql


def test_unified_model_customer_address_cross_cte_join(engine):
    # Bug (enriched q6): grouping/filtering by a 2-hop
    # `<role>_customer.address.<prop>` on the unified `all_sales` model. The
    # customer->address FK key (C_CURRENT_ADDR_SK) is materialized by the grouped
    # parent CTE, but datasource inlining left the raw customer table inlined-but-
    # dangling, so the address join's ON referenced the customer alias from a
    # sibling CTE that isn't in scope ("table ... not found"). The left key must
    # bind against the emitted parent CTE that exposes the FK column.
    for role in ("purchasing_customer", "billing_customer"):
        query = f"""
import all_sales as sales;
where sales.date.year = 2001
  and sales.{role}.address.id is not null
select
    sales.{role}.address.state as state,
    count(sales.item.id) as n
limit 100;
"""
        sql = engine.generate_sql(query)[-1]
        # every join ON alias must be introduced by that CTE's own FROM/JOIN
        assert not _out_of_scope_join_aliases(sql), sql
        # executes end-to-end against real data (was a BinderException)
        engine.execute_text(query)[0].fetchall()


def test_or_filter_over_differently_filtered_aggregates_no_recursion(engine):
    # Bug B1 (second shape, enriched q77): a measure that is a difference of two
    # inline-`?`-filtered values with DIFFERENT filter flags, aggregated, while a
    # WHERE references those same flags as an OR. The flag `f1` is both a derived
    # row-argument of `(f1 or f2)` and a concept that must be built (the `? f1`
    # operand). The discovery loop routed the condition into building `f1`, but
    # sourcing `f1`'s parents re-injected `f1` via the condition's row args —
    # unbounded (opaque Python RecursionError). A condition can't be routed into
    # building one of its own derived inputs; the flag is built first and the
    # condition applied once above, over the joined rows.
    query = """
    import all_sales as sales;
    auto f1 <- sales.date.date between '2000-08-23'::date and '2000-09-22'::date;
    auto f2 <- sales.return_date.date between '2000-08-23'::date and '2000-09-22'::date;
    auto m <- coalesce(sales.net_profit ? f1, 0) - coalesce(sales.return_net_loss ? f2, 0);
    where sales.channel_dim_id is not null and (f1 or f2)
    select sales.channel_dim_id, sum(m::numeric(15,2)) as t
    limit 100;
    """
    sql = engine.generate_sql(query)[-1]
    # the (f1 or f2) row filter must survive as an OR of both date-range checks,
    # not be silently dropped when lifted off the recursive level
    assert re.search(
        r"between date '2000-08-23'.*?\bor\b.*?between date '2000-08-23'",
        sql,
        re.S | re.I,
    ), sql
    # and the query resolves and executes end-to-end against real data
    engine.execute_text(query)[0].fetchall()


def test_pivot_over_filtered_rowset_aggregate_no_nested_aggregate():
    # Enriched q02 shape: a rowset aggregates a measure at a fine grain
    # (`day_sales <- sum(ext_sales_price) by week_seq, dow, year`), then a second
    # rowset pivots it with per-day inline filters (`sum(day_sales ? dow = N)`)
    # under a membership filter on week_seq. The membership filter forms a FILTER
    # node carrying the `day_sales ? dow=N` expression (an inline filter over the
    # un-materialized daily aggregate) between the two GROUP BYs. CollapseSingleParent
    # folded that FILTER parent into the pivot's GROUP, inlining the daily aggregate
    # inside the pivot's sum() -> `sum(CASE WHEN dow=N THEN sum(ext_sales_price) END)`,
    # which DuckDB rejects ("aggregate function calls cannot be nested"). The daily
    # aggregate must stay materialized in its own CTE.
    query = """
import all_sales as s;

with daily_sales as
where s.channel in ('WEB', 'CATALOG')
select
    s.date.week_seq as week_seq,
    s.date.day_of_week as dow,
    s.date.year as year,
    sum(s.ext_sales_price) as day_sales
;

with weeks_2001 as
where s.channel in ('WEB', 'CATALOG') and s.date.year = 2001
select s.date.week_seq as week_seq
;

with pivoted as
where daily_sales.week_seq in weeks_2001.week_seq
select
    daily_sales.week_seq,
    sum(daily_sales.day_sales ? daily_sales.dow = 0) as sun_sales,
    sum(daily_sales.day_sales ? daily_sales.dow = 1) as mon_sales
;

select
    pivoted.daily_sales.week_seq as week_seq,
    round(pivoted.sun_sales / lead(pivoted.sun_sales, 53) over (order by pivoted.daily_sales.week_seq), 2) as sun_ratio,
    round(pivoted.mon_sales / lead(pivoted.mon_sales, 53) over (order by pivoted.daily_sales.week_seq), 2) as mon_ratio
order by pivoted.daily_sales.week_seq nulls first;
"""
    env = Environment(working_path=working_path)
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]
    assert not _has_nested_aggregate(sql), sql

def test_property_via_partial_fk_does_not_broadcast(engine_sf001):
    # q05 shape: an entity label (`return_channel_dim_text_id`, a PROPERTY of the
    # FK `return_channel_dim_id`) is selected at row grain alongside its return
    # measure. The FK is PARTIAL on the returns grain (web_returns doesn't map it),
    # so the planner sourced the FK from the dim datasource (where it's the grain
    # key) instead of from the returns fact, then joined fact->dim on the only
    # shared concept, `channel`. That channel-only join fans out: every catalog
    # page in the channel pairs with the full channel return set, so an outer
    # `sum(return_amount) by entity` broadcasts the channel total identically onto
    # every entity. The FK must be sourced from the fact so the dim join keys on it.
    query = """
import all_sales as s;

with combined as union(
  (where s.date.date between '2000-08-23'::date and '2000-09-06'::date
    and s.channel_dim_id is not null
   select
     case when s.channel = 'CATALOG' then 'catalog channel' else 'other' end as ch,
     concat('catalog_page', s.channel_dim_text_id) as ent,
     s.ext_sales_price as gs,
     0::float as ret_amt),
  (where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date
    and s.return_channel_dim_id is not null
   select
     case when s.channel = 'CATALOG' then 'catalog channel' else 'other' end as ch,
     concat('catalog_page', s.return_channel_dim_text_id) as ent,
     0::float as gs,
     s.return_amount as ret_amt)
) -> (channel_type, entity_id, gross_sales, return_amounts);

select
  combined.channel_type,
  combined.entity_id,
  sum(combined.gross_sales) as total_gross_sales,
  sum(combined.return_amounts) as total_returns
where combined.channel_type = 'catalog channel'
order by total_returns desc
limit 10;
"""
    rows = engine_sf001.execute_text(query)[0].fetchall()
    totals = [round(r.total_returns, 2) for r in rows]
    # A broadcast collapses every page onto the same channel total; correct
    # per-page returns vary.
    assert len(set(totals)) > 1, totals
