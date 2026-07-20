from trilogy import Dialects, Environment

# regression (TPC-DS q74): narrowing a filter node's outputs to
# [concept] + optionals dropped the row parent's grain keys (customer_sk /
# year), so the node advertised a coarser, false grain and the downstream
# merge re-correlated web totals to customers on non-unique
# (first_name, last_name) — silently fanning each customer's totals out to
# every same-named customer.
MODEL = """
key csk int;
property csk.cid string;
property csk.first_name string;
property csk.last_name string;

key ss_id int;
property ss_id.ss_year int;
property ss_id.ss_net float;

key ws_id int;
property ws_id.ws_year int;
property ws_id.ws_net float;

datasource customers (
    csk: csk,
    cid: cid,
    first_name: first_name,
    last_name: last_name,
)
grain (csk)
query '''
select 1 as csk, 'A' as cid, 'John' as first_name, 'Doe' as last_name
union all
select 2, 'B', 'John', 'Doe'
union all
select 3, 'C', 'Jane', 'Smith'
''';

datasource store_sales (
    id: ss_id,
    customer: csk,
    year: ss_year,
    net: ss_net,
)
grain (ss_id)
query '''
select 1 as id, 1 as customer, 2001 as year, 100.0 as net
union all select 2, 1, 2002, 100.0
union all select 3, 2, 2001, 100.0
union all select 4, 2, 2002, 100.0
union all select 5, 3, 2001, 100.0
union all select 6, 3, 2002, 100.0
''';

datasource web_sales (
    id: ws_id,
    customer: csk,
    year: ws_year,
    net: ws_net,
)
grain (ws_id)
query '''
select 1 as id, 1 as customer, 2001 as year, 100.0 as net
union all select 2, 1, 2002, 300.0
union all select 3, 2, 2001, 100.0
union all select 4, 2, 2002, 50.0
union all select 5, 3, 2001, 100.0
union all select 6, 3, 2002, 200.0
''';
"""

QUERY = """
with st as
where csk is not null and ss_year in (2001, 2002)
select
    csk as s_csk,
    cid as s_cid,
    first_name as s_fn,
    last_name as s_ln,
    ss_year as s_yr,
    sum(ss_net) as st_tot
;

with wb as
where csk is not null and ws_year in (2001, 2002)
select
    csk as w_csk,
    cid as w_cid,
    first_name as w_fn,
    last_name as w_ln,
    ws_year as w_yr,
    sum(ws_net) as wb_tot
;

where st.s_cid is not null
select
    st.s_cid as customer_code,
    st.s_fn as first_name_out,
    st.s_ln as last_name_out
  subset join st.s_csk = wb.w_csk
  subset join st.s_yr = wb.w_yr
having
  sum(st.st_tot ? st.s_yr = 2001) > 0
  and sum(wb.wb_tot ? wb.w_yr = 2001) > 0
  and sum(st.st_tot ? st.s_yr = 2002) is not null
  and sum(wb.wb_tot ? wb.w_yr = 2002) is not null
  and (sum(wb.wb_tot ? wb.w_yr = 2002) / sum(wb.wb_tot ? wb.w_yr = 2001))
      > (sum(st.st_tot ? st.s_yr = 2002) / sum(st.st_tot ? st.s_yr = 2001))
order by customer_code asc nulls first;
"""


def test_year_over_year_growth_not_recorrelated_on_names():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = [tuple(r) for r in executor.execute_query(QUERY).fetchall()]
    # customer B shares (John, Doe) with A but has a declining web ratio
    # (50/100) vs store (100/100), so B must not appear; a name-based
    # re-correlation pools A+B web totals and admits both.
    assert rows == [("A", "John", "Doe"), ("C", "Jane", "Smith")], rows
