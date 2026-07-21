import sys

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

sys.path.insert(0, "tests/discovery")
from test_filter_node_retains_row_grain_keys import MODEL  # noqa: E402

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
for f in CONFIG.optimizations.__dataclass_fields__:
    if isinstance(getattr(CONFIG.optimizations, f), bool):
        setattr(CONFIG.optimizations, f, False)

QUERIES = {
    # just the st rowset, projected
    "a": """
with st as
where csk is not null and ss_year in (2001, 2002)
select csk as s_csk, cid as s_cid, first_name as s_fn, ss_year as s_yr,
    sum(ss_net) as st_tot;

select st.s_csk, st.s_cid, st.st_tot order by st.s_csk asc;
""",
    # rowset with dim + agg, aggregated in outer
    "b": """
with st as
where csk is not null and ss_year in (2001, 2002)
select csk as s_csk, cid as s_cid, first_name as s_fn, ss_year as s_yr,
    sum(ss_net) as st_tot;

select st.s_cid, sum(st.st_tot) as t order by st.s_cid asc;
""",
    # plain select, no rowset
    "c": """
where csk is not null and ss_year in (2001, 2002)
select csk, cid, first_name, ss_year, sum(ss_net) as st_tot order by csk asc;
""",
    # rowset without the WHERE
    "d": """
with st as
select csk as s_csk, cid as s_cid, first_name as s_fn, ss_year as s_yr,
    sum(ss_net) as st_tot;

select st.s_csk, st.s_cid, st.st_tot order by st.s_csk asc;
""",
    # rowset, no first_name
    "e": """
with st as
where csk is not null and ss_year in (2001, 2002)
select csk as s_csk, cid as s_cid, ss_year as s_yr, sum(ss_net) as st_tot;

select st.s_csk, st.s_cid, st.st_tot order by st.s_csk asc;
""",
    # outer projects the full rowset output
    "f": """
with st as
select csk as s_csk, cid as s_cid, ss_year as s_yr, sum(ss_net) as st_tot;

select st.s_csk, st.s_cid, st.s_yr, st.st_tot order by st.s_csk asc;
""",
    # outer projects key + agg only (no property)
    "g": """
with st as
select csk as s_csk, cid as s_cid, ss_year as s_yr, sum(ss_net) as st_tot;

select st.s_csk, st.s_yr, st.st_tot order by st.s_csk asc;
""",
    # outer projects property + agg only (no key)
    "h": """
with st as
select csk as s_csk, cid as s_cid, ss_year as s_yr, sum(ss_net) as st_tot;

select st.s_cid, st.s_yr, st.st_tot order by st.s_cid asc;
""",
}

q = QUERIES[sys.argv[2]]
env = Environment()
env.parse(MODEL)
exe = Dialects.DUCK_DB.default_executor(environment=env)
print(exe.generate_sql(q)[-1])
print("---- ROWS ----")
for r in exe.execute_query(q).fetchall():
    print(tuple(r))
