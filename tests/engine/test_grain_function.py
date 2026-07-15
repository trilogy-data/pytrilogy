import pytest

from trilogy import Dialects, Environment

DDL = """
key rid int;
property rid.fname string;
property rid.lname string;
property rid.sdate date;

key item_id int;
key store_id int;

datasource lines (
    rid: rid,
    fname: fname,
    lname: lname,
    sdate: sdate,
    item_id: item_id,
    store_id: store_id,
)
grain (rid)
address lines;
"""

# (fname, lname) combos: (ann,smith) x2, (bob,NULL) x2, (cy,NULL), ('',NULL), (NULL,'')
# -> 5 distinct (fname, lname, sdate) combinations over 7 rows.
SEED = """
create table lines as select * from (values
    (1, 'ann', 'smith', date '2020-01-01', 10, 100),
    (2, 'ann', 'smith', date '2020-01-01', 10, 100),
    (3, 'bob', NULL,    date '2020-01-01', 10, 200),
    (4, 'bob', NULL,    date '2020-01-01', 20, 200),
    (5, 'cy',  NULL,    date '2020-01-02', 20, 100),
    (6, '',    NULL,    date '2020-01-03', 20, 100),
    (7, NULL,  '',      date '2020-01-03', 20, 100)
) t(rid, fname, lname, sdate, item_id, store_id)
"""


def make_executor():
    env = Environment()
    env.parse(DDL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_raw_sql(SEED)
    return executor


def scalar(executor, query: str):
    return executor.execute_raw_sql(executor.generate_sql(query)[-1]).fetchall()[0][0]


def test_grain_counts_null_bearing_combinations():
    """The hash is total, so a NULL member does not delete the row — this is the
    q87 failure (count of a nullable column silently drops those rows)."""
    executor = make_executor()
    assert scalar(executor, "select count(lname) as c;") == 3
    assert scalar(executor, "select count(grain(fname, lname, sdate)) as c;") == 7
    assert (
        scalar(executor, "select count_distinct(grain(fname, lname, sdate)) as c;") == 5
    )


def test_grain_null_sentinel_prevents_collision():
    """('', NULL) and (NULL, '') must hash differently — without a NULL sentinel
    distinct from the empty string they would collide and undercount."""
    executor = make_executor()
    assert scalar(executor, "select count_distinct(grain(fname, lname)) as c;") == 5


def test_grain_over_coarser_keys_dedupes_to_the_tuple():
    """Args that are keys carry their own grain, so the population dedupes to the
    tuple: 4 distinct (item_id, store_id) pairs across 7 rows."""
    executor = make_executor()
    assert scalar(executor, "select count(grain(item_id, store_id)) as c;") == 4
    sql = executor.generate_sql("select count(grain(item_id, store_id)) as c;")[-1]
    assert "group by" in sql.lower()


def test_grain_args_survive_into_the_group_by():
    """Anti-prune guard: the tuple columns are load-bearing GROUP BY keys. If an
    'unused column' pass drops them the dedupe collapses and the count inflates —
    silently. The rendering references every argument to prevent exactly that."""
    executor = make_executor()
    sql = executor.generate_sql("select count(grain(item_id, store_id)) as c;")[-1]
    assert "item_id" in sql and "store_id" in sql
    assert scalar(executor, "select count(grain(item_id, store_id)) as c;") == 4


def test_grain_composes_with_filter():
    executor = make_executor()
    query = (
        "select count(grain(fname, lname, sdate) ? sdate = '2020-01-01'::date) as c;"
    )
    assert scalar(executor, query) == 4


def test_grain_requires_two_keys():
    env = Environment()
    env.parse(DDL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        executor.generate_sql("select count(grain(fname)) as c;")
    except Exception:
        return
    raise AssertionError("single-argument grain() should not parse")


# --- grain() over imported-dimension keys (q23 shape) ---
# A `sales` fact at (ticket, item_sk) grain — finer than the (item_sk, date_sk)
# tuple grain() declares. item_id/item_desc are 1:1 properties of item_sk, so
# count(grain(item_id, item_desc, date_sk)) must dedupe to distinct sold dates
# per item: item 10 sells on 2 dates, item 20 on 1.
STAR_DDL = """
key item_sk int;
property item_sk.item_id int;
property item_sk.item_desc string;

key date_sk int;
property date_sk.yr int;

key ticket int;
property <ticket, item_sk>.qty int;

datasource items (
    item_sk: item_sk,
    item_id: item_id,
    item_desc: item_desc,
)
grain (item_sk)
address items;

datasource dates (
    date_sk: date_sk,
    yr: yr,
)
grain (date_sk)
address dates;

datasource sales (
    ticket: ticket,
    item_sk: item_sk,
    date_sk: date_sk,
    qty: qty,
)
grain (ticket, item_sk)
address sales;
"""

STAR_SEED = """
create table items as select * from (values
    (10, 1000, 'widget alpha'), (20, 2000, 'widget beta')
) t(item_sk, item_id, item_desc);
create table dates as select * from (values
    (1, 2000), (2, 2000), (3, 1999)
) t(date_sk, yr);
create table sales as select * from (values
    (1, 10, 1, 5), (2, 10, 1, 5), (3, 10, 1, 5), (4, 10, 2, 5),
    (5, 20, 1, 5), (6, 20, 1, 5)
) t(ticket, item_sk, date_sk, qty);
"""


def make_star_executor():
    env = Environment()
    env.parse(STAR_DDL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    for stmt in STAR_SEED.strip().split(";"):
        if stmt.strip():
            executor.execute_raw_sql(stmt)
    return executor


def rows(executor, query: str):
    return executor.execute_raw_sql(executor.generate_sql(query)[-1]).fetchall()


def test_grain_over_imported_keys_dedupes_without_filter():
    """Control: with no dimension-property filter the input IS normalized to the
    (item_sk, date_sk) tuple grain, so the count is the distinct-date count."""
    executor = make_star_executor()
    assert rows(
        executor,
        "select item_sk, count(grain(item_id, item_desc, date_sk)) as c "
        "order by item_sk asc;",
    ) == [(10, 2), (20, 1)]


@pytest.mark.xfail(
    strict=True,
    reason="OPEN: WHERE filter on a joined dimension property drops the "
    "composite-grain normalization; count(grain(...)) counts physical fact rows. "
    "See handoff_q23_grain_function_imported_keys_skip_input_normalization.md",
)
def test_grain_over_imported_keys_dedupes_under_dimension_filter():
    """A WHERE filter on a joined dimension property must not defeat grain()
    normalization: count(grain(...)) counts distinct declared-grain tuples, not
    physical fact rows. The bug returned (10, 4), (20, 2) — the raw ticket-line
    counts — because the composite-grain group was dropped once the fact joined
    the filtered date CTE."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 "
        "select item_sk, count(grain(item_id, item_desc, date_sk)) as c "
        "order by item_sk asc;",
    ) == [(10, 2), (20, 1)]


@pytest.mark.xfail(
    strict=True,
    reason="OPEN: same normalization drop as above, expressed as the q23 "
    "HAVING-in-rowset shape. See handoff_q23_grain_function_imported_keys_"
    "skip_input_normalization.md",
)
def test_grain_over_imported_keys_having_under_dimension_filter():
    """The q23 shape: the count feeds a HAVING inside a rowset. Only item 10
    exceeds one distinct date; counting physical rows wrongly admits item 20."""
    executor = make_star_executor()
    query = (
        "rowset frequent <- where yr = 2000 "
        "select item_sk, count(grain(item_id, item_desc, date_sk)) as c "
        "having c > 1;"
        "select frequent.item_sk order by frequent.item_sk asc;"
    )
    assert rows(executor, query) == [(10,)]
