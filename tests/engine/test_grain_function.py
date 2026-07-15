from trilogy import Dialects, Environment
from trilogy.core.processing.node_generators.group_node import get_aggregate_grain

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


def test_grain_over_imported_keys_dedupes_under_dimension_filter():
    """A WHERE filter on a joined dimension property must not defeat grain()
    normalization: count(grain(...)) counts distinct declared-grain tuples, not
    physical fact rows. Regression: the filter forced the fact to join the
    filtered date CTE, and the fact's composite-grain group was deferred past
    that merge and then silently dropped, so the count ran over ticket lines
    (10 -> 4, 20 -> 2)."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 "
        "select item_sk, count(grain(item_id, item_desc, date_sk)) as c "
        "order by item_sk asc;",
    ) == [(10, 2), (20, 1)]


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


def test_grain_aggregate_input_grain_carries_composite_keys():
    """Non-result guard: the dedup depends on the aggregate's INPUT grain, so
    assert it at the model level rather than only through a row count. The
    count's argument tuple must resolve to the composite (item_sk, date_sk)
    grain — item_id/item_desc are 1:1 properties of item_sk — independent of any
    WHERE or sourcing. If this drifts, every count(grain(...)) silently
    mis-dedupes."""
    env = Environment()
    env.parse(STAR_DDL)
    env.parse("select item_sk, count(grain(item_id, item_desc, date_sk)) as c;")
    benv = env.materialize_for_select()
    c = benv.concepts["local.c"]
    assert c.is_aggregate
    assert set(get_aggregate_grain(c, benv).components) == {
        "local.item_sk",
        "local.date_sk",
    }


def test_grain_under_dimension_filter_normalizes_before_count_in_sql():
    """Non-result guard on the generated plan: a normalization GROUP BY at the
    composite grain must exist BEFORE the outer count, else duplicate fact rows
    inflate it. Asserts the date key is grouped alongside the item key (so the
    tuple is deduped) rather than the count running straight over the fact
    scan."""
    executor = make_star_executor()
    sql = executor.generate_sql(
        "where yr = 2000 "
        "select item_sk, count(grain(item_id, item_desc, date_sk)) as c;"
    )[-1].lower()
    normalize_cte, _, count_cte = sql.partition("count(md5")
    assert "group by" in normalize_cte
    assert "date_sk" in normalize_cte and "item_sk" in normalize_cte


def test_grain_count_equals_count_distinct_under_dimension_filter():
    """grain()'s hash is injective+total, so count(grain(...)) must equal
    count_distinct(grain(...)) even once the dimension filter forces the join
    that used to defeat normalization."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 select item_sk, "
        "count(grain(item_id, item_desc, date_sk)) as c, "
        "count_distinct(grain(item_id, item_desc, date_sk)) as cd order by item_sk;",
    ) == [(10, 2, 2), (20, 1, 1)]


def test_grain_inline_derived_expression_under_dimension_filter():
    """The handoff's exact shape: a derived expression (substring) inline in
    grain(), not a pre-bound property. Its row identity is still item_sk, so the
    tuple grain stays (item_sk, date_sk) and the count dedupes to distinct
    dates."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 select item_sk, "
        "count(grain(item_id, substring(item_desc, 1, 30), date_sk)) as c "
        "order by item_sk asc;",
    ) == [(10, 2), (20, 1)]


def test_grain_count_alongside_ordinary_aggregate_of_different_grain():
    """Regression guard for the regroup: a co-located sum(qty) needs raw
    ticket-line rows while count(grain(...)) needs the (item_sk, date_sk) tuple.
    Forcing the merge regroup must not collapse the fact rows the sum depends on
    — item 10 keeps its 4-line qty sum (20) while the count still dedupes to 2
    dates."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 select item_sk, sum(qty) as q, "
        "count(grain(item_id, item_desc, date_sk)) as c order by item_sk asc;",
    ) == [(10, 20, 2), (20, 10, 1)]


def test_grain_normalizes_under_fact_local_filter():
    """A fact-local filter (no dimension join) takes a different sourcing path;
    the tuple must still dedupe to distinct dates per item."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where qty > 0 select item_sk, "
        "count(grain(item_id, item_desc, date_sk)) as c order by item_sk asc;",
    ) == [(10, 2), (20, 1)]


def test_grain_global_count_under_dimension_filter():
    """`by *` global aggregate over grain(): the whole (item, date) tuple
    population deduped across all items — 3 distinct pairs — even under the
    dimension filter."""
    executor = make_star_executor()
    assert rows(
        executor,
        "where yr = 2000 select count(grain(item_id, item_desc, date_sk)) as c;",
    ) == [(3,)]
