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
