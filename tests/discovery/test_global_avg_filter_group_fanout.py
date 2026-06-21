from trilogy import Dialects, Environment

# regression (TPC-H q22 shape): a `by *` global-average aggregate compared in a
# WHERE clause, together with a second row predicate on a derived grouping key
# (`cntrycode in (...)`), used to fan out every aggregated output row. The
# group-by node enriched the filter's row-arguments (e.g. `acctbal`) back in at
# the finer pre-group grain and joined them on the grouping key, multiplying
# each already-aggregated row by its member count. The aggregate applies the
# filter below and reports it satisfied, so the filter-only args must not be
# re-sourced as outputs.
MODEL = """
key custkey int;
property custkey.phone string;
property custkey.acctbal float;

datasource customers (
    custkey: custkey,
    phone: phone,
    acctbal: acctbal,
)
grain (custkey)
query '''
select 1 as custkey, '13x' as phone, 500.0 as acctbal
union all select 2, '13x', 700.0
union all select 3, '17x', 800.0
union all select 4, '17x', 600.0
union all select 5, '31x', 100.0
union all select 6, '99x', 900.0
''';

auto cntrycode <- substring(phone, 1, 2);
auto avg_bal <- avg(acctbal ? acctbal > 0 and cntrycode in ('13','17','31')) by *;
"""

QUERY = MODEL + """
select
    cntrycode,
    count(custkey) as cust_count,
    sum(acctbal) as total_bal
where
    acctbal > 0
    and cntrycode in ('13','17','31')
    and acctbal > avg_bal
order by cntrycode asc;
"""


def test_global_avg_filter_does_not_fan_out_group():
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = [tuple(r) for r in exec.execute_query(QUERY).fetchall()]
    # avg over the filtered set (500,700,800,600,100) is 540; only 700/800/600
    # survive `acctbal > avg_bal`. The '17' group has two members and would be
    # duplicated by the fan-out join.
    assert rows == [("13", 1, 700.0), ("17", 2, 1400.0)]
