"""A membership projected as a column, in a select that also aggregates.

The subselect feeder has to be wired onto the node that renders the comparison.
When an aggregate splits the plan, that node is a pre-group projection rather
than the final node, and the feeder used to land only on the latter.
"""

from trilogy import Dialects

_FIXTURE = """
key cust_id int;
property cust_id.zip string;
property cust_id.flag string;

datasource customers (
    cust_id: cust_id,
    zip: zip,
    flag: flag,
)
grain (cust_id)
query '''
select 1 as cust_id, '1' as zip, 'Y' as flag union all
select 2 as cust_id, '2' as zip, 'N' as flag union all
select 3 as cust_id, '3' as zip, 'Y' as flag union all
select 4 as cust_id, '1' as zip, 'N' as flag
''';

key store_id int;
property store_id.store_zip string;

datasource stores (
    store_id: store_id,
    store_zip: store_zip,
)
grain (store_id)
query '''
select 10 as store_id, '1' as store_zip union all
select 11 as store_id, '9' as store_zip
''';

const zips <- '1,2';
"""


def _exec():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    return executor


def _rows(query: str):
    return [tuple(r) for r in _exec().execute_text(query)[0].fetchall()]


def test_projected_split_membership_beside_an_aggregate():
    assert _rows("""
select
    zip,
    count(cust_id ? flag = 'Y') as n,
    zip in split(zips, ',') as in_param
order by zip asc;
""") == [("1", 1, True), ("2", 0, True), ("3", 1, False)]


def test_projected_split_membership_negated_beside_an_aggregate():
    assert _rows("""
select
    zip,
    count(cust_id) as n,
    zip not in split(zips, ',') as out_param
order by zip asc;
""") == [("1", 2, False), ("2", 1, False), ("3", 1, True)]


def test_projected_concept_membership_beside_an_aggregate():
    assert _rows("""
select
    zip,
    count(cust_id) as n,
    zip in store_zip as in_store
order by zip asc;
""") == [("1", 2, True), ("2", 1, False), ("3", 1, False)]


def test_projected_membership_beside_two_aggregates():
    assert _rows("""
select
    zip,
    count(cust_id) as n,
    max(flag) as mf,
    zip in split(zips, ',') as in_param
order by zip asc;
""") == [("1", 2, "Y", True), ("2", 1, "N", True), ("3", 1, "Y", False)]


def test_projected_membership_beside_an_aggregate_with_having():
    assert _rows("""
select
    zip,
    count(cust_id) as n,
    zip in split(zips, ',') as in_param
having n > 1
order by zip asc;
""") == [("1", 2, True)]


def test_projected_membership_beside_an_aggregate_inside_a_rowset():
    assert _rows("""
with r as
select
    zip,
    count(cust_id) as n,
    zip in split(zips, ',') as in_param;

select r.zip, r.n, r.in_param
order by r.zip asc;
""") == [("1", 2, True), ("2", 1, True), ("3", 1, False)]


def test_projected_membership_without_an_aggregate_is_unchanged():
    assert _rows("""
select zip, zip in split(zips, ',') as in_param
order by zip asc;
""") == [("1", True), ("2", True), ("3", False)]
