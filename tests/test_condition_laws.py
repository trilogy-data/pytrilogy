from trilogy import Dialects

MODEL = """
key event_id int;
property event_id.group_id int;
property event_id.amount int;
property event_id.nullable_amount int?;
property event_id.active bool;
datasource events (eid: event_id, gid: group_id, amount: amount,
    nullable_amount: nullable_amount, active: active)
grain (event_id)
query '''
select 1 as eid, 1 as gid, 10 as amount, null as nullable_amount, true as active
union all select 2, 1, 5, 2, false
union all select 3, 2, 7, 7, true
union all select 4, 2, 3, null, false
union all select 5, 3, 11, 1, true
''';
"""


def _rows(query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    return sorted(tuple(row) for row in executor.execute_text(MODEL + query)[-1])


def test_compound_not_is_two_valued_like_atomic_not():
    # Event 4: `nullable_amount > 0` is UNKNOWN, `active` FALSE.
    select = " select event_id;"
    assert _rows("where not (nullable_amount > 0 or active)" + select) == [(4,)]
    assert _rows("where (not nullable_amount > 0) and (not active)" + select) == [(4,)]
    assert _rows("where not (nullable_amount > 0 and active)" + select) == [
        (1,),
        (2,),
        (4,),
    ]
    assert _rows("where (not nullable_amount > 0) or (not active)" + select) == [
        (1,),
        (2,),
        (4,),
    ]


def test_bare_boolean_conjunct_beside_filtered_aggregate_stays_a_row_filter():
    # The filtered aggregate's conjunction used to keep `active` unsplit and
    # route it to an invalid HAVING.
    query = """
where active and nullable_amount > 0
    and sum(amount ? active and nullable_amount > 0) by group_id > 0
select group_id, sum(amount) as t;
"""
    assert _rows(query) == [(2, 7), (3, 11)]


def test_named_where_beside_subset_joined_rowset_stays_below_the_aggregate():
    # A subset-joined rowset is a side input, not a row-shape barrier the
    # atom's rows cross: the gate used to be pushed past the aggregate.
    prefix = """
auto has_amount <- nullable_amount is not null;
rowset totals <- select group_id as tgid, sum(amount) as tt;
rowset joined <- where """
    suffix = """ and totals.tt > 0
select group_id as gid, sum(amount) as t
subset join totals.tgid = group_id;
select joined.gid, joined.t;
"""
    expected = [(1, 5), (2, 7), (3, 11)]
    assert _rows(prefix + "has_amount = true" + suffix) == expected
    assert _rows(prefix + "nullable_amount is not null" + suffix) == expected
