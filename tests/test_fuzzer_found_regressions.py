from trilogy import Dialects

MODEL = """
key group_id int;

key event_id int;
property event_id.event_amount int;
property event_id.active bool;
datasource events (
    eid: event_id,
    gid: group_id,
    amount: event_amount,
    active: active
)
grain (event_id)
query '''
select 1 as eid, 1 as gid, 0 as amount, false as active
union all select 2, 1, -2, true
union all select 3, 2, 6, true
union all select 4, 2, 6, true
union all select 5, 3, 9, false
union all select 6, 3, 1, true
union all select 7, 4, 12, false
''';
"""


def _run(query: str) -> tuple[list[tuple], str]:
    executor = Dialects.DUCK_DB.default_executor()
    try:
        sql = executor.generate_sql(MODEL + query)[-1]
        rows = [tuple(row) for row in executor.execute_raw_sql(sql).fetchall()]
        return rows, sql
    finally:
        executor.close()


def test_three_way_union_join_coalesces_middle_only_key() -> None:
    rows, sql = _run("""
rowset bucket_zero <- where event_id % 3 = 0
select group_id as gid, sum(event_amount) as total_zero;
rowset bucket_one <- where event_id % 3 = 1
select group_id as gid, sum(event_amount) as total_one;
rowset bucket_two <- where event_id % 3 = 2
select group_id as gid, sum(event_amount) as total_two;

select
    bucket_zero.gid,
    bucket_zero.total_zero,
    bucket_one.total_one,
    bucket_two.total_two
union join bucket_zero.gid = bucket_one.gid = bucket_two.gid
order by bucket_zero.gid asc;
""")

    assert rows == [
        (1, None, 0, -2),
        (2, 6, 6, None),
        (3, 1, None, 9),
        (4, None, 12, None),
    ]
    assert "bucket_one_gid" in sql


def test_offgrain_having_keeps_null_window_output() -> None:
    rows, _ = _run("""
rowset grouped <- select
    group_id as gid,
    active as flag,
    sum(event_amount) as total;

select
    grouped.gid,
    grouped.total - lag(grouped.total, 1) over (
        partition by grouped.gid
        order by grouped.flag asc
    ) as change
having grouped.flag = true
order by grouped.gid asc;
""")

    assert rows == [(1, -2), (2, None), (3, -8)]


def test_rollup_grouping_window_keeps_grand_total_rank() -> None:
    rows, sql = _run("""
select
    group_id,
    sum(event_amount) as total,
    grouping(group_id) as grouping_level,
    rank() over (
        partition by grouping(group_id)
        order by sum(event_amount) desc
    ) as level_rank
by rollup (group_id)
order by grouping_level asc, level_rank asc, group_id asc nulls last;
""")

    assert rows == [
        (2, 12, 0, 1),
        (4, 12, 0, 1),
        (3, 10, 0, 3),
        (1, -2, 0, 4),
        (None, 32, 1, 1),
    ]
    # One grouping pass, no stitch join back onto the rollup (which previously
    # had to be null-safe to keep the grand-total row).
    assert sql.upper().count("ROLLUP") == 1
    assert " JOIN " not in sql.upper()


def test_named_grouping_partition_ranks_rollup_subtotals() -> None:
    rows, _ = _run("""
auto partition_label <- case
    when grouping(group_id) = 1 then '~TOTAL~'
    else cast(group_id as string)
end;
auto named_rank <- rank(active) over (
    partition by partition_label
    order by sum(event_amount) desc
);

select
    group_id,
    active,
    sum(event_amount) as total,
    named_rank as ranked
by rollup (group_id, active)
order by group_id asc nulls last, active asc nulls last;
""")

    assert rows == [
        (1, False, 0, 1),
        (1, True, -2, 2),
        (1, None, -2, 2),
        (2, True, 12, 1),
        (2, None, 12, 1),
        (3, False, 9, 2),
        (3, True, 1, 3),
        (3, None, 10, 1),
        (4, False, 12, 1),
        (4, None, 12, 1),
        (None, None, 32, 1),
    ]
