from __future__ import annotations

from collections.abc import Iterable

from local_scripts.fuzzer.models import SEEDS, FuzzCase, SeedData, indent


def _case(
    seed: SeedData,
    family: str,
    name: str,
    description: str,
    tags: Iterable[str],
    body: str,
    oracle_body: str,
) -> FuzzCase:
    case_id = f"{seed.name}__{family}__{name}"
    oracle = f"with {seed.oracle_ctes()}\n{oracle_body.strip()}"
    return FuzzCase(
        case_id=case_id,
        seed=seed.name,
        family=family,
        description=description,
        tags=tuple(tags),
        trilogy=seed.trilogy_model() + body.strip() + "\n",
        oracle_sql=oracle,
    )


def _scalar_cases(seed: SeedData) -> list[FuzzCase]:
    filters = (
        (
            "nullable_is_null",
            "nullable_name is null",
            "nullable_name is null",
            ("nullable", "where"),
        ),
        (
            "nullable_is_not_null",
            "nullable_name is not null",
            "nullable_name is not null",
            ("nullable", "not", "where"),
        ),
        (
            "boolean_not",
            "not active",
            "not active",
            ("boolean", "not", "where"),
        ),
    )
    cases = []
    for name, trilogy_filter, sql_filter, tags in filters:
        if name.startswith("nullable"):
            body = f"""
where {trilogy_filter}
select
    group_id,
    coalesce(nullable_name, required_name) as display_name
order by group_id asc;
"""
            oracle = f"""
select gid, coalesce(nullable_name, required_name)
from groups
where {sql_filter}
order by gid
"""
        else:
            body = f"""
where {trilogy_filter}
select event_id, group_id, event_amount
order by event_id asc;
"""
            oracle = f"""
select eid, gid, amount
from events
where {sql_filter}
order by eid
"""
        cases.append(
            _case(
                seed,
                "scalar",
                name,
                f"Scalar projection filtered by `{trilogy_filter}`.",
                tags,
                body,
                oracle,
            )
        )
    return cases


def _aggregate_cases(seed: SeedData) -> list[FuzzCase]:
    predicates = (
        ("all", "event_id is not null", "eid is not null", ("aggregate",)),
        (
            "nullable_is_null",
            "nullable_amount is null",
            "nullable_amount is null",
            ("aggregate", "nullable", "where"),
        ),
        (
            "nullable_is_not_null",
            "nullable_amount is not null",
            "nullable_amount is not null",
            ("aggregate", "nullable", "not", "where"),
        ),
        ("not_active", "not active", "not active", ("aggregate", "not", "where")),
    )
    cases = []
    for name, trilogy_filter, sql_filter, tags in predicates:
        body = f"""
where {trilogy_filter}
select
    group_id,
    sum(event_amount) as total,
    count(event_id) as event_count
having sum(event_amount) > 2
order by group_id asc;
"""
        oracle = f"""
select gid, sum(amount), count(eid)
from events
where {sql_filter}
group by gid
having sum(amount) > 2
order by gid
"""
        cases.append(
            _case(
                seed,
                "aggregate",
                name,
                "Grouped SUM/COUNT with composed WHERE and HAVING filters.",
                (*tags, "having"),
                body,
                oracle,
            )
        )
    return cases


def _function_cases(seed: SeedData) -> list[FuzzCase]:
    statistics = _case(
        seed,
        "function",
        "grouped_statistics",
        "MIN/MAX/AVG/COUNT compose at a shared grouping grain.",
        ("function", "aggregate", "having", "min", "max", "avg"),
        """
select
    group_id,
    min(event_amount) as minimum_amount,
    max(event_amount) as maximum_amount,
    avg(event_amount) as average_amount,
    count(event_id) as event_count
having count(event_id) > 1
order by group_id asc;
""",
        """
select gid, min(amount), max(amount), avg(amount), count(eid)
from events
group by gid
having count(eid) > 1
order by gid
""",
    )
    filtered = _case(
        seed,
        "function",
        "filtered_aggregates",
        "Complementary predicates filter aggregates without filtering group rows.",
        ("function", "aggregate", "filter", "not", "nullable"),
        """
select
    group_id,
    sum(event_amount ? active) as active_total,
    sum(event_amount ? not active) as inactive_total,
    count(event_id ? nullable_amount is null) as null_count
order by group_id asc;
""",
        """
select
    gid,
    sum(amount) filter (where active),
    sum(amount) filter (where not active),
    count(eid) filter (where nullable_amount is null)
from events
group by gid
order by gid
""",
    )
    scalar = _case(
        seed,
        "function",
        "scalar_case_abs",
        "CASE, ABS, and COALESCE operate over nullable scalar inputs.",
        ("function", "scalar", "case", "nullable", "coalesce"),
        """
select
    event_id,
    abs(event_amount) as absolute_amount,
    case
        when nullable_amount is null then 0
        else nullable_amount
    end as safe_amount
order by event_id asc;
""",
        """
select
    eid,
    abs(amount),
    case when nullable_amount is null then 0 else nullable_amount end
from events
order by eid
""",
    )
    return [statistics, filtered, scalar]


def _rowset_cases(seed: SeedData) -> list[FuzzCase]:
    cases = []
    variants = (
        (
            "where_then_having",
            "nullable_amount is not null",
            "nullable_amount is not null",
            3,
        ),
        ("not_then_having", "not active", "not active", 2),
    )
    for name, trilogy_filter, sql_filter, threshold in variants:
        body = f"""
rowset grouped <- where {trilogy_filter}
select group_id as gid, sum(event_amount) as total
having sum(event_amount) > {threshold};

where grouped.total is not null
select grouped.gid, grouped.total
order by grouped.gid asc;
"""
        oracle = f"""
select gid, total
from (
    select gid, sum(amount) as total
    from events
    where {sql_filter}
    group by gid
    having sum(amount) > {threshold}
) grouped
where total is not null
order by gid
"""
        cases.append(
            _case(
                seed,
                "rowset",
                name,
                "Filtered aggregate rowset consumed by an outer WHERE.",
                ("rowset", "aggregate", "where", "having"),
                body,
                oracle,
            )
        )

    body = """
rowset grouped <- select group_id as gid, sum(event_amount) as total;
rowset grand <- select sum(grouped.total) as grand_total;
select grand.grand_total;
"""
    oracle = """
select sum(total)
from (
    select gid, sum(amount) as total
    from events
    group by gid
) grouped
"""
    cases.append(
        _case(
            seed,
            "rowset",
            "nested_global_aggregate",
            "A second rowset globally aggregates the first rowset.",
            ("rowset", "nested", "aggregate"),
            body,
            oracle,
        )
    )
    cross_body = """
rowset active_groups <- where active
select group_id as gid, sum(event_amount) as active_total;
rowset inactive_groups <- where not active
select group_id as gid, sum(event_amount) as inactive_total;

where active_groups.active_total is not null
    and inactive_groups.inactive_total is not null
select
    active_groups.gid,
    active_groups.active_total,
    inactive_groups.inactive_total
union join active_groups.gid = inactive_groups.gid
order by active_groups.gid asc;
"""
    cross_oracle = """
select a.gid, a.total, i.total
from (
    select gid, sum(amount) as total
    from events
    where active
    group by gid
) a
join (
    select gid, sum(amount) as total
    from events
    where not active
    group by gid
) i on a.gid = i.gid
order by a.gid
"""
    cases.append(
        _case(
            seed,
            "rowset",
            "cross_rowset_intersection",
            "Two filtered aggregate rowsets are union-related then intersected.",
            ("rowset", "aggregate", "union", "where", "not"),
            cross_body,
            cross_oracle,
        )
    )
    return cases


def _window_cases(seed: SeedData) -> list[FuzzCase]:
    lag_body = """
rowset grouped <- select group_id as gid, sum(event_amount) as total;
select
    grouped.gid,
    grouped.total,
    lag(grouped.total, 1) over (order by grouped.gid asc) as previous_total
order by grouped.gid asc;
"""
    lag_oracle = """
select gid, total, lag(total, 1) over (order by gid)
from (
    select gid, sum(amount) as total
    from events
    group by gid
) grouped
order by gid
"""
    rank_body = """
select
    group_id,
    sum(event_amount) as total,
    rank() over (order by sum(event_amount) desc) as total_rank
having rank() over (order by sum(event_amount) desc) <= 2
order by group_id asc;
"""
    rank_oracle = """
select gid, total, total_rank
from (
    select
        gid,
        sum(amount) as total,
        rank() over (order by sum(amount) desc) as total_rank
    from events
    group by gid
) ranked
where total_rank <= 2
order by gid
"""
    running_body = """
rowset grouped <- select group_id as gid, sum(event_amount) as total;
select
    grouped.gid,
    grouped.total,
    sum(grouped.total) over (order by grouped.gid asc) as running_total
order by grouped.gid asc;
"""
    running_oracle = """
select gid, total, sum(total) over (order by gid)
from (
    select gid, sum(amount) as total
    from events
    group by gid
) grouped
order by gid
"""
    row_number_body = """
select
    event_id,
    group_id,
    event_amount,
    row_number() over (
        partition by group_id
        order by event_amount desc, event_id asc
    ) as group_row
order by event_id asc;
"""
    row_number_oracle = """
select
    eid,
    gid,
    amount,
    row_number() over (partition by gid order by amount desc, eid asc)
from events
order by eid
"""
    return [
        _case(
            seed,
            "window",
            "lag_over_rowset",
            "LAG consumes an aggregate rowset.",
            ("window", "rowset", "aggregate", "lag"),
            lag_body,
            lag_oracle,
        ),
        _case(
            seed,
            "window",
            "rank_in_having",
            "RANK over an aggregate is repeated in HAVING.",
            ("window", "aggregate", "having", "rank"),
            rank_body,
            rank_oracle,
        ),
        _case(
            seed,
            "window",
            "running_sum_over_rowset",
            "A cumulative SUM window consumes aggregate rowset outputs.",
            ("window", "rowset", "aggregate", "sum"),
            running_body,
            running_oracle,
        ),
        _case(
            seed,
            "window",
            "partitioned_row_number",
            "ROW_NUMBER partitions raw rows and uses a deterministic tie-breaker.",
            ("window", "row_number", "partition"),
            row_number_body,
            row_number_oracle,
        ),
    ]


def _union_cases(seed: SeedData) -> list[FuzzCase]:
    variants = (
        (
            "boolean_partition",
            "where active select group_id as gid, event_amount as value",
            "where not active select group_id as gid, event_amount as value",
            "select distinct gid, amount as value from events where active",
            "select distinct gid, amount as value from events where not active",
        ),
        (
            "nullable_partition",
            (
                "where nullable_amount is null "
                "select group_id as gid, event_amount as value"
            ),
            (
                "where nullable_amount is not null "
                "select group_id as gid, nullable_amount as value"
            ),
            (
                "select distinct gid, amount as value from events "
                "where nullable_amount is null"
            ),
            (
                "select distinct gid, nullable_amount as value from events "
                "where nullable_amount is not null"
            ),
        ),
    )
    cases = []
    for name, trilogy_a, trilogy_b, sql_a, sql_b in variants:
        body = f"""
with combined as union(
    ({trilogy_a}),
    ({trilogy_b})
) -> (gid, value);

select combined.gid, sum(combined.value) as total
having sum(combined.value) > 2
order by combined.gid asc;
"""
        oracle = f"""
select gid, sum(value)
from (
{indent(sql_a, 4)}
    union all
{indent(sql_b, 4)}
) combined
group by gid
having sum(value) > 2
order by gid
"""
        cases.append(
            _case(
                seed,
                "union",
                name,
                "UNION ALL row stack partitioned by a complementary predicate.",
                ("union", "aggregate", "having", "nullable", "not"),
                body,
                oracle,
            )
        )
    nested_body = """
with combined as union(
    (
        where active
        select event_id as eid, group_id as gid, event_amount as value
    ),
    (
        where not active
        select event_id as eid, group_id as gid, event_amount as value
    )
) -> (eid, gid, value);

rowset rolled <- select combined.gid as gid, sum(combined.value) as total;
select rolled.gid, rolled.total
order by rolled.gid asc;
"""
    nested_oracle = """
select gid, sum(value)
from (
    select eid, gid, amount as value from events where active
    union all
    select eid, gid, amount as value from events where not active
) combined
group by gid
order by gid
"""
    cases.append(
        _case(
            seed,
            "union",
            "nested_rowset_consumer",
            "A union preserving row IDs feeds a grouped rowset consumer.",
            ("union", "rowset", "nested", "aggregate", "not"),
            nested_body,
            nested_oracle,
        )
    )
    return cases


def _membership_cases(seed: SeedData) -> list[FuzzCase]:
    dimension_body = """
rowset named_groups <- where nullable_name is not null
select group_id as gid;

where group_id in named_groups.gid
select group_id, sum(event_amount) as total
order by group_id asc;
"""
    dimension_oracle = """
select gid, sum(amount)
from events
where gid in (
    select gid
    from groups
    where nullable_name is not null
)
group by gid
order by gid
"""
    activity_body = """
rowset active_groups <- where active
select group_id as gid;

where group_id in active_groups.gid and not active
select group_id, sum(event_amount) as inactive_total
order by group_id asc;
"""
    activity_oracle = """
select gid, sum(amount)
from events
where not active
  and gid in (select distinct gid from events where active)
group by gid
order by gid
"""
    return [
        _case(
            seed,
            "membership",
            "dimension_keyset",
            "A dimension-derived nullable keyset filters an event aggregate.",
            ("membership", "rowset", "nullable", "where", "aggregate"),
            dimension_body,
            dimension_oracle,
        ),
        _case(
            seed,
            "membership",
            "complementary_event_keyset",
            "Inactive rows are retained only for groups also containing active rows.",
            ("membership", "rowset", "not", "where", "aggregate"),
            activity_body,
            activity_oracle,
        ),
    ]


def _grouping_cases(seed: SeedData) -> list[FuzzCase]:
    rollup_body = """
select group_id, sum(event_amount) as total
by rollup (group_id)
order by group_id asc nulls last;
"""
    rollup_oracle = """
select gid, sum(amount)
from events
group by rollup (gid)
order by gid asc nulls last
"""
    cube_body = """
select group_id, active, count(event_id) as event_count
by cube (group_id, active)
order by group_id asc nulls last, active asc nulls last;
"""
    cube_oracle = """
select gid, active, count(eid)
from events
group by cube (gid, active)
order by gid asc nulls last, active asc nulls last
"""
    return [
        _case(
            seed,
            "grouping",
            "single_rollup",
            "ROLLUP produces grouped rows and a grand total.",
            ("grouping", "rollup", "aggregate", "nullable"),
            rollup_body,
            rollup_oracle,
        ),
        _case(
            seed,
            "grouping",
            "two_axis_cube",
            "CUBE spans a key and boolean dimension.",
            ("grouping", "cube", "aggregate", "boolean", "nullable"),
            cube_body,
            cube_oracle,
        ),
    ]


def _join_cases(seed: SeedData) -> list[FuzzCase]:
    cases = []
    joins = (
        (
            "subset",
            "subset",
            "subset_facts",
            "subset_key",
            "subset_value",
            "left join",
        ),
        (
            "union",
            "union",
            "union_facts",
            "union_key",
            "union_value",
            "full join",
        ),
    )
    filters = (
        ("all", "", ""),
        (
            "matched",
            "where right_side.total is not null\n",
            "where r.total is not null",
        ),
        ("large_right", "where right_side.total > 30\n", "where r.total > 30"),
    )
    for join_name, keyword, table, key, value, sql_join in joins:
        for filter_name, trilogy_where, sql_where in filters:
            body = f"""
rowset left_side <- select left_key as k, sum(left_value) as total;
rowset right_side <- select {key} as k, sum({value}) as total;

{trilogy_where}select left_side.k, left_side.total, right_side.total
{keyword} join right_side.k = left_side.k
order by left_side.k asc nulls last;
"""
            key_projection = "l.k" if join_name == "subset" else "coalesce(l.k, r.k)"
            oracle = f"""
select {key_projection}, l.total, r.total
from (
    select k, sum(value) as total
    from left_facts
    group by k
) l
{sql_join} (
    select k, sum(value) as total
    from {table}
    group by k
) r on l.k is not distinct from r.k
{sql_where}
order by {key_projection} asc nulls last
"""
            cases.append(
                _case(
                    seed,
                    "join",
                    f"{join_name}_{filter_name}",
                    (
                        f"{join_name.upper()} domain join over aggregate rowsets "
                        f"with `{filter_name}` post-join filtering."
                    ),
                    (
                        "join",
                        join_name,
                        "rowset",
                        "aggregate",
                        "nullable",
                        "where" if trilogy_where else "unfiltered",
                    ),
                    body,
                    oracle,
                )
            )
    return cases


def _derived_join_cases(seed: SeedData) -> list[FuzzCase]:
    cases = []
    variants = (
        (
            "subset",
            "subset",
            "subset_key",
            "subset_value",
            "subset_facts",
            "left join",
        ),
        (
            "union",
            "union",
            "union_key",
            "union_value",
            "union_facts",
            "full join",
        ),
    )
    for name, keyword, key, value, table, sql_join in variants:
        body = f"""
auto shifted_left <- left_key + 17;
auto shifted_right <- {key} + 17;

select
    shifted_left,
    sum(left_value) as left_total,
    sum({value}) as right_total
{keyword} join shifted_right = shifted_left
order by shifted_left asc nulls last;
"""
        projection = "l.k" if name == "subset" else "coalesce(l.k, r.k)"
        oracle = f"""
select {projection}, l.total, r.total
from (
    select k + 17 as k, sum(value) as total
    from left_facts
    group by k + 17
) l
{sql_join} (
    select k + 17 as k, sum(value) as total
    from {table}
    group by k + 17
) r on l.k is not distinct from r.k
order by {projection} asc nulls last
"""
        cases.append(
            _case(
                seed,
                "derived_join",
                name,
                f"A {name} declaration relates nullable transformed root keys.",
                ("join", name, "derived", "aggregate", "nullable"),
                body,
                oracle,
            )
        )
    return cases


def _chasm_cases(seed: SeedData) -> list[FuzzCase]:
    direct_body = """
select
    group_id,
    sum(sale_amount) as sales_total,
    sum(return_amount) as returns_total
order by group_id asc;
"""
    direct_oracle = """
select s.gid, s.total, r.total
from (
    select gid, sum(amount) as total
    from sales
    group by gid
) s
join (
    select gid, sum(amount) as total
    from returns
    group by gid
) r on s.gid = r.gid
order by s.gid
"""
    dimension_body = """
select
    group_name,
    sum(sale_amount) as sales_total,
    sum(return_amount) as returns_total
order by group_name asc;
"""
    dimension_oracle = """
select g.name, s.total, r.total
from groups g
join (
    select gid, sum(amount) as total
    from sales
    group by gid
) s on g.gid = s.gid
join (
    select gid, sum(amount) as total
    from returns
    group by gid
) r on g.gid = r.gid
order by g.name
"""
    filtered_body = """
where group_id is not null
select
    group_id,
    sum(sale_amount) as sales_total,
    sum(return_amount) as returns_total
having coalesce(sum(sale_amount), 0) + coalesce(sum(return_amount), 0) > 10
order by group_id asc;
"""
    filtered_oracle = """
select gid, sales_total, returns_total
from (
    select s.gid, s.total as sales_total, r.total as returns_total
    from (
        select gid, sum(amount) as total
        from sales
        group by gid
    ) s
    join (
        select gid, sum(amount) as total
        from returns
        group by gid
    ) r on s.gid = r.gid
) totals
where gid is not null
  and coalesce(sales_total, 0) + coalesce(returns_total, 0) > 10
order by gid
"""
    return [
        _case(
            seed,
            "chasm",
            "direct_fact_aggregates",
            "Two many-side facts aggregate independently at a shared key.",
            ("chasm", "fanout", "aggregate"),
            direct_body,
            direct_oracle,
        ),
        _case(
            seed,
            "chasm",
            "dimension_projection",
            "A dimension labels the intersection of two independent fact aggregates.",
            ("chasm", "fanout", "aggregate", "dimension"),
            dimension_body,
            dimension_oracle,
        ),
        _case(
            seed,
            "chasm",
            "where_and_having",
            "A chasm result is restricted by both WHERE and aggregate HAVING.",
            ("chasm", "fanout", "aggregate", "where", "having"),
            filtered_body,
            filtered_oracle,
        ),
    ]


def generate_cases(seeds: Iterable[SeedData] = SEEDS) -> list[FuzzCase]:
    cases = []
    builders = (
        _scalar_cases,
        _aggregate_cases,
        _function_cases,
        _rowset_cases,
        _window_cases,
        _union_cases,
        _membership_cases,
        _grouping_cases,
        _join_cases,
        _derived_join_cases,
        _chasm_cases,
    )
    for seed in seeds:
        for builder in builders:
            cases.extend(builder(seed))
    return cases
