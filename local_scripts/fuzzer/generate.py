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
    normalized_tags = tuple(tags)
    if "where " in body.lower() and "where" not in normalized_tags:
        normalized_tags = (*normalized_tags, "where")
    return FuzzCase(
        case_id=case_id,
        seed=seed.name,
        family=family,
        description=description,
        tags=normalized_tags,
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


def _complex_where_cases(seed: SeedData) -> list[FuzzCase]:
    nested_boolean = _case(
        seed,
        "where_complex",
        "nested_boolean_nulls",
        "Nested AND/OR branches combine nullable and boolean predicates.",
        ("where", "nested", "and", "or", "not", "nullable", "boolean"),
        """
where (active and nullable_amount is null)
    or (not active and (event_amount < 0 or event_amount >= 10))
select event_id, event_amount, nullable_amount, active
order by event_id asc;
""",
        """
select eid, amount, nullable_amount, active
from events
where (active and nullable_amount is null)
   or (not active and (amount < 0 or amount >= 10))
order by eid
""",
    )
    compound_not = _case(
        seed,
        "where_complex",
        "compound_not_parentheses",
        "NOT wraps a parenthesized nullable boolean expression.",
        ("where", "nested", "not", "or", "nullable", "boolean"),
        """
where not (active or nullable_amount is null)
select event_id, nullable_amount, active
order by event_id asc;
""",
        """
select eid, nullable_amount, active
from events
where not (active or nullable_amount is null)
order by eid
""",
    )
    derived_between = _case(
        seed,
        "where_complex",
        "derived_between",
        "BETWEEN filters a nested ABS/COALESCE expression.",
        ("where", "between", "function", "coalesce", "nullable", "derived"),
        """
where abs(coalesce(nullable_amount, event_amount)) between 2 and 10
select event_id, event_amount, nullable_amount
order by event_id asc;
""",
        """
select eid, amount, nullable_amount
from events
where abs(coalesce(nullable_amount, amount)) between 2 and 10
order by eid
""",
    )
    case_comparison = _case(
        seed,
        "where_complex",
        "case_comparison",
        "An inline CASE expression feeds a numeric WHERE comparison.",
        ("where", "case", "nullable", "derived", "comparison"),
        """
where case
    when nullable_amount is null then event_amount
    else nullable_amount
end >= 5
select event_id, event_amount, nullable_amount
order by event_id asc;
""",
        """
select eid, amount, nullable_amount
from events
where case
    when nullable_amount is null then amount
    else nullable_amount
end >= 5
order by eid
""",
    )
    string_predicate = _case(
        seed,
        "where_complex",
        "string_like_or_null",
        "LIKE and string inequality sit on separate nullable OR branches.",
        ("where", "like", "string", "or", "and", "nullable"),
        """
where required_name like 'A%'
    or (nullable_name is not null and group_name != 'alpha')
select group_id, group_name, nullable_name, required_name
order by group_id asc;
""",
        """
select gid, name, nullable_name, required_name
from groups
where required_name like 'A%'
   or (nullable_name is not null and name != 'alpha')
order by gid
""",
    )
    membership_nested = _case(
        seed,
        "where_complex",
        "membership_nested_boolean",
        "Rowset membership participates in a nested boolean predicate.",
        ("where", "membership", "rowset", "nested", "and", "or", "not"),
        """
rowset active_groups <- where active
select group_id as gid;

where (group_id in active_groups.gid and not active)
    or (event_amount < 0 and active)
select event_id, group_id, event_amount, active
order by event_id asc;
""",
        """
select eid, gid, amount, active
from events e
where (
    gid in (select distinct gid from events where active)
    and not active
) or (amount < 0 and active)
order by eid
""",
    )
    not_in_keyset = _case(
        seed,
        "where_complex",
        "not_in_keyset",
        "NOT IN composes with a boolean predicate over a rowset keyset.",
        ("where", "membership", "not_in", "rowset", "and", "boolean"),
        """
rowset negative_groups <- where event_amount < 0
select group_id as gid;

where group_id not in negative_groups.gid and active
select event_id, group_id, event_amount
order by event_id asc;
""",
        """
select eid, gid, amount
from events
where gid not in (
    select distinct gid from events where amount < 0
) and active
order by eid
""",
    )
    post_join = _case(
        seed,
        "where_complex",
        "post_join_mixed_sides",
        "A nested post-join predicate references nullable measures from both sides.",
        (
            "where",
            "join",
            "union",
            "rowset",
            "independent_sources",
            "nested",
            "or",
            "and",
            "nullable",
        ),
        """
rowset left_grouped <- where left_value <= 8
select left_key as k, sum(left_value) as left_total;
rowset right_grouped <- where union_value <= 80
select union_key as k, sum(union_value) as right_total;

where (left_grouped.left_total is null or right_grouped.right_total is null)
    or (
        left_grouped.k is not null
        and left_grouped.left_total < right_grouped.right_total
    )
select
    left_grouped.k,
    left_grouped.left_total,
    right_grouped.right_total
union join left_grouped.k = right_grouped.k
order by left_grouped.k asc nulls last;
""",
        """
select coalesce(l.k, r.k), l.total, r.total
from (
    select k, sum(value) as total
    from left_facts
    where value <= 8
    group by k
) l
full join (
    select k, sum(value) as total
    from union_facts
    where value <= 80
    group by k
) r on l.k is not distinct from r.k
where (l.total is null or r.total is null)
   or (l.k is not null and l.total < r.total)
order by coalesce(l.k, r.k) asc nulls last
""",
    )
    aggregate_comparison = _case(
        seed,
        "where_complex",
        "cross_rowset_aggregate_comparison",
        "A post-join WHERE compares aggregates from complementary rowsets.",
        (
            "where",
            "join",
            "union",
            "rowset",
            "aggregate",
            "cross_rowset",
            "coalesce",
            "or",
        ),
        """
rowset active_groups <- where active
select group_id as gid, sum(event_amount) as active_total;
rowset inactive_groups <- where not active
select group_id as gid, sum(event_amount) as inactive_total;

where active_groups.active_total > coalesce(inactive_groups.inactive_total, 0)
    or (
        active_groups.active_total is null
        and inactive_groups.inactive_total < 0
    )
select
    active_groups.gid,
    active_groups.active_total,
    inactive_groups.inactive_total
union join active_groups.gid = inactive_groups.gid
order by active_groups.gid asc;
""",
        """
select coalesce(a.gid, i.gid), a.total, i.total
from (
    select gid, sum(amount) as total
    from events
    where active
    group by gid
) a
full join (
    select gid, sum(amount) as total
    from events
    where not active
    group by gid
) i on a.gid = i.gid
where a.total > coalesce(i.total, 0)
   or (a.total is null and i.total < 0)
order by coalesce(a.gid, i.gid)
""",
    )
    window_boundary = _case(
        seed,
        "where_complex",
        "window_boundary_boolean",
        "A wrapping WHERE applies nested nullable logic after LEAD evaluation.",
        ("where", "window", "rowset", "lead", "nested", "or", "and", "not"),
        """
rowset windowed <- select
    event_id as eid,
    event_amount as amount,
    nullable_amount as optional,
    lead(event_amount, 1) over (order by event_id asc) as next_amount;

where (
    windowed.next_amount is null
    or windowed.next_amount > windowed.amount
) and not (
    windowed.optional is null
    and windowed.amount < 0
)
select
    windowed.eid,
    windowed.amount,
    windowed.optional,
    windowed.next_amount
order by windowed.eid asc;
""",
        """
select eid, amount, nullable_amount, next_amount
from (
    select
        eid,
        amount,
        nullable_amount,
        lead(amount, 1) over (order by eid) as next_amount
    from events
) windowed
where (next_amount is null or next_amount > amount)
  and not (nullable_amount is null and amount < 0)
order by eid
""",
    )
    chasm_predicate = _case(
        seed,
        "where_complex",
        "chasm_mixed_measure_predicate",
        "A nested WHERE compares measures from two fanout-prone fact domains.",
        (
            "where",
            "chasm",
            "fanout",
            "join",
            "union",
            "rowset",
            "nested",
            "coalesce",
        ),
        """
rowset sale_groups <- select
    group_id as gid,
    sum(sale_amount) as sale_total;
rowset return_groups <- select
    group_id as gid,
    sum(return_amount) as return_total;

where (
    sale_groups.sale_total > coalesce(return_groups.return_total, 0)
    and return_groups.return_total is not null
) or sale_groups.sale_total is null
select
    sale_groups.gid,
    sale_groups.sale_total,
    return_groups.return_total
union join sale_groups.gid = return_groups.gid
order by sale_groups.gid asc;
""",
        # sales/returns bind group_id partially (`~`); groups is the complete
        # domain source, so each rowset aggregates over the FULL group domain
        # (factless groups carry NULL totals) before the union join relates them.
        """
select g.gid, s.total, r.total
from groups g
left join (
    select gid, sum(amount) as total
    from sales
    group by gid
) s on g.gid = s.gid
left join (
    select gid, sum(amount) as total
    from returns
    group by gid
) r on g.gid = r.gid
where (
    s.total > coalesce(r.total, 0)
    and r.total is not null
) or s.total is null
order by g.gid
""",
    )
    return [
        nested_boolean,
        compound_not,
        derived_between,
        case_comparison,
        string_predicate,
        membership_nested,
        not_in_keyset,
        post_join,
        aggregate_comparison,
        window_boundary,
        chasm_predicate,
    ]


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


def _having_cases(seed: SeedData) -> list[FuzzCase]:
    hidden_body = """
rowset repeated_groups <- select
    group_id as gid,
    sum(event_amount) as total
having count(event_id) > 1;

select repeated_groups.gid, repeated_groups.total
order by repeated_groups.gid asc;
"""
    hidden_oracle = """
select gid, sum(amount)
from events
group by gid
having count(eid) > 1
order by gid
"""
    comparison_body = """
select
    group_id,
    sum(event_amount) as total
having sum(event_amount) > count(event_id) * 3
order by group_id asc;
"""
    comparison_oracle = """
select gid, sum(amount)
from events
group by gid
having sum(amount) > count(eid) * 3
order by gid
"""
    parent_total_body = """
select
    group_id,
    active,
    sum(event_amount) as total,
    sum(event_amount) by group_id as group_total
having sum(event_amount) < 0.6 * sum(event_amount) by group_id
order by group_id asc, active asc;
"""
    parent_total_oracle = """
select g.gid, g.active, g.total, t.group_total
from (
    select gid, active, sum(amount) as total
    from events
    group by gid, active
) g
join (
    select gid, sum(amount) as group_total
    from events
    group by gid
) t on g.gid = t.gid
where g.total < 0.6 * t.group_total
order by g.gid, g.active
"""
    nested_avg_body = """
select
    group_id,
    active,
    sum(event_amount) as total,
    avg(sum(event_amount) by group_id, active) by active as active_average
having sum(event_amount)
    > avg(sum(event_amount) by group_id, active) by active
order by group_id asc, active asc;
"""
    nested_avg_oracle = """
select gid, active, total, active_average
from (
    select
        gid,
        active,
        total,
        avg(total) over (partition by active) as active_average
    from (
        select gid, active, sum(amount) as total
        from events
        group by gid, active
    ) grouped
) compared
where total > active_average
order by gid, active
"""
    return [
        _case(
            seed,
            "having",
            "nonprojected_count",
            "HAVING promotes a COUNT that is absent from rowset outputs.",
            ("having", "rowset", "aggregate", "hidden"),
            hidden_body,
            hidden_oracle,
        ),
        _case(
            seed,
            "having",
            "aggregate_comparison",
            "HAVING compares independently rendered SUM and COUNT aggregates.",
            ("having", "aggregate", "comparison"),
            comparison_body,
            comparison_oracle,
        ),
        _case(
            seed,
            "having",
            "parent_grain_total",
            "A detail aggregate is compared with its parent-grain total.",
            ("having", "aggregate", "nested", "parent_grain"),
            parent_total_body,
            parent_total_oracle,
        ),
        _case(
            seed,
            "having",
            "nested_average",
            "A grouped total is compared with an average of grouped totals.",
            ("having", "aggregate", "nested", "avg"),
            nested_avg_body,
            nested_avg_oracle,
        ),
    ]


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
    wrapped_lead_body = """
rowset grouped <- select group_id as gid, sum(event_amount) as total;
with windowed as select
    grouped.gid as gid,
    grouped.total as total,
    lead(grouped.total, 1) over (order by grouped.gid asc) as next_total;

where windowed.next_total is not null
select windowed.gid, windowed.total, windowed.next_total
order by windowed.gid asc;
"""
    wrapped_lead_oracle = """
select gid, total, next_total
from (
    select gid, total, lead(total, 1) over (order by gid) as next_total
    from (
        select gid, sum(amount) as total
        from events
        group by gid
    ) grouped
) windowed
where next_total is not null
order by gid
"""
    semijoin_body = """
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
"""
    semijoin_oracle = """
select gid, total - previous_total
from (
    select
        gid,
        active,
        total,
        lag(total, 1) over (partition by gid order by active) as previous_total
    from (
        select gid, active, sum(amount) as total
        from events
        group by gid, active
    ) grouped
) windowed
where active = true
order by gid
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
        _case(
            seed,
            "window",
            "wrapped_lead_filter",
            "A wrapping rowset filters a LEAD output after window evaluation.",
            ("window", "rowset", "lead", "where", "nested"),
            wrapped_lead_body,
            wrapped_lead_oracle,
        ),
        _case(
            seed,
            "window",
            "offgrain_having_semijoin",
            "HAVING filters an off-grain dimension used inside a LAG window.",
            ("window", "rowset", "lag", "having", "semijoin"),
            semijoin_body,
            semijoin_oracle,
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
    three_arm_body = """
with combined as union(
    (
        where event_id % 3 = 0
        select event_id as eid, group_id as gid, event_amount as value
    ),
    (
        where event_id % 3 = 1
        select event_id as eid, group_id as gid, event_amount as value
    ),
    (
        where event_id % 3 = 2
        select event_id as eid, group_id as gid, event_amount as value
    )
) -> (eid, gid, value);

select combined.gid, sum(combined.value) as total
order by combined.gid asc;
"""
    three_arm_oracle = """
select gid, sum(value)
from (
    select eid, gid, amount as value from events where eid % 3 = 0
    union all
    select eid, gid, amount as value from events where eid % 3 = 1
    union all
    select eid, gid, amount as value from events where eid % 3 = 2
) combined
group by gid
order by gid
"""
    cases.append(
        _case(
            seed,
            "union",
            "three_arm_partition",
            "Three UNION ALL arms partition rows by a modulo expression.",
            ("union", "three_way", "expression", "aggregate"),
            three_arm_body,
            three_arm_oracle,
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
    rollup_having_body = """
select group_id, sum(event_amount) as total
having sum(event_amount) > 0
by rollup (group_id)
order by group_id asc nulls last;
"""
    rollup_having_oracle = """
select gid, sum(amount)
from events
group by rollup (gid)
having sum(amount) > 0
order by gid asc nulls last
"""
    rollup_window_body = """
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
"""
    rollup_window_oracle = """
select
    gid,
    sum(amount),
    grouping(gid) as grouping_level,
    rank() over (
        partition by grouping(gid)
        order by sum(amount) desc
    ) as level_rank
from events
group by rollup (gid)
order by grouping_level, level_rank, gid asc nulls last
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
        _case(
            seed,
            "grouping",
            "rollup_having",
            "HAVING filters detail and subtotal rows produced by ROLLUP.",
            ("grouping", "rollup", "aggregate", "having", "nullable"),
            rollup_having_body,
            rollup_having_oracle,
        ),
        _case(
            seed,
            "grouping",
            "rollup_window_rank",
            "RANK partitions ROLLUP rows by their grouping level.",
            ("grouping", "rollup", "window", "rank", "aggregate"),
            rollup_window_body,
            rollup_window_oracle,
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


def _multiway_join_cases(seed: SeedData) -> list[FuzzCase]:
    subset_body = """
rowset all_groups <- select group_id as gid, sum(event_amount) as total;
rowset positive_groups <- where event_amount > 0
select group_id as gid, sum(event_amount) as positive_total;
rowset active_groups <- where active
select group_id as gid, sum(event_amount) as active_total;

select
    all_groups.gid,
    all_groups.total,
    positive_groups.positive_total,
    active_groups.active_total
subset join positive_groups.gid = all_groups.gid
subset join active_groups.gid = all_groups.gid
order by all_groups.gid asc;
"""
    subset_oracle = """
select a.gid, a.total, p.total, x.total
from (
    select gid, sum(amount) as total
    from events
    group by gid
) a
left join (
    select gid, sum(amount) as total
    from events
    where amount > 0
    group by gid
) p on a.gid = p.gid
left join (
    select gid, sum(amount) as total
    from events
    where active
    group by gid
) x on a.gid = x.gid
order by a.gid
"""
    union_body = """
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
"""
    union_oracle = """
select
    coalesce(z.gid, o.gid, t.gid),
    z.total,
    o.total,
    t.total
from (
    select gid, sum(amount) as total
    from events
    where eid % 3 = 0
    group by gid
) z
full join (
    select gid, sum(amount) as total
    from events
    where eid % 3 = 1
    group by gid
) o on z.gid = o.gid
full join (
    select gid, sum(amount) as total
    from events
    where eid % 3 = 2
    group by gid
) t on coalesce(z.gid, o.gid) = t.gid
order by coalesce(z.gid, o.gid, t.gid)
"""
    return [
        _case(
            seed,
            "multiway_join",
            "two_subsets_one_anchor",
            "Two filtered subset rowsets share one complete aggregate anchor.",
            ("join", "subset", "multiway", "rowset", "aggregate"),
            subset_body,
            subset_oracle,
        ),
        _case(
            seed,
            "multiway_join",
            "three_way_union_chain",
            "A chained union declaration coalesces three disjoint rowsets.",
            ("join", "union", "multiway", "rowset", "three_way"),
            union_body,
            union_oracle,
        ),
    ]


def _composite_join_cases(seed: SeedData) -> list[FuzzCase]:
    body = """
rowset even_events <- where event_id % 2 = 0
select
    group_id as gid,
    active as flag,
    sum(event_amount) as even_total;
rowset odd_events <- where event_id % 2 = 1
select
    group_id as gid,
    active as flag,
    sum(event_amount) as odd_total;

select
    even_events.gid,
    even_events.flag,
    even_events.even_total,
    odd_events.odd_total
union join even_events.gid = odd_events.gid
union join even_events.flag = odd_events.flag
order by even_events.gid asc, even_events.flag asc;
"""
    oracle = """
select
    coalesce(e.gid, o.gid),
    coalesce(e.active, o.active),
    e.total,
    o.total
from (
    select gid, active, sum(amount) as total
    from events
    where eid % 2 = 0
    group by gid, active
) e
full join (
    select gid, active, sum(amount) as total
    from events
    where eid % 2 = 1
    group by gid, active
) o on e.gid = o.gid and e.active = o.active
order by coalesce(e.gid, o.gid), coalesce(e.active, o.active)
"""
    return [
        _case(
            seed,
            "composite_join",
            "two_plain_keys",
            "A union relation joins aggregate rowsets on a composite key.",
            ("join", "union", "composite", "rowset", "aggregate"),
            body,
            oracle,
        )
    ]


def _independent_rowset_join_cases(seed: SeedData) -> list[FuzzCase]:
    rowsets = """
rowset left_rows <- where left_value <= 8
select
    left_key as k,
    left_id as row_id,
    left_id % 2 as bucket,
    left_value as value;
rowset right_rows <- where union_value <= 80
select
    union_key as k,
    union_id as row_id,
    union_id % 2 as bucket,
    union_value as value;
"""
    sql_rowsets = """
from (
    select k, id as row_id, id % 2 as bucket, value
    from left_facts
    where value <= 8
) l
full join (
    select k, id as row_id, id % 2 as bucket, value
    from union_facts
    where value <= 80
) r
"""
    # A projected member of a coalescing (`union`/`full`) join-key group renders
    # as the row-by-row coalesce of the whole group (see
    # tests/test_scoped_join_permutations.py) — the oracle projections below
    # mirror that per variant, including the derived member (`row_id` ~
    # `r.row_id - 1`) in the derived-equality variant.
    variants = (
        (
            "single_key_fanout",
            "union join left_rows.k = right_rows.k",
            "on l.k is not distinct from r.k",
            "A single-key union join preserves many-to-many rows from two "
            "independently filtered rowsets.",
            ("single_key", "fanout"),
            "l.row_id",
        ),
        (
            "composite_plain_equality",
            (
                "union join left_rows.k = right_rows.k "
                "and left_rows.bucket = right_rows.bucket"
            ),
            (
                "on l.k is not distinct from r.k "
                "and l.bucket is not distinct from r.bucket"
            ),
            "A plain composite union join keeps both independently filtered rowsets.",
            ("composite", "plain_equality"),
            "l.row_id",
        ),
        (
            "composite_derived_equality",
            (
                "union join left_rows.k = right_rows.k "
                "and left_rows.row_id = right_rows.row_id - 1"
            ),
            ("on l.k is not distinct from r.k " "and l.row_id = r.row_id - 1"),
            "A derived composite union join retains its plain co-key and both sides.",
            ("composite", "derived_equality"),
            "coalesce(l.row_id, r.row_id - 1)",
        ),
    )
    cases = []
    for name, trilogy_join, sql_join, description, variant_tags, l_row_id in variants:
        body = f"""
{rowsets}

select
    left_rows.k,
    left_rows.row_id,
    left_rows.value,
    right_rows.row_id,
    right_rows.value
{trilogy_join}
order by
    left_rows.k asc nulls last,
    left_rows.row_id asc nulls last,
    right_rows.row_id asc nulls last;
"""
        oracle = f"""
select
    coalesce(l.k, r.k),
    {l_row_id},
    l.value,
    r.row_id,
    r.value
{sql_rowsets}
{sql_join}
order by
    coalesce(l.k, r.k) asc nulls last,
    {l_row_id} asc nulls last,
    r.row_id asc nulls last
"""
        cases.append(
            _case(
                seed,
                "independent_rowset_join",
                name,
                description,
                (
                    "join",
                    "union",
                    "rowset",
                    "independent_sources",
                    "where",
                    *variant_tags,
                ),
                body,
                oracle,
            )
        )
        key_only_body = f"""
{rowsets}

select left_rows.k, right_rows.k
{trilogy_join}
order by left_rows.k asc nulls last, right_rows.k asc nulls last;
"""
        key_only_oracle = f"""
select coalesce(l.k, r.k), coalesce(r.k, l.k)
{sql_rowsets}
{sql_join}
order by coalesce(l.k, r.k) asc nulls last, coalesce(r.k, l.k) asc nulls last
"""
        cases.append(
            _case(
                seed,
                "independent_rowset_join",
                f"{name}_key_only",
                f"{description} Only the coalesced keys are projected.",
                (
                    "join",
                    "union",
                    "rowset",
                    "independent_sources",
                    "where",
                    "key_only",
                    *variant_tags,
                ),
                key_only_body,
                key_only_oracle,
            )
        )
    return cases


def _rowset_boundary_cases(seed: SeedData) -> list[FuzzCase]:
    cases = []
    variants = (
        ("subset", "subset join subordinate.k = anchor.k", "left join"),
        ("union", "union join subordinate.k = anchor.k", "full join"),
        ("full", "full join subordinate.k = anchor.k", "full join"),
        ("left", "left join anchor.k = subordinate.k", "left join"),
    )
    for name, trilogy_join, sql_join in variants:
        body = f"""
rowset anchor <- select left_key as k, sum(left_value) as anchor_total;
rowset subordinate <- select
    subset_key as k,
    sum(subset_value) as subordinate_total;

with boundary as
select
    subordinate.k,
    anchor.anchor_total,
    subordinate.subordinate_total
{trilogy_join};

select
    boundary.subordinate.k,
    boundary.anchor_total,
    boundary.subordinate_total
order by boundary.subordinate.k asc nulls last;
"""
        key = "a.k" if name in ("subset", "left") else "coalesce(a.k, s.k)"
        oracle = f"""
select {key}, a.total, s.total
from (
    select k, sum(value) as total
    from left_facts
    group by k
) a
{sql_join} (
    select k, sum(value) as total
    from subset_facts
    group by k
) s on a.k is not distinct from s.k
order by {key} asc nulls last
"""
        cases.append(
            _case(
                seed,
                "rowset_boundary",
                f"{name}_subordinate_readback",
                (
                    f"An unaliased subordinate {name} key crosses a rowset "
                    "boundary under its authored address."
                ),
                (
                    "rowset",
                    "boundary",
                    "subordinate",
                    "coalesce",
                    name,
                    "nullable",
                ),
                body,
                oracle,
            )
        )

    composite_body = """
rowset even_events <- where event_id % 2 = 0
select
    group_id as gid,
    active as flag,
    sum(event_amount) as even_total;
rowset odd_events <- where event_id % 2 = 1
select
    group_id as gid,
    active as flag,
    sum(event_amount) as odd_total;

with boundary as
select
    even_events.gid,
    even_events.flag,
    even_events.even_total,
    odd_events.odd_total
union join even_events.gid = odd_events.gid
union join even_events.flag = odd_events.flag;

select
    boundary.even_events.gid,
    boundary.even_events.flag,
    boundary.even_total,
    boundary.odd_total
order by
    boundary.even_events.gid asc,
    boundary.even_events.flag asc;
"""
    composite_oracle = """
select
    coalesce(e.gid, o.gid),
    coalesce(e.active, o.active),
    e.total,
    o.total
from (
    select gid, active, sum(amount) as total
    from events
    where eid % 2 = 0
    group by gid, active
) e
full join (
    select gid, active, sum(amount) as total
    from events
    where eid % 2 = 1
    group by gid, active
) o on e.gid = o.gid and e.active = o.active
order by coalesce(e.gid, o.gid), coalesce(e.active, o.active)
"""
    cases.append(
        _case(
            seed,
            "rowset_boundary",
            "composite_subordinate_readback",
            "Two subordinate composite keys cross a coalescing rowset boundary.",
            (
                "rowset",
                "boundary",
                "subordinate",
                "coalesce",
                "composite",
                "union",
            ),
            composite_body,
            composite_oracle,
        )
    )

    window_body = """
rowset anchor <- select left_key as k, sum(left_value) as anchor_total;
rowset subordinate <- select
    subset_key as k,
    sum(subset_value) as subordinate_total;

with boundary as
select
    subordinate.k,
    anchor.anchor_total,
    subordinate.subordinate_total
subset join subordinate.k = anchor.k;

select
    boundary.subordinate.k,
    boundary.anchor_total,
    boundary.subordinate_total,
    row_number() over (
        order by boundary.subordinate.k asc
    ) as key_row
order by boundary.subordinate.k asc nulls last;
"""
    window_oracle = """
select
    a.k,
    a.total,
    s.total,
    row_number() over (order by a.k asc nulls last)
from (
    select k, sum(value) as total
    from left_facts
    group by k
) a
left join (
    select k, sum(value) as total
    from subset_facts
    group by k
) s on a.k is not distinct from s.k
order by a.k asc nulls last
"""
    cases.append(
        _case(
            seed,
            "rowset_boundary",
            "subordinate_window_readback",
            "A downstream window orders by a subordinate key crossing a boundary.",
            (
                "rowset",
                "boundary",
                "subordinate",
                "window",
                "subset",
                "nullable",
            ),
            window_body,
            window_oracle,
        )
    )
    return cases


def _named_grouping_window_cases(seed: SeedData) -> list[FuzzCase]:
    cases = []
    variants = (
        ("rollup_alias", "rollup", False),
        ("rollup_double_alias", "rollup", True),
        ("cube_alias", "cube", False),
    )
    for name, grouping_mode, double_alias in variants:
        alias_definition = "auto rank_alias <- named_rank;\n" if double_alias else ""
        selected_rank = "rank_alias" if double_alias else "named_rank"
        body = f"""
auto partition_label <- case
    when grouping(group_name) = 1 then '~TOTAL~'
    else group_name
end;
auto named_rank <- rank(active) over (
    partition by partition_label
    order by sum(event_amount) desc
);
{alias_definition}
select
    group_name,
    active,
    sum(event_amount) as total,
    {selected_rank} as ranked
by {grouping_mode} (group_name, active)
order by group_name asc nulls last, active asc nulls last;
"""
        oracle = f"""
select
    group_name,
    active,
    total,
    rank() over (
        partition by case
            when name_grouping = 1 then '~TOTAL~'
            else group_name
        end
        order by total desc
    )
from (
    select
        g.name as group_name,
        e.active,
        sum(e.amount) as total,
        grouping(g.name) as name_grouping
    from events e
    join groups g on e.gid = g.gid
    group by {grouping_mode} (g.name, e.active)
) grouped
order by group_name asc nulls last, active asc nulls last
"""
        cases.append(
            _case(
                seed,
                "named_grouping_window",
                name,
                (
                    "A named grouping-derived partition feeds a named window "
                    f"through {'two aliases' if double_alias else 'an alias'} "
                    f"under {grouping_mode.upper()}."
                ),
                (
                    "grouping",
                    grouping_mode,
                    "window",
                    "named",
                    "alias",
                    "nested" if double_alias else "single_alias",
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
    offset_body = """
rowset left_side <- select left_key as k, sum(left_value) as total;
rowset right_side <- select union_key as k, sum(union_value) as total;

select
    coalesce(left_side.k + 1, right_side.k) as joined_key,
    left_side.total,
    right_side.total
union join left_side.k + 1 = right_side.k
order by joined_key asc nulls last;
"""
    offset_oracle = """
select coalesce(l.k + 1, r.k), l.total, r.total
from (
    select k, sum(value) as total
    from left_facts
    group by k
) l
full join (
    select k, sum(value) as total
    from union_facts
    group by k
) r on l.k + 1 is not distinct from r.k
order by coalesce(l.k + 1, r.k) asc nulls last
"""
    cases.append(
        _case(
            seed,
            "derived_join",
            "offset_expression",
            "A full-domain relation joins a transformed key to a plain key.",
            ("join", "union", "derived", "offset", "rowset", "nullable"),
            offset_body,
            offset_oracle,
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
    # sales/returns bind group_id partially (`~`); groups is the complete
    # domain source, so the chasm aggregation runs over the FULL group domain
    # (factless groups carry NULL totals on both sides).
    direct_oracle = """
select g.gid, s.total, r.total
from groups g
left join (
    select gid, sum(amount) as total
    from sales
    group by gid
) s on g.gid = s.gid
left join (
    select gid, sum(amount) as total
    from returns
    group by gid
) r on g.gid = r.gid
order by g.gid
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
left join (
    select gid, sum(amount) as total
    from sales
    group by gid
) s on g.gid = s.gid
left join (
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
    select g.gid, s.total as sales_total, r.total as returns_total
    from groups g
    left join (
        select gid, sum(amount) as total
        from sales
        group by gid
    ) s on g.gid = s.gid
    left join (
        select gid, sum(amount) as total
        from returns
        group by gid
    ) r on g.gid = r.gid
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
        _complex_where_cases,
        _function_cases,
        _rowset_cases,
        _having_cases,
        _window_cases,
        _union_cases,
        _membership_cases,
        _grouping_cases,
        _join_cases,
        _multiway_join_cases,
        _composite_join_cases,
        _independent_rowset_join_cases,
        _rowset_boundary_cases,
        _named_grouping_window_cases,
        _derived_join_cases,
        _chasm_cases,
    )
    for seed in seeds:
        for builder in builders:
            cases.extend(builder(seed))
    return cases
