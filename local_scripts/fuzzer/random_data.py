from __future__ import annotations

import random

from local_scripts.fuzzer.models import Row, SeedData, TableData


def generate_random_seeds(
    count: int,
    start: int,
    rows: int,
) -> tuple[SeedData, ...]:
    return tuple(random_seed(start + offset, rows) for offset in range(count))


def random_seed(seed: int, rows: int) -> SeedData:
    rng = random.Random(seed)
    row_count = max(8, rows)
    group_count = max(5, min(16, row_count // 3))
    group_ids = sorted(rng.sample(range(-50, 51), group_count))

    groups: list[Row] = []
    for index, group_id in enumerate(group_ids):
        nullable_name = None if index % 3 == seed % 3 else f"n{index}"
        groups.append((group_id, f"group_{index}", nullable_name, f"R{index}"))

    events: list[Row] = []
    for index in range(row_count):
        amount = rng.randint(-10_000, 10_000)
        nullable_amount = None if rng.random() < 0.4 else rng.randint(-500, 500)
        active = rng.random() < 0.5
        events.append(
            (index + 1, rng.choice(group_ids), amount, nullable_amount, active)
        )
    events[0] = (*events[0][:3], None, False)
    events[1] = (*events[1][:3], rng.randint(-500, 500), True)
    duplicate_amount = nonzero_int(rng, -10_000, 10_000)
    events[2] = (events[2][0], group_ids[1], duplicate_amount, events[2][3], True)
    events[3] = (events[3][0], group_ids[1], duplicate_amount, events[3][3], True)

    join_domain = rng.sample(range(-100, 101), 6)
    left_facts = keyed_rows(rng, row_count, join_domain, include_null=True)
    subset_facts = keyed_rows(
        rng,
        max(6, row_count // 2),
        join_domain[:4],
        include_null=True,
    )
    extra_domain = unique_values(rng, 3, excluded=set(join_domain))
    union_facts = keyed_rows(
        rng,
        max(8, row_count // 2),
        join_domain[:3] + extra_domain,
        include_null=True,
    )

    fact_rows = max(8, row_count // 2)
    sales_groups = [group_ids[0], group_ids[0], group_ids[1], group_ids[2]]
    return_groups = [
        group_ids[0],
        group_ids[0],
        group_ids[0],
        group_ids[1],
        group_ids[3],
    ]
    sales = fact_rows_for_groups(rng, fact_rows, sales_groups, group_ids)
    returns = fact_rows_for_groups(rng, fact_rows + 1, return_groups, group_ids)

    return SeedData(
        name=f"random_{seed:06d}",
        description=(
            f"Deterministic pseudo-random seed {seed} with {row_count} event rows, "
            "wide signed measures, nullable fields, duplicate keys, asymmetric "
            "fanout, and overlapping/exclusive domains."
        ),
        groups=TableData(
            "groups",
            ("gid", "name", "nullable_name", "required_name"),
            tuple(groups),
        ),
        events=TableData(
            "events",
            ("eid", "gid", "amount", "nullable_amount", "active"),
            tuple(events),
        ),
        left_facts=TableData(
            "left_facts",
            ("id", "k", "value"),
            left_facts,
        ),
        subset_facts=TableData(
            "subset_facts",
            ("id", "k", "value"),
            subset_facts,
        ),
        union_facts=TableData(
            "union_facts",
            ("id", "k", "value"),
            union_facts,
        ),
        sales=TableData("sales", ("id", "gid", "amount"), sales),
        returns=TableData("returns", ("id", "gid", "amount"), returns),
    )


def keyed_rows(
    rng: random.Random,
    count: int,
    domain: list[int],
    include_null: bool,
) -> tuple[Row, ...]:
    keys: list[int | None] = list(domain)
    if include_null:
        keys.append(None)
    generated = [
        (index + 1, key, nonzero_int(rng, -5_000, 5_000))
        for index, key in enumerate(keys)
    ]
    while len(generated) < count:
        generated.append(
            (
                len(generated) + 1,
                rng.choice(keys),
                nonzero_int(rng, -5_000, 5_000),
            )
        )
    return tuple(generated)


def fact_rows_for_groups(
    rng: random.Random,
    count: int,
    required: list[int],
    domain: list[int],
) -> tuple[Row, ...]:
    groups = required + [
        rng.choice(domain) for _ in range(max(0, count - len(required)))
    ]
    return tuple(
        (index + 1, group_id, nonzero_int(rng, -2_000, 2_000))
        for index, group_id in enumerate(groups)
    )


def unique_values(
    rng: random.Random,
    count: int,
    excluded: set[int],
) -> list[int]:
    values: list[int] = []
    while len(values) < count:
        candidate = rng.randint(-150, 150)
        if candidate in excluded or candidate in values:
            continue
        values.append(candidate)
    return values


def nonzero_int(rng: random.Random, lower: int, upper: int) -> int:
    value = rng.randint(lower, upper)
    return value or 1
