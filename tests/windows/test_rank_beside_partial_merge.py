"""A rank ordered by a per-vehicle total, gated by HAVING, beside columns of a
partial merge (launches and the satellites whose `~launch_id` merges into
`~id`) (#1004). A window is evaluated over the rows of its own grain
(`Windows.rank_of_distinct`); computed on the raw launch rows it counts each
launch (`Windows.row_stream_rank_counts_repeats`). Widening the merge's scan
chain with the rank as a join key planted it on the launch scan, which binds
neither the rank nor the total, and planning raised "Missing source map
entry". Every case is checked against rows computed in Python."""

import random
from collections import defaultdict
from itertools import combinations

import pytest

from tests.helpers.rows import executor_for, fetch_rows, normalize_numbers
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.strategy_builder import _computes_barrier
from trilogy.executor import Executor

TEMPLATE = """
key vehicle_name string;
key vehicle_variant string;
datasource lv_info (vehicle_name, vehicle_variant) grain (vehicle_name, vehicle_variant)
query '''{vehicles}''';

key id string;
property id.orb_pay float;
property id.category string;
property id.orbit_code <- split(category, ' ')[2];
datasource launch_info (id, vehicle_name: ~vehicle_name,
    vehicle_variant: ~vehicle_variant, orb_pay, category)
grain (id)
query '''{launches}''';

key jcat string;
key payload_bus string;
property jcat.payload_name string;
key payload_launch_id string;
datasource satcat (jcat, payload_launch_id: ~payload_launch_id, payload_bus,
    payload_name)
grain (jcat)
query '''{satellites}''';

merge payload_launch_id into ~id;

property payload_label <- CASE WHEN payload_bus = '-' THEN payload_name
    ELSE payload_bus END;
property vehicle_label <- CASE WHEN vehicle_variant != '-'
    THEN concat(vehicle_name, '-', vehicle_variant) ELSE vehicle_name END;
"""

RANK = "rank vehicle_name order by sum(orb_pay) by vehicle_name desc as vehicle_rank"
COLUMNS = {
    "payload_label": "payload_label",
    "vehicle_label": "vehicle_label",
    "orbit_code": "orbit_code",
    "payload_count": "jcat.count as payload_count",
}
SUBSETS = [
    list(combo)
    for size in range(1, len(COLUMNS) + 1)
    for combo in combinations(COLUMNS, size)
]

Vehicle = tuple[str, str]
Launch = tuple[str, str, str, float, str]
Satellite = tuple[str, str, str, str]

VEHICLES: list[Vehicle] = [
    ("Atlas", "-"),
    ("Atlas", "V"),
    ("Delta", "-"),
    ("Soyuz", "-"),
]
LAUNCHES: list[Launch] = [
    ("L1", "Atlas", "-", 5.0, "O LEO"),
    ("L2", "Atlas", "V", 7.0, "O GTO"),
    ("L3", "Delta", "-", 20.0, "O LEO"),
    ("L4", "Soyuz", "-", 1.0, "O SSO"),
    ("L5", "Delta", "-", 3.0, "O GTO"),
]
SATELLITES: list[Satellite] = [
    ("S1", "L1", "-", "Sat A"),
    ("S2", "L3", "BUS1", "Sat B"),
    ("S3", "L3", "BUS1", "Sat C"),
    ("S4", "L5", "-", "Sat D"),
    ("S5", "L4", "BUS2", "Sat E"),
]


def _values(rows: list[tuple], columns: str) -> str:
    def lit(v) -> str:
        return f"'{v}'" if isinstance(v, str) else repr(float(v))

    body = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)
    return f"select * from (values {body}) t({columns})"


def model(
    vehicles: list[Vehicle], launches: list[Launch], satellites: list[Satellite]
) -> str:
    return TEMPLATE.format(
        vehicles=_values(vehicles, "vehicle_name, vehicle_variant"),
        launches=_values(
            launches, "id, vehicle_name, vehicle_variant, orb_pay, category"
        ),
        satellites=_values(
            satellites, "jcat, payload_launch_id, payload_bus, payload_name"
        ),
    )


def query(columns: list[str], having: bool) -> str:
    select = ", ".join([*(COLUMNS[c] for c in columns), RANK])
    return f"select {select}{' having vehicle_rank = 1' if having else ''};"


def expected(
    vehicles: list[Vehicle],
    launches: list[Launch],
    satellites: list[Satellite],
    columns: list[str],
    having: bool,
) -> list[tuple]:
    """Each launch beside each of its satellites (or alone), and each vehicle
    no launch flew (`lv_info` is the vehicles' full domain), grouped by the
    selected columns and the rank of its vehicle's total over the vehicles."""
    totals: dict[str, float] = defaultdict(float)
    for _, name, _, pay, _ in launches:
        totals[name] += pay
    rank = {
        name: 1 + sum(other > total for other in totals.values())
        for name, total in totals.items()
    }
    by_launch: dict[str, list[Satellite]] = defaultdict(list)
    for sat in satellites:
        by_launch[sat[1]].append(sat)
    counts: dict[tuple, int] = defaultdict(int)
    flown = {(name, variant) for _, name, variant, _, _ in launches}
    unflown = [(None, n, v, 0.0, None) for n, v in vehicles if (n, v) not in flown]
    for lid, name, variant, _, category in [*launches, *unflown]:
        for sat in (by_launch[lid] if lid else []) or [None]:
            row = {
                "payload_label": (
                    None if sat is None else (sat[3] if sat[2] == "-" else sat[2])
                ),
                "vehicle_label": name if variant == "-" else f"{name}-{variant}",
                "orbit_code": category and category.split(" ")[1],
            }
            dims = [row[c] for c in columns if c != "payload_count"]
            counts[(*dims, rank[name])] += sat is not None
    out = []
    for key, count in counts.items():
        *dims, r = key
        if having and r != 1:
            continue
        values = iter(dims)
        out.append(
            tuple(count if c == "payload_count" else next(values) for c in columns)
            + (r,)
        )
    return normalize_numbers(out)


def draw(seed: int) -> tuple[list[Vehicle], list[Launch], list[Satellite]]:
    rng = random.Random(seed)
    names = rng.sample(
        ["Atlas", "Delta", "Soyuz", "Ariane", "Falcon"], rng.randint(2, 5)
    )
    vehicles = sorted(
        {
            (n, v)
            for n in names
            for v in ["-", *rng.sample(["A", "B"], rng.randint(0, 2))]
        }
    )
    flown = [*((n, "-") for n in names), *rng.choices(vehicles, k=rng.randint(0, 8))]
    launches = [
        (
            f"L{i}",
            *vehicle,
            float(rng.randint(1, 6)),
            f"O {rng.choice(['LEO', 'GTO', 'SSO'])}",
        )
        for i, vehicle in enumerate(flown)
    ]
    satellites = [
        (
            f"S{i}",
            rng.choice(launches)[0],
            rng.choice(["-", "BUS1", "BUS2"]),
            f"Sat {rng.choice('ABCD')}",
        )
        for i in range(rng.randint(1, 14))
    ]
    return vehicles, launches, satellites


@pytest.fixture(scope="module")
def fixed() -> Executor:
    executor = executor_for(model(VEHICLES, LAUNCHES, SATELLITES))
    yield executor
    executor.close()


def test_issue_query_ranks_the_vehicle_totals(fixed: Executor):
    rows = fetch_rows(fixed, query(list(COLUMNS), having=True))
    assert normalize_numbers(rows) == normalize_numbers(
        [("BUS1", "Delta", "LEO", 2, 1), ("Sat D", "Delta", "GTO", 1, 1)]
    )


@pytest.mark.parametrize("having", [True, False])
@pytest.mark.parametrize("columns", SUBSETS, ids="+".join)
def test_rank_beside_partial_merge_matches_rows(
    fixed: Executor, columns: list[str], having: bool
):
    rows = fetch_rows(fixed, query(columns, having))
    assert normalize_numbers(rows) == expected(
        VEHICLES, LAUNCHES, SATELLITES, columns, having
    )


@pytest.mark.parametrize("seed", range(12))
def test_rank_beside_partial_merge_random_rows(seed: int):
    vehicles, launches, satellites = draw(seed)
    executor = executor_for(model(vehicles, launches, satellites))
    try:
        for columns in (list(COLUMNS), ["vehicle_label", "payload_count"]):
            rows = fetch_rows(executor, query(columns, having=True))
            assert normalize_numbers(rows) == expected(
                vehicles, launches, satellites, columns, having=True
            ), (seed, columns)
    finally:
        executor.close()


def test_projection_passes_a_barrier_through_never_computes_it(fixed: Executor):
    env = fixed.environment.materialize_for_select()
    rank = next(c for c in env.concepts.values() if c.derivation == Derivation.WINDOW)
    total = rank.lineage.order_by[0].expr
    scan = {"local.vehicle_name", "local.orb_pay", "local.id"}
    assert _computes_barrier(rank, scan)
    assert _computes_barrier(total, scan)
    assert not _computes_barrier(rank, scan | {rank.address})
    assert not _computes_barrier(env.concepts["local.orbit_code"], scan)
