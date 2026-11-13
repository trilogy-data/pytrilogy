"""Selects over a fact table and a chain of dimensions, against an oracle over
the raw rows. Each case draws a flight -> aircraft -> model model with carrier
and airport dimensions beside it, and varies what the planner reads, never
what the answer is: whether each binding is complete (`~`) or nullable (`?`),
whether aircraft is sourced at its `tail_num` grain, its `id` grain or both,
and whether count rollups sit beside the facts. Every dimension row is
referenced and every reference resolves, so the answer is the plain join.

The selects pair dimension attributes with a per-flight measure
(`sum(group(seats) by id)`), the shape whose join axis two planner regressions
lost (`test_aggregate_side_parity.py`). A NULL `tail_num` among the flights
exercises the NULL-key pairings (`test_null_key_groups.py`)."""

import random
from collections import defaultdict

import pytest

from tests.helpers.rows import fetch_rows
from trilogy import Dialects

SEEDS = 600
NAMES = ["737", "A320", "E175"]
MAKERS = ["BOEING", "AIRBUS"]
ATTRS = ["model_name", "model_maker", "carrier_name", "airport_name", "tail_num"]
MEASURES = {
    "n": "count(id)",
    "s": "sum(group(seats) by id)",
    "mx": "max(seats)",
}


def _covering(rng: random.Random, n: int, pool: int) -> list[int]:
    picks = [i % pool for i in range(max(n, pool))]
    rng.shuffle(picks)
    return picks


def _lit(v) -> str:
    return "null" if v is None else f"'{v}'" if isinstance(v, str) else str(v)


def _values(rows: list[tuple], columns: str) -> str:
    body = ", ".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in rows)
    return f"SELECT * FROM (VALUES {body}) t({columns})"


def _draw(rng: random.Random) -> tuple[str, list[dict]]:
    models = [
        (f"M{i}", rng.choice(NAMES), rng.choice(MAKERS), rng.randint(1, 9))
        for i in range(rng.randint(1, 3))
    ]
    model_nullable = rng.random() < 0.3
    aircraft = []
    for i, m in enumerate(_covering(rng, rng.randint(1, 4), len(models))):
        code = models[m][0]
        aircraft.append((i, f"N{i}", code))
    if model_nullable:
        aircraft.append((len(aircraft), f"N{len(aircraft)}", None))
    carriers = [(f"C{i}", f"carrier{i}") for i in range(rng.randint(1, 2))]
    airports = [(i, f"P{i}", f"airport{i}") for i in range(rng.randint(1, 2))]
    tail_nullable = rng.random() < 0.3
    n_flights = max(len(aircraft), len(carriers), len(airports)) + rng.randint(0, 3)
    tails = _covering(rng, n_flights, len(aircraft))
    cars = _covering(rng, len(tails), len(carriers))
    ports = _covering(rng, len(tails), len(airports))
    flights = [
        (i, aircraft[t][1], carriers[c][0], airports[p][1])
        for i, (t, c, p) in enumerate(zip(tails, cars, ports))
    ]
    if tail_nullable:
        flights.append((len(flights), None, carriers[0][0], airports[0][1]))

    def mod(nullable: bool) -> str:
        return ("~" if rng.random() < 0.5 else "") + ("?" if nullable else "")

    model_layouts = rng.choice(
        [["tail_num"], ["aircraft_id"], ["tail_num", "aircraft_id"]]
    )
    text = f"""
key model_code string;
property model_code.model_name string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: {"~" if rng.random() < 0.5 else ""}model_code,
    model_name, model_maker, seats) grain (model_code)
query '''{_values(models, 'model_code, model_name, model_maker, seats')} ''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''{_values(carriers, 'carrier_code, carrier_name')} ''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''{_values(airports, 'airport_id, airport_code, airport_name')} ''';
key id int;
datasource flight (id, tail_num: {mod(tail_nullable)}tail_num,
    carrier_code: {mod(False)}carrier_code, airport_code: {mod(False)}airport_code)
grain (id)
query '''{_values(flights, 'id, tail_num, carrier_code, airport_code')} ''';
"""
    for grain in model_layouts:
        text += f"""
datasource aircraft_by_{grain} (aircraft_id, tail_num, model_code: {mod(model_nullable)}model_code)
grain ({grain})
query '''{_values(aircraft, 'aircraft_id, tail_num, model_code')} ''';
"""
    if rng.random() < 0.5:
        counts: dict[str, int] = defaultdict(int)
        for _, _, carrier, _ in flights:
            counts[carrier] += 1
        text += f"""
datasource flight_count_by_carrier (carrier_code, n: count)
grain (carrier_code)
query '''{_values(sorted(counts.items()), 'carrier_code, n')} ''';
auto count <- count(id);
"""
    by_model = {m[0]: m for m in models}
    by_tail = {a[1]: a for a in aircraft}
    names = dict(carriers)
    ports_by_code = {a[1]: a[2] for a in airports}
    rows = []
    for fid, tail, carrier, port in flights:
        model = by_model.get(by_tail[tail][2]) if tail else None
        rows.append(
            {
                "id": fid,
                "tail_num": tail,
                "model_name": model[1] if model else None,
                "model_maker": model[2] if model else None,
                "seats": model[3] if model else None,
                "carrier_name": names[carrier],
                "airport_name": ports_by_code[port],
            }
        )
    return text, rows


def _oracle(rows: list[dict], attrs: list[str], measures: list[str]) -> list[tuple]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[tuple(r[a] for a in attrs)].append(r)
    out = []
    for key, members in groups.items():
        seats = [m["seats"] for m in members if m["seats"] is not None]
        values = {
            "n": len(members),
            "s": sum(seats) if seats else None,
            "mx": max(seats) if seats else None,
        }
        out.append((*key, *(values[m] for m in measures)))
    return out


def _sorted(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.mark.parametrize("seed", range(SEEDS))
def test_dimension_chain_select_matches_raw_rows(seed: int):
    rng = random.Random(seed)
    text, rows = _draw(rng)
    attrs = rng.sample(ATTRS, rng.randint(1, 3))
    # a fact measure puts the select at the flights' grain; `max(seats)` alone
    # is read off the dimensions
    measures = [rng.choice(["n", "s"])] + (["mx"] if rng.random() < 0.4 else [])
    select = ", ".join([*attrs, *(f"{MEASURES[m]} as {m}" for m in measures)])
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(text)
    query = f"select {select};"
    got = [tuple(r) for r in fetch_rows(executor, query)]
    assert _sorted(got) == _sorted(_oracle(rows, attrs, measures)), (query, text)
