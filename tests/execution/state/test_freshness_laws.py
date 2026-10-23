"""Freshness laws (formal/Trilogy/Freshness.lean), checked end to end.

The verdict compares watermarks in Python and the refresh filters rows in SQL,
so the two agree only if Python orders watermarks as SQL's ``>`` does
(``Freshness.verdict_agrees_with_refresh``). With arrivals strictly past the
watermark, a refresh equals a full rebuild (``Freshness.incremental_exact``).
Each case builds a target, appends rows, and refreshes, then requires:

- the target holds exactly the rows a full rebuild would
- a second plan finds nothing stale
- a plan seeded from a snapshot round trip reaches the live plan's verdict
"""

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.core.models.environment import Environment
from trilogy.execution.state import (
    SnapshotStateStore,
    StateSnapshot,
    build_datasource_state,
    create_refresh_plan,
    merge_into_snapshot,
    restore_watermark_value,
)
from trilogy.execution.state.snapshot import _watermark_value
from trilogy.execution.state.state_store import refresh_stale_assets
from trilogy.execution.state.watermarks import _compare_watermark_values

KEYS = {
    "int": ("int", "INTEGER", lambda i: i * 7 - 3, str),
    "float": ("float", "DOUBLE", lambda i: i * 1.5 - 4.25, str),
    "numeric": (
        "numeric(12,2)",
        "DECIMAL(12,2)",
        lambda i: Decimal(i * 3) + Decimal("0.25"),
        lambda v: f"{v}::DECIMAL(12,2)",
    ),
    "date": (
        "date",
        "DATE",
        lambda i: date(2024, 1, 1) + timedelta(days=i),
        lambda v: f"DATE '{v}'",
    ),
    "datetime": (
        "datetime",
        "TIMESTAMP",
        lambda i: datetime(2024, 1, 1) + timedelta(hours=7 * i),
        lambda v: f"TIMESTAMP '{v}'",
    ),
    "string": ("string", "VARCHAR", lambda i: f"k{i:04d}", lambda v: f"'{v}'"),
}
STORAGE = {
    "table": ("address {name}", "{name}"),
    "parquet": ("file `{path}.parquet`", "read_parquet('{path}.parquet')"),
    "csv": ("file `{path}.csv`", "read_csv('{path}.csv')"),
}
COLUMNS = "(ev_id: ev_id, ev_k: ev_k, cat: cat, bucket: bucket)"
ROOT_ARMS = (("a", "cat = 'a'"), ("b", "cat != 'a'"), ("n", "cat is null"))


def _target(name: str, case: dict, storage: str, path: str, where: bool) -> str:
    address = STORAGE[storage][0].format(name=name, path=path)
    return f"""{"partial " if where else ""}datasource {name} {COLUMNS}
grain (ev_id)
{"complete where cat = 'a'" if where else ""}
{address}
{case["trigger"]} by ev_k
{"partition by bucket" if case["part"] else ""};
"""


def _model(case: dict, path: str) -> str:
    if case["split"]:
        roots = "\n".join(
            f"root partial datasource src_{n} {COLUMNS}\ngrain (ev_id)"
            f" complete where {w} query '''select * from src where {w} ''';"
            for n, w in ROOT_ARMS
        )
    else:
        roots = f"root datasource src {COLUMNS}\ngrain (ev_id) address src;"
    model = f"""key ev_id int;
property ev_id.ev_k {KEYS[case["key"]][0]};
property ev_id.cat string;
property ev_id.bucket date;

{roots}

{_target("target", case, case["storage"], path, case["where"])}
"""
    if case["chain"]:
        model += _target("target2", {**case, "part": False}, "table", path + "2", False)
    return model


def _insert(executor, key: str, rows: list[tuple]) -> None:
    render = KEYS[key][3]
    values = ", ".join(
        f"({i}, {'NULL' if v is None else render(v)}, '{c}', DATE '{b}')"
        for i, v, c, b in rows
    )
    executor.execute_raw_sql(f"insert into src values {values}")
    executor.connection.commit()


def _ids(executor, storage: str, name: str, path: str) -> list[int]:
    relation = STORAGE[storage][1].format(name=name, path=path)
    rows = executor.execute_raw_sql(f"select ev_id from {relation}").fetchall()
    return sorted(r[0] for r in rows)


def _snapshot(executor) -> StateSnapshot:
    plan = create_refresh_plan(executor)
    entries = [
        (
            ds.safe_address,
            build_datasource_state(ds, plan.watermarks.get(ds.identifier), None),
        )
        for ds in executor.environment.datasources.values()
    ]
    return StateSnapshot.model_validate_json(
        merge_into_snapshot(entries).model_dump_json()
    )


def _stale(executor, **kwargs) -> list[str]:
    plan = create_refresh_plan(executor, **kwargs)
    return sorted(a.datasource_id for a in plan.stale_assets)


def _draw(rng: random.Random) -> dict:
    storage = rng.choice(["table", "table", "parquet", "csv"])
    return {
        "key": rng.choice(list(KEYS)),
        "storage": storage,
        "trigger": rng.choice(["incremental", "freshness"]),
        "part": storage == "table" and rng.random() < 0.3,
        "where": rng.random() < 0.3,
        "split": rng.random() < 0.3,
        "chain": rng.random() < 0.3,
        "nulls": rng.random() < 0.3,
    }


def _rows(rng: random.Random, case: dict) -> tuple[list[tuple], list[tuple]]:
    first, second = rng.randint(1, 6), rng.randint(0, 6)
    picks = sorted(rng.sample(range(1, 40), first + second))
    make = KEYS[case["key"]][2]
    rows = [
        (i, make(k), rng.choice("ab"), date(2024, 1, 1) + timedelta(days=k // 10))
        for i, k in enumerate(picks)
    ]
    rows[0] = (*rows[0][:2], "a", rows[0][3])
    if case["nulls"]:
        rows.insert(1, (99, None, rng.choice("ab"), date(2024, 1, 1)))
        first += 1
    return rows[:first], rows[first:]


def check_case(case: dict, rng: random.Random, workdir: Path) -> list[str]:
    key, path = case["key"], (workdir / "target").as_posix()
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workdir)
    )
    executor.execute_raw_sql(
        f"create table src (ev_id int, ev_k {KEYS[key][1]}, cat varchar,"
        " bucket date)"
    )
    first, second = _rows(rng, case)
    _insert(executor, key, first)
    executor.parse_text(_model(case, path))
    refresh_stale_assets(executor)
    snapshot = _snapshot(executor)
    if second:
        _insert(executor, key, second)
    failures = []
    live = _stale(executor)
    seeded = _stale(executor, state_store=SnapshotStateStore(snapshot))
    if seeded != live:
        failures.append(f"snapshot verdict {seeded} != live {live}")
    refresh_stale_assets(executor)
    if case["chain"]:
        refresh_stale_assets(executor)
    rows = first + second
    want = sorted(r[0] for r in rows if not case["where"] or r[2] == "a")
    if (got := _ids(executor, case["storage"], "target", path)) != want:
        failures.append(f"target rows {got} != {want}")
    if case["chain"]:
        got = _ids(executor, "table", "target2", path + "2")
        if got != sorted(r[0] for r in rows):
            failures.append(f"target2 rows {got} != {sorted(r[0] for r in rows)}")
    if again := _stale(executor):
        failures.append(f"still stale after refresh: {again}")
    return failures


@pytest.mark.parametrize("seed", range(40))
def test_refresh_converges_to_a_rebuild(seed, tmp_path):
    rng = random.Random(seed)
    case = _draw(rng)
    assert check_case(case, rng, tmp_path) == [], case


@pytest.mark.parametrize("storage", ["table", "parquet", "csv"])
@pytest.mark.parametrize("trigger", ["incremental", "freshness"])
def test_a_numeric_watermark_refreshes(storage, trigger, tmp_path):
    case = {
        "key": "numeric",
        "storage": storage,
        "trigger": trigger,
        "part": False,
        "where": False,
        "split": False,
        "chain": False,
        "nulls": False,
    }
    assert check_case(case, random.Random(1), tmp_path) == []


def test_numbers_order_by_value_across_types():
    assert _compare_watermark_values(9, 10.0) == -1
    assert _compare_watermark_values(Decimal("9.50"), 10.0) == -1
    assert _compare_watermark_values(10, Decimal("10.00")) == 0
    assert _compare_watermark_values(Decimal("117.25"), 81.25) == 1


def test_a_decimal_watermark_round_trips_a_snapshot():
    value = Decimal("9.50")
    entry = _watermark_value(
        "k", UpdateKey("k", UpdateKeyType.INCREMENTAL_KEY, value), None
    )
    restored = restore_watermark_value(entry)
    assert restored == value and isinstance(restored, Decimal)


def test_a_decimal_renders_as_a_plain_literal():
    generator = Dialects.DUCK_DB.default_executor().generator
    assert generator.render_expr(Decimal("57.25")) == "57.25"
    assert generator.render_expr(Decimal("1E+2")) == "100"
