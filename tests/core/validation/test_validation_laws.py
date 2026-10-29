"""Datasource validation laws (formal/Trilogy/Validate.lean), checked on random tables.

Each check guards a claim the planner trusts. It must fail exactly when the
claim fails:

- `null_check_exact`: a non-nullable binding holds no NULL, past any sample
- `domain_check_exact`: every non-null value lies in a declared range
- `grain_check_exact`: no two rows share a grain key, NULL keys equal
- `completeness_exact`: a complete binding holds every value any source holds
- `unique_prop_exact`: a `unique` property maps to one key, NULL a key value
- `containment_exact`: every row of a `complete where` arm satisfies its claim
"""

import random
from decimal import Decimal
from typing import Any

import pytest

from trilogy import Dialects, Executor
from trilogy.core.enums import ValidationScope
from trilogy.core.models.core import ValueRange
from trilogy.core.validation.environment import validate_environment

SEEDS = range(40)


def _executor() -> Executor:
    return Dialects.DUCK_DB.default_executor()


def _table(exec: Executor, name: str, columns: list[str], rows: list[tuple]) -> None:
    exec.execute_raw_sql(
        f"CREATE TABLE {name} ({', '.join(f'{c} INTEGER' for c in columns)})"
    )
    if rows:
        values = ", ".join(
            "(" + ", ".join("NULL" if v is None else str(v) for v in row) + ")"
            for row in rows
        )
        exec.execute_raw_sql(f"INSERT INTO {name} VALUES {values}")


def _errors(exec: Executor, text: str, scope=ValidationScope.ALL) -> list[str]:
    exec.parse_text(text)
    results = validate_environment(
        exec.environment, scope, exec=exec, generate_only=True
    )
    return [str(r.result) for r in results if r.result]


def _maybe(rng: random.Random, value: int, null_rate: float = 0.15) -> int | None:
    return None if rng.random() < null_rate else value


@pytest.mark.parametrize("seed", SEEDS)
def test_null_check_exact(seed: int):
    rng = random.Random(seed)
    n = rng.choice([5, 150, 250])
    rows = [(i, rng.randint(0, 3), rng.randint(0, 3)) for i in range(n)]
    if rng.random() < 0.6:
        at = rng.randrange(n)
        rows[at] = (rows[at][0], rows[at][1], None)
    if rng.random() < 0.5:
        rows = [(k, None, b) for k, _, b in rows[: n // 2]] + rows[n // 2 :]
    exec = _executor()
    _table(exec, "t", ["k", "a", "b"], rows)
    errors = _errors(
        exec,
        "key k int; property k.a int; property k.b int;\n"
        "datasource t (k, a: ?a, b) grain (k) address t;",
        ValidationScope.DATASOURCES,
    )
    holds = all(b is not None for _, _, b in rows)
    assert (not errors) == holds, errors


def _random_ranges(rng: random.Random) -> list[tuple[int | None, int | None]]:
    ranges: list[tuple[int | None, int | None]] = []
    for _ in range(rng.randint(1, 3)):
        lo, hi = sorted(rng.sample(range(-5, 15), 2))
        ranges.append(
            rng.choice([(lo, hi), (lo, None), (None, hi), (lo, lo)])  # type: ignore[arg-type]
        )
    return ranges


def _render_range(lo: int | None, hi: int | None) -> str:
    if lo is not None and lo == hi:
        return str(lo)
    return f"{'' if lo is None else lo}..{'' if hi is None else hi}"


@pytest.mark.parametrize("seed", SEEDS)
def test_domain_check_exact(seed: int):
    rng = random.Random(seed)
    ranges = _random_ranges(rng)
    rows = [(i, _maybe(rng, rng.randint(-8, 18))) for i in range(rng.randint(1, 12))]
    exec = _executor()
    _table(exec, "t", ["k", "v"], rows)
    spec = ", ".join(_render_range(lo, hi) for lo, hi in ranges)
    errors = _errors(
        exec,
        f"key k int; property k.v int[{spec}];\n"
        "datasource t (k, v: ?v) grain (k) address t;",
        ValidationScope.DATASOURCES,
    )
    holds = all(
        v is None
        or any((lo is None or lo <= v) and (hi is None or v <= hi) for lo, hi in ranges)
        for _, v in rows
    )
    assert (not errors) == holds, (spec, rows, errors)


@pytest.mark.parametrize("seed", SEEDS)
def test_grain_check_exact(seed: int):
    rng = random.Random(seed)
    rows = [
        (_maybe(rng, rng.randint(0, 4)), rng.randint(0, 2), i)
        for i in range(rng.randint(1, 8))
    ]
    exec = _executor()
    _table(exec, "t", ["a", "b", "v"], rows)
    errors = _errors(
        exec,
        "key a int; key b int; property <a, b>.v int;\n"
        "datasource t (a: ?a, b, v) grain (a, b) address t;",
        ValidationScope.DATASOURCES,
    )
    keys = [(a, b) for a, b, _ in rows]
    assert (not errors) == (len(set(keys)) == len(keys)), (rows, errors)


@pytest.mark.parametrize("seed", SEEDS)
def test_completeness_exact(seed: int):
    rng = random.Random(seed)
    sources = []
    for idx in range(rng.randint(2, 3)):
        held = rng.sample(range(6), rng.randint(1, 5))
        sources.append((f"s{idx}", held, rng.random() < 0.6))
    exec = _executor()
    text = "key x int;\n"
    for name, held, complete in sources:
        rows: list[tuple] = [(v,) for v in held]
        if rng.random() < 0.3:
            rows.append((None,))
        _table(exec, name, ["x"], rows)
        text += (
            f"datasource {name} (x: {'' if complete else '~'}?x) "
            f"grain (x) address {name};\n"
        )
    errors = _errors(exec, text, ValidationScope.CONCEPTS)
    held_anywhere = {v for _, held, _ in sources for v in held}
    holds = all(held_anywhere <= set(held) for _, held, complete in sources if complete)
    assert (not errors) == holds, (sources, errors)


@pytest.mark.parametrize("seed", SEEDS)
def test_unique_prop_exact(seed: int):
    rng = random.Random(seed)
    rows = [
        (_maybe(rng, k, 0.25), _maybe(rng, rng.randint(0, 3), 0.25))
        for k in rng.sample(range(10), rng.randint(1, 6))
    ]
    exec = _executor()
    _table(exec, "t", ["k", "u"], rows)
    errors = _errors(
        exec,
        "key k int; unique property k.u int;\n"
        "datasource t (k: ?k, u: ?u) address t;",
        ValidationScope.DATASOURCES,
    )
    groups: dict[Any, set] = {}
    for k, u in rows:
        groups.setdefault(u, set()).add(k)
    holds = all(len(keys) <= 1 for keys in groups.values())
    assert (not errors) == holds, (rows, errors)


@pytest.mark.parametrize("seed", SEEDS)
def test_containment_exact(seed: int):
    rng = random.Random(seed)
    cut = rng.randint(0, 5)
    rows = [(i, _maybe(rng, rng.randint(0, 8))) for i in range(rng.randint(1, 8))]
    exec = _executor()
    _table(exec, "t", ["k", "v"], rows)
    errors = _errors(
        exec,
        "key k int; property k.v int;\n"
        f"partial datasource t (k, v: ?v) grain (k) complete where v > {cut} address t;",
        ValidationScope.DATASOURCES,
    )
    holds = all(v is not None and v > cut for _, v in rows)
    assert (not errors) == holds, (cut, rows, errors)


def test_decimal_value_against_float_bound():
    """`float_bound_rejects_its_decimal`: the bound is the literal, not the double."""
    assert ValueRange(min=0.1, max=0.3).contains(Decimal("0.30"))
    assert not ValueRange(min=0.1, max=0.3).contains(Decimal("0.31"))
    exec = _executor()
    errors = _errors(
        exec,
        "key f numeric(10,2)[0.1..0.3];\n"
        "datasource t (f) grain (f) query '''select 0.3::decimal(10,2) as f''';",
        ValidationScope.DATASOURCES,
    )
    assert not errors, errors
