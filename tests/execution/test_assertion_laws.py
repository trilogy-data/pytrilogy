"""Datasource assertion laws (formal/Trilogy/Assert.lean), checked on random builds.

`check_exact`: a build passes its assertions exactly when every group of either
side passes the documented rule (`judge`): a vanished group fails, a group with
no history passes, a NULL term fails on the new side and passes on the prior
one, and otherwise the condition decides, with its `by` keys read as the
group's own values. That needs every substituted value to survive its literal
round trip and the engine's TRUE to be read as true on every driver.
"""

import math
import random
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.models.environment import Environment
from trilogy.execution.assertions import DatasourceAssertionError
from trilogy.parsing.render import (
    decimal_literal,
    float_literal,
    string_literal,
)

SEEDS = range(40)
GROUPS = [None, 1, 2, 3]
RATIO = {1: 0.9, 2: 0.2}
FLOOR = {1: 4, 3: 2}

KEYED = """
key id int;
property id.g int?;
root datasource raw (id: id, g: g) grain (id) address raw;
datasource built (id: id, g: g)
grain (id)
address built
assert count(id) by g >= case when g = 1 then 0.9 when g = 2 then 0.2 else 0.5 end * prior(count(id) by g)
assert count(id) by g >= case when g = 1 then 4 when g = 3 then 2 else 1 end
;
"""


def _ratio(g: int | None) -> float:
    return RATIO.get(g, 0.5) if g is not None else 0.5


def _floor(g: int | None) -> int:
    return FLOOR.get(g, 1) if g is not None else 1


def _counts(rows: list[int | None]) -> dict[int | None, int]:
    out: dict[int | None, int] = {}
    for g in rows:
        out[g] = out.get(g, 0) + 1
    return out


def _spec(new: dict[int | None, int], old: dict[int | None, int]) -> bool:
    for g in set(new) | set(old):
        if g not in new:
            return False
        if new[g] < _floor(g):
            return False
        if g in old and new[g] < _ratio(g) * old[g]:
            return False
    return True


def _load(executor: Executor, rows: list[int | None]) -> None:
    executor.execute_raw_sql("DELETE FROM raw")
    values = ", ".join(
        f"({i}, {'NULL' if g is None else g})" for i, g in enumerate(rows)
    )
    executor.execute_raw_sql(f"INSERT INTO raw VALUES {values}")


def _passes(executor: Executor) -> bool:
    try:
        executor.update_datasource(executor.environment.datasources["built"])
    except DatasourceAssertionError:
        return False
    return True


def _random_rows(rng: random.Random) -> list[int | None]:
    present = [g for g in GROUPS if rng.random() < 0.85]
    return [g for g in present for _ in range(rng.randint(1, 6))]


@pytest.mark.parametrize("seed", SEEDS)
def test_check_exact(seed: int, tmp_path: Path) -> None:
    rng = random.Random(seed)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql("CREATE TABLE raw (id INTEGER, g INTEGER)")
    executor.parse_text(KEYED)
    first = [g for g in GROUPS for _ in range(rng.randint(4, 8))]
    _load(executor, first)
    assert _passes(executor)
    second = _random_rows(rng)
    _load(executor, second)
    assert _passes(executor) == _spec(_counts(second), _counts(first))


def test_key_threshold_is_the_groups_own() -> None:
    """`unsubstituted_key_reads_another_group`: group 1's 10 rows meet its 5, and
    group 0's 10 rows miss its 80, whichever row a constant query would see."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE raw AS SELECT i AS id, i % 2 AS g FROM range(20) t(i)"
    )
    executor.parse_text(
        "key id int; property id.g int;"
        " root datasource raw (id: id, g: g) grain (id) address raw;"
        " datasource built (id: id, g: g) grain (id) address built"
        " assert count(id) by g >= case when g = 0 then 80 else 5 end;"
    )
    with pytest.raises(DatasourceAssertionError) as info:
        executor.update_datasource(executor.environment.datasources["built"])
    assert "for g = 0" in str(info.value)
    assert "for g = 1" not in str(info.value)


TYPED = [
    (
        "numeric(38,2)",
        "DECIMAL(38,2)",
        Decimal("5000000000000000.00"),
        Decimal("5000000000000000.01"),
    ),
    ("float", "DOUBLE", 1e-05, 2e-05),
    ("float", "DOUBLE", 1e17, 2e17),
    ("string", "VARCHAR", "O'Brien", "O'Brian"),
    ("string", "VARCHAR", 'say "hi"', 'say "ho"'),
    ("string", "VARCHAR", 'it\'s "x"', 'it\'s "y"'),
    ("date", "DATE", date(2024, 1, 1), date(2024, 1, 2)),
    ("datetime", "TIMESTAMP", datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 2)),
]


def _sql_literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    return str(value)


@pytest.mark.parametrize("changed", [False, True])
@pytest.mark.parametrize("trilogy_type, sql_type, before, after", TYPED)
def test_literal_round_trip_is_exact(
    trilogy_type: str, sql_type: str, before: object, after: object, changed: bool
) -> None:
    """`lossy_literal_passes_a_change`: an unchanged value passes an equality
    with its prior and a changed one fails it, for every value type."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(f"CREATE TABLE raw (id INTEGER, v {sql_type})")
    executor.execute_raw_sql(f"INSERT INTO raw VALUES (1, {_sql_literal(before)})")
    executor.parse_text(
        f"key id int; property id.v {trilogy_type};"
        " root datasource raw (id: id, v: v) grain (id) address raw;"
        " datasource built (id: id, v: v) grain (id) address built"
        " assert max(v) = prior(max(v));"
    )
    built = executor.environment.datasources["built"]
    executor.update_datasource(built)
    if changed:
        executor.execute_raw_sql(f"UPDATE raw SET v = {_sql_literal(after)}")
    assert _passes(executor) is not changed


@pytest.mark.parametrize("floor, holds", [(1, True), (100, False)])
def test_engine_true_is_read_on_sqlite(floor: int, holds: bool) -> None:
    """`truthy_exact`: SQLite answers the constant query with 1, not True."""
    executor = Dialects.SQLITE.default_executor()
    executor.execute_raw_sql("CREATE TABLE raw (id INTEGER)")
    executor.execute_raw_sql("INSERT INTO raw VALUES (1), (2), (3)")
    executor.parse_text(
        "key id int; root datasource raw (id: id) grain (id) address raw;"
        " datasource built (id: id) grain (id) address built"
        f" assert count(id) >= {floor};"
    )
    assert _passes(executor) is holds


STRINGS = ["plain", "it's", 'say "hi"', 'it\'s "x"', "a\\b", "ends\\", "\\'", "x${y}"]


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def backend(request, monkeypatch: pytest.MonkeyPatch) -> ParserBackend:
    monkeypatch.setattr(CONFIG, "parser_backend", request.param)
    return request.param


def _parsed_constant(text: str) -> object:
    env = Environment()
    env.parse(f"const a <- {text};")
    lineage = env.concepts["a"].lineage
    assert lineage is not None
    return lineage.arguments[0]


@pytest.mark.parametrize("value", STRINGS)
def test_string_literal_parses_back(value: str, backend: ParserBackend) -> None:
    if backend == ParserBackend.LARK and "${" in value:
        pytest.skip("lark reserves `${` for interpolation")
    assert _parsed_constant(string_literal(value)) == value


def test_string_without_a_literal_raises() -> None:
    with pytest.raises(ValueError):
        string_literal("'''\"")


@pytest.mark.parametrize(
    "value", [1e-05, 1e17, -2.5e-08, 0.1, 3.0, 1.2345678901234568e17]
)
def test_float_literal_parses_back(value: float, backend: ParserBackend) -> None:
    assert _parsed_constant(float_literal(value)) == value


@pytest.mark.parametrize(
    "value",
    [Decimal("5000000000000000.01"), Decimal("-0.001"), Decimal("1E+3"), Decimal(0)],
)
def test_decimal_literal_parses_back(value: Decimal, backend: ParserBackend) -> None:
    parsed = _parsed_constant(decimal_literal(value))
    assert isinstance(parsed, Decimal) and parsed == value


@pytest.mark.parametrize("value", [math.inf, -math.inf])
def test_non_finite_float_runs(value: float) -> None:
    assert _parsed_constant(float_literal(value)) == value
    executor = Dialects.DUCK_DB.default_executor()
    result = executor.execute_text(
        f"select {float_literal(value)} = {float_literal(value)} as ok;"
    )
    assert result[-1].fetchall() == [(True,)]
