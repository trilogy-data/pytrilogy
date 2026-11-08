"""`like`, `ilike` and `contains` (formal/Trilogy/StringMatch.lean, 0.3.470),
on DuckDB, SQLite and ClickHouse through chdb where it installs, against the
reference matcher in `trilogy.core.like_patterns`.

- one pattern language: `%`, `_`, and `\\` making the next character literal
  (`escaped_literal_exact`), whatever the engine's own default escape is
  (`unescaped_wildcard_matches_more`);
- `like` keeps case and `ilike` folds it, SQLite included
  (`sqlite_like_folds_case`); `contains` is a case-folded substring test
  (`contains_is_folded_infix`, `clickhouse_contains_keeps_case`);
- each is NULL on a NULL operand, so a `not like` keeps no NULL row;
- a pattern with no wildcard is its string's equality, and is planned as one
  (`exact_pattern_is_eq`);
- a string keeps its backslashes when written to a persisted copy and read
  back (`backslash_literal_halved`).
"""

import random

import pytest

from tests.windows.test_window_laws import CHDB, new_executor, seeds_for
from trilogy import Dialects, Executor
from trilogy.core.like_patterns import matches
from trilogy.core.models.environment import Environment
from trilogy.parser import parse

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(3)
ALPHABET = "aAbB%_\\[*?"
NEEDLES = ["a", "B", "%", "_", "\\", "[", "*", "", "aB", "b%"]

MODEL = """
key id int;
property id.v string;
property id.p string;
datasource strings (id: id, v: ?v, p: ?p) grain (id) address strings;
"""

DDL = {
    Dialects.DUCK_DB: "create table strings (id int, v varchar, p varchar)",
    Dialects.SQLITE: "create table strings (id int, v varchar, p varchar)",
    Dialects.CLICKHOUSE: (
        "create table strings (id Int64, v Nullable(String), p Nullable(String))"
        " engine = Memory"
    ),
}


def _string(rng: random.Random, length: int) -> str:
    return "".join(rng.choice(ALPHABET) for _ in range(rng.randrange(length)))


def _pattern(rng: random.Random) -> str:
    """A pattern that does not end in a lone escape, which engines refuse."""
    p = _string(rng, 5)
    trailing = len(p) - len(p.rstrip("\\"))
    return p + "a" if trailing % 2 else p


def _near(rng: random.Random, value: str) -> str:
    """A pattern close to `value`: each character kept (escaped when it is
    special), case-swapped, or replaced by a wildcard."""
    out = []
    for c in value:
        roll = rng.random()
        if roll < 0.15:
            out.append(rng.choice("%_"))
        elif roll < 0.3:
            out.append(c.swapcase())
        elif c in "%_\\" and roll < 0.85:
            out.append("\\" + c)
        else:
            out.append(c)
    p = "".join(out)
    trailing = len(p) - len(p.rstrip("\\"))
    return p + "a" if trailing % 2 else p


def _rows(rng: random.Random) -> list[tuple[int, str | None, str | None]]:
    rows = []
    for i in range(40):
        v = None if rng.random() < 0.15 else _string(rng, 6)
        if rng.random() < 0.15:
            p = None
        elif v is not None and rng.random() < 0.7:
            p = _near(rng, v)
        else:
            p = _pattern(rng)
        rows.append((i, v, p))
    return rows


def _executor(dialect: Dialects, rows: list[tuple]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists strings")
    executor.execute_raw_sql("drop table if exists copied")
    executor.execute_raw_sql(DDL[dialect])
    literal = executor.generator.render_string_literal
    values = ", ".join(
        "("
        + ", ".join([str(i)] + ["NULL" if s is None else literal(s) for s in (v, p)])
        + ")"
        for i, v, p in rows
    )
    executor.execute_raw_sql(f"insert into strings values {values}")
    executor.parse_text(MODEL)
    return executor


def _ids(executor: Executor, text: str) -> list[int]:
    return sorted(r[0] for r in executor.execute_text(text)[-1].fetchall())


def _like(v: str | None, p: str | None, negate: bool, fold: bool) -> bool:
    if v is None or p is None:
        return False
    return matches(v, p, fold) != negate


def _literal_patterns(rng: random.Random, rows: list[tuple]) -> list[str]:
    values = [v for _, v, _ in rows if v]
    candidates = [_pattern(rng) for _ in range(6)] + [
        _near(rng, rng.choice(values)) for _ in range(10)
    ]
    # a Trilogy string literal cannot end in a backslash
    return [p for p in candidates if not p.endswith("\\")]


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


OPERATORS = {
    "like": (False, False),
    "not like": (True, False),
    "ilike": (False, True),
    "not ilike": (True, True),
}


@pytest.mark.parametrize("seed", SEEDS)
def test_literal_patterns(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    rng = random.Random(seed)
    rows = _rows(rng)
    executor = _executor(dialect, rows)
    for pattern in _literal_patterns(rng, rows):
        for op, (negate, fold) in OPERATORS.items():
            want = [i for i, v, _ in rows if _like(v, pattern, negate, fold)]
            got = _ids(executor, f"select id where v {op} '{pattern}';")
            assert got == want, (op, pattern)


@pytest.mark.parametrize("seed", SEEDS)
def test_column_patterns(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    for op, (negate, fold) in OPERATORS.items():
        want = [i for i, v, p in rows if _like(v, p, negate, fold)]
        assert _ids(executor, f"select id where v {op} p;") == want, op


@pytest.mark.parametrize("seed", SEEDS)
def test_contains_is_a_folded_substring(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    for needle in NEEDLES:
        if needle.endswith("\\"):
            continue
        want = [
            (i, None if v is None else needle.lower() in v.lower()) for i, v, _ in rows
        ]
        got = executor.execute_text(
            f"select id, contains(v, '{needle}') as c order by id asc;"
        )[-1].fetchall()
        assert [(r[0], None if r[1] is None else bool(r[1])) for r in got] == want
        assert _ids(executor, f"select id where contains(v, '{needle}');") == [
            i for i, hit in want if hit
        ], needle


def test_a_match_is_null_on_a_null_operand(dialect: Dialects) -> None:
    rows = _rows(random.Random(0))
    executor = _executor(dialect, rows)
    got = executor.execute_text(
        "select id, v like 'a%' as l, v ilike 'a%' as il order by id asc;"
    )[-1].fetchall()
    for (i, v, _), row in zip(rows, got):
        assert row[0] == i
        if v is None:
            assert row[1] is None and row[2] is None
        else:
            assert bool(row[1]) == matches(v, "a%")
            assert bool(row[2]) == matches(v, "a%", True)


PARTITIONED = """
key id int;
property id.city string;
property id.v int;
datasource all_rows (id: id, city: city, v: v) grain (id) address all_rows;
partial datasource boston (id: id, city: city, v: v) grain (id)
complete where city = 'Bos%ton' address boston_rows;
"""


@pytest.mark.parametrize(
    "condition, partition",
    [
        ("city = 'Bos%ton'", True),
        ("city like 'Bos\\%ton'", True),
        ("like(city, 'Bos\\%ton')", True),
        ("city like 'Bos%ton'", False),
        ("city ilike 'Bos\\%ton'", False),
    ],
)
def test_an_exact_pattern_is_an_equality(condition: str, partition: bool) -> None:
    env = Environment()
    parse(PARTITIONED, env)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(f"select id, v where {condition};")[-1]
    assert ("boston_rows" in sql) == partition, sql
    assert ("all_rows" in sql) != partition, sql


def test_not_like_an_exact_pattern_is_inequality() -> None:
    env = Environment()
    parse(PARTITIONED, env)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql("select id where city not like 'Bos\\_x';")[-1]
    assert "!= 'Bos_x'" in sql, sql


def test_a_persisted_copy_keeps_its_backslashes(dialect: Dialects) -> None:
    rows = _rows(random.Random(1))
    executor = _executor(dialect, rows)
    executor.execute_text(
        "persist copied into copied from "
        "select id, v, concat(v, '\\n\\x') as w where v like '%\\%%';"
    )
    want = [(i, v, v + "\\n\\x") for i, v, _ in rows if v is not None and "%" in v]
    got = executor.execute_raw_sql("select id, v, w from copied").fetchall()
    assert sorted(tuple(r) for r in got) == want
    assert _ids(executor, "select id where w like '%\\\\n\\\\x';") == [
        i for i, _, _ in rows
    ]
    value = next(v for _, v, _ in rows if v and "\\" in v and not v.endswith("\\"))
    assert _ids(executor, f"select id where v = '{value}';") == [
        i for i, v, _ in rows if v == value
    ]


@pytest.mark.parametrize("value", ["a\\b", "a\\\\n", "\\x%_", "it's"])
def test_a_string_literal_is_its_value(dialect: Dialects, value: str) -> None:
    executor = new_executor(dialect)
    quoted = '"' + value + '"' if "'" in value else "'" + value + "'"
    ((got,),) = executor.execute_text(f"select {quoted} as x;")[-1].fetchall()
    assert got == value
