"""String functions (formal/Trilogy/Strings.lean, 0.3.471) on DuckDB, SQLite
and ClickHouse through chdb where it installs, against a Python reference.

- `len`, `substring` and `strpos` count characters, not UTF-8 bytes
  (`byte_length_counts_bytes`, `byte_position_counts_bytes`);
- `substring(s, start, n)` reads positions as DuckDB and SQLite do, for
  literal and computed positions, negative and zero ones included
  (`clamp_exact`, `clickhouse_zero_start`, `clickhouse_negative_count`);
- the substring at `strpos(s, t)` of `len t` characters is `t`
  (`strpos_extract`), and an empty needle is found at 1 (`strpos_empty`);
- `upper` and `lower` map non-ASCII letters (SQLite's are ASCII-only);
- `trim` strips spaces, not tabs; `replace` and `regexp_replace` replace every
  occurrence (`replace_first_only`); `regexp_extract` returns the group asked
  for (`clickhouse_extract_ignores_group`);
- `like '%'` is `is not null`, and an `is not null` beside a sibling that
  rejects NULL is not rendered (`percent_matches_all`, `sibling_rejects_null`).
"""

import random
import re

import pytest

from tests.windows.test_window_laws import CHDB, new_executor, seeds_for
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.parser import parse

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
REGEX_DIALECTS = [Dialects.DUCK_DB, *CHDB]
SEEDS = range(3)
ALPHABET = "aAbBé日 "
ASCII = "aAbB "
NEEDLES = ["a", "é", "b日", "", " ", "zz"]
POSITIONS = [-7, -3, -1, 0, 1, 2, 5, 9]
COUNTS = [-4, -1, 0, 1, 3, 20]

MODEL = """
key id int;
property id.v string;
property id.t string;
property id.a int;
property id.c int;
datasource strings (id: id, v: ?v, t: ?t, a: ?a, c: ?c) grain (id) address strings;
"""

DDL = {
    Dialects.DUCK_DB: "create table strings (id int, v varchar, t varchar, a int, c int)",
    Dialects.SQLITE: "create table strings (id int, v varchar, t varchar, a int, c int)",
    Dialects.CLICKHOUSE: (
        "create table strings (id Int64, v Nullable(String), t Nullable(String),"
        " a Nullable(Int64), c Nullable(Int64)) engine = Memory"
    ),
}

Row = tuple[int, str | None, str | None, int | None, int | None]


def substr(s: str, start: int, count: int) -> str:
    """`Strings.substr`: the window of 1-based positions, clipped to `s`."""
    begin = len(s) + 1 + start if start < 0 else start
    low, high = (begin + count, begin) if count < 0 else (begin, begin + count)
    return s[max(low, 1) - 1 : max(min(high, len(s) + 1) - 1, 0)]


def strpos(s: str, t: str) -> int:
    return s.find(t) + 1


def _string(rng: random.Random, alphabet: str) -> str:
    return "".join(rng.choice(alphabet) for _ in range(rng.randrange(7)))


def _rows(rng: random.Random, alphabet: str = ALPHABET) -> list[Row]:
    rows: list[Row] = []
    for i in range(40):
        v = None if rng.random() < 0.15 else _string(rng, alphabet)
        if rng.random() < 0.15:
            t = None
        elif v and rng.random() < 0.6:
            start = rng.randrange(len(v))
            t = v[start : start + rng.randrange(1, 3)]
        else:
            t = rng.choice(NEEDLES)
        a = None if rng.random() < 0.1 else rng.randint(-8, 9)
        c = None if rng.random() < 0.1 else rng.randint(-5, 8)
        rows.append((i, v, t, a, c))
    return rows


def _executor(dialect: Dialects, rows: list[Row]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists strings")
    executor.execute_raw_sql("drop table if exists copied")
    executor.execute_raw_sql(DDL[dialect])
    literal = executor.generator.render_string_literal

    def value(x: str | int | None) -> str:
        if x is None:
            return "NULL"
        return literal(x) if isinstance(x, str) else str(x)

    values = ", ".join("(" + ", ".join(value(x) for x in row) + ")" for row in rows)
    executor.execute_raw_sql(f"insert into strings values {values}")
    executor.parse_text(MODEL)
    return executor


def _column(executor: Executor, expr: str) -> list:
    got = executor.execute_text(f"select id, {expr} as x order by id asc;")
    return [r[1] for r in got[-1].fetchall()]


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


def _seeded(dialect: Dialects, seed: int) -> list[Row]:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    return _rows(random.Random(seed))


@pytest.mark.parametrize("seed", SEEDS)
def test_length_counts_characters(dialect: Dialects, seed: int) -> None:
    rows = _seeded(dialect, seed)
    executor = _executor(dialect, rows)
    assert _column(executor, "len(v)") == [
        None if v is None else len(v) for _, v, _, _, _ in rows
    ]


@pytest.mark.parametrize("seed", SEEDS)
def test_substring_of_computed_positions(dialect: Dialects, seed: int) -> None:
    rows = _seeded(dialect, seed)
    executor = _executor(dialect, rows)
    assert _column(executor, "substring(v, a, c)") == [
        None if None in (v, a, c) else substr(v, a, c)  # type: ignore[arg-type]
        for _, v, _, a, c in rows
    ]


def test_substring_of_literal_positions(dialect: Dialects) -> None:
    rows = _rows(random.Random(0))
    executor = _executor(dialect, rows)
    for start in POSITIONS:
        for count in COUNTS:
            got = _column(executor, f"substring(v, {start}, {count})")
            want = [None if v is None else substr(v, start, count) for _, v, *_ in rows]
            assert got == want, (start, count)
    assert _column(executor, "substring(v, a, 2)") == [
        None if v is None or a is None else substr(v, a, 2) for _, v, _, a, _ in rows
    ]


@pytest.mark.parametrize("seed", SEEDS)
def test_strpos_finds_the_needle(dialect: Dialects, seed: int) -> None:
    rows = _seeded(dialect, seed)
    executor = _executor(dialect, rows)
    want = [None if None in (v, t) else strpos(v, t) for _, v, t, _, _ in rows]  # type: ignore[arg-type]
    assert _column(executor, "strpos(v, t)") == want
    for needle in NEEDLES:
        assert _column(executor, f"strpos(v, '{needle}')") == [
            None if v is None else strpos(v, needle) for _, v, *_ in rows
        ], needle
    extracted = _column(executor, "substring(v, strpos(v, t), len(t))")
    for (_, v, t, _, _), found, got in zip(rows, want, extracted):
        if found:
            assert got == t, (v, t)


@pytest.mark.parametrize("seed", SEEDS)
def test_case_maps_every_letter(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    # SQLite's upper and lower are ASCII-only (a lead, not fixed here)
    alphabet = ASCII if dialect == Dialects.SQLITE else ALPHABET
    rows = _rows(random.Random(seed), alphabet)
    executor = _executor(dialect, rows)
    vs = [v for _, v, *_ in rows]
    assert _column(executor, "upper(v)") == [
        None if v is None else v.upper() for v in vs
    ]
    assert _column(executor, "lower(v)") == [
        None if v is None else v.lower() for v in vs
    ]


def test_trim_strips_spaces(dialect: Dialects) -> None:
    rows = _rows(random.Random(0), " \taé")
    executor = _executor(dialect, rows)
    vs = [v for _, v, *_ in rows]
    for fn, ref in (("trim", str.strip), ("ltrim", str.lstrip), ("rtrim", str.rstrip)):
        assert _column(executor, f"{fn}(v)") == [
            None if v is None else ref(v, " ") for v in vs
        ], fn


def test_replace_every_occurrence(dialect: Dialects) -> None:
    rows = _rows(random.Random(1))
    executor = _executor(dialect, rows)
    for needle in ("a", "é", "b日"):
        assert _column(executor, f"replace(v, '{needle}', '-')") == [
            None if v is None else v.replace(needle, "-") for _, v, *_ in rows
        ], needle


@pytest.mark.parametrize("dialect", REGEX_DIALECTS, ids=lambda d: d.value)
def test_regexp_replace_every_match(dialect: Dialects) -> None:
    rows = _rows(random.Random(2))
    executor = _executor(dialect, rows)
    for pattern in ("[aA]", "é+", "b."):
        assert _column(executor, f"regexp_replace(v, '{pattern}', '-')") == [
            None if v is None else re.sub(pattern, "-", v) for _, v, *_ in rows
        ], pattern


def _extract(v: str, pattern: str, group: int | None) -> str:
    match = re.search(pattern, v)
    if match is None:
        return ""
    if group is None:
        group = 1 if match.re.groups else 0
    return match.group(group) or ""


@pytest.mark.parametrize("dialect", REGEX_DIALECTS, ids=lambda d: d.value)
def test_regexp_extract_the_group_asked_for(dialect: Dialects) -> None:
    rows = _rows(random.Random(2))
    executor = _executor(dialect, rows)
    for pattern, group in [
        ("([aA]+)(b)", 2),
        ("([aA]+)(b)", 0),
        ("([aA]+)(b)", None),
        ("[aA]b", None),
        ("[aA]b", 0),
    ]:
        args = f"v, '{pattern}'" + ("" if group is None else f", {group}")
        assert _column(executor, f"regexp_extract({args})") == [
            None if v is None else _extract(v, pattern, group) for _, v, *_ in rows
        ], (pattern, group)


def test_a_persisted_copy_keeps_characters(dialect: Dialects) -> None:
    rows = _rows(random.Random(3))
    executor = _executor(dialect, rows)
    executor.execute_text(
        "persist copied into copied from "
        "select id, substring(v, -3, 2) as tail, len(v) as n where v is not null;"
    )
    got = executor.execute_raw_sql("select id, tail, n from copied").fetchall()
    assert sorted(tuple(r) for r in got) == [
        (i, substr(v, -3, 2), len(v)) for i, v, *_ in rows if v is not None
    ]


def test_like_percent_keeps_every_present_string(dialect: Dialects) -> None:
    rows = _rows(random.Random(4))
    executor = _executor(dialect, rows)
    for condition in ("v like '%'", "v ilike '%%'", "like(v, '%')"):
        got = executor.execute_text(f"select id where {condition};")[-1].fetchall()
        assert sorted(r[0] for r in got) == [i for i, v, *_ in rows if v is not None]


PLANNED = """
key id int;
property id.v string;
datasource s (id: id, v: ?v) grain (id) address s;
"""


@pytest.mark.parametrize(
    "condition, where",
    [
        ("v like '%'", '"s"."v" is not null'),
        ("v = 'b' and v is not null", '"s"."v" = \'b\''),
        ("v is not null and v like 'a%'", "\"s\".\"v\" like 'a%' ESCAPE '\\'"),
        ("(v = 'a' or v = 'b') and v is not null", '"s"."v" = \'a\' or'),
        ("v is not null and coalesce(v, 'x') = 'x'", '"s"."v" is not null and'),
    ],
)
def test_planned_null_rejection(condition: str, where: str) -> None:
    env = Environment()
    parse(PLANNED, env)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = " ".join(executor.generate_sql(f"select id where {condition};")[-1].split())
    clause = sql[sql.index("WHERE") :]
    assert where in clause, clause
    if "coalesce" not in condition and "is not null" in condition:
        assert "is not null" not in clause, clause
