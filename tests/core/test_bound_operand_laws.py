"""A cast or a clamped substring evaluates its operand once
(formal/Trilogy/Sharing.lean, 0.3.476) on DuckDB, SQLite and ClickHouse
through chdb where it installs.

- a rendering that names its operand k times grows a nested operand k-fold
  per level (`inline_size`, `inline_nested`): SQLite's datetime read named it
  756 times, so `cast(cast(cast(s as datetime) as string) as datetime)` was
  77 MB of SQL (`sqlite_instant_blowup`); bound once, the SQL grows by the
  operand's size per level (`bound_size`, `bound_nested`);
- a volatile operand named k times is drawn k times, so a float's written
  text mixed digits from two draws (`inline_redraws`, `bind_draws_once`);
- SQLite's flattener inlines each bound level back into the next, so the
  binding stays a subquery (an OFFSET keeps it one) and the compiled program
  stays linear in the SQL;
- the SQLite reads parse on SQLite before 3.46, whose parser stack is 100
  frames deep, when one is installed (Ubuntu 24.04's `/usr/bin/python3`).
"""

import json
import random
import sqlite3
import subprocess
from pathlib import Path

import pytest

from tests.core.test_instant_read_laws import _strings
from tests.core.test_text_read_laws import _executor
from tests.windows.test_window_laws import CHDB
from trilogy import Dialects, Environment
from trilogy.constants import Rendering
from trilogy.core.casts import float_to_text
from trilogy.parser import parse
from trilogy.render import get_dialect_generator

MODEL = """
key id int;
property id.v string;
property id.i int;
datasource s (id: id, v: ?v, i: ?i) grain (id) address s;
"""
OPERANDS = tuple(f"concat(v, '{'x' * n}')" for n in (100, 200))
READS = ["int", "bigint", "numeric(12,2)", "float", "date", "datetime", "timestamp"]
CASES = {
    **{f"read {t}": f"cast({{o}} as {t})" for t in READS},
    "write float": "cast(len({o}) * 1.5 as string)",
    "write datetime": "cast(cast({o} as datetime) as string)",
    "write timestamp": "cast(cast({o} as timestamp) as string)",
    "substring": "substring({o}, i, 3)",
    "substring bound": "substring(v, len({o}) - 3, i)",
}
# how many times each rendering names its operand; DuckDB computes a
# repeated expression once, as ClickHouse does, where a lambda costs more
NAMED = {
    Dialects.SQLITE: {},
    Dialects.CLICKHOUSE: {
        "read int": 3,
        "read bigint": 3,
        "read numeric(12,2)": 3,
        "read float": 2,
        "write float": 2,
    },
    Dialects.DUCK_DB: {
        "read int": 2,
        "read bigint": 2,
        "read numeric(12,2)": 2,
        "read float": 2,
        "read date": 2,
        "read datetime": 4,
        "read timestamp": 4,
        "write datetime": 4,
        "write timestamp": 4,
    },
}


def _select(dialect: Dialects, expr: str) -> str:
    env = Environment()
    _, statements = parse(MODEL + f"select id, {expr} as x;", env)
    renderer = get_dialect_generator(dialect, rendering=Rendering(parameters=False))
    return renderer.compile_statements(*renderer.generate_queries(env, statements))[-1]


def _named(dialect: Dialects, template: str) -> float:
    short, long = (len(_select(dialect, template.format(o=o))) for o in OPERANDS)
    return (long - short) / 100


@pytest.mark.parametrize("dialect", list(NAMED))
@pytest.mark.parametrize("case", list(CASES))
def test_operand_named_once(dialect: Dialects, case: str) -> None:
    named = _named(dialect, CASES[case])
    assert abs(named - round(named)) < 0.05
    assert round(named) == NAMED[dialect].get(case, 1)


@pytest.mark.parametrize("dialect", [Dialects.SQLITE, Dialects.CLICKHOUSE])
def test_nested_casts_grow_linearly(dialect: Dialects) -> None:
    """Each level adds the read's own size, not a multiple of the operand's
    (`bound_nested`); on main SQLite's three levels were 77 MB. DuckDB's read
    names its operand four times, so its levels still multiply."""
    read = "cast(cast({o} as datetime) as string)"
    once = len(_select(dialect, read.format(o="v")))
    nested = len(_select(dialect, read.format(o=read.format(o=read.format(o="v")))))
    assert nested < 3 * once + 1000


def _significant(text: str) -> int:
    mantissa = text.lstrip("-").split("e")[0].replace(".", "")
    return len(mantissa.strip("0"))


def _shortest(dialect: Dialects, text: str | None) -> bool:
    """`repr`'s text; SQLite's printf is exact to 15 digits only, so past 15
    it is held to `repr`'s layout and 16 or 17 digits."""
    if text is None:
        return False
    expected = float_to_text(float(text))
    if dialect != Dialects.SQLITE or _significant(expected) <= 15:
        return text == expected
    return _significant(text) <= 17 and ("e" in text) == ("e" in expected)


@pytest.mark.parametrize("dialect", [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB])
def test_volatile_operand_drawn_once(dialect: Dialects) -> None:
    """A float's text is the shortest text of one draw (`bind_draws_once`):
    SQLite chose the digit count and the layout from one draw and printed
    another."""
    executor = _executor(dialect, [str(k) for k in range(300)])
    draw = "random(1000000) / 7.0 * 10.0 ** (random(40) - 20) + id * 0"
    rows = executor.execute_text(f"select id, cast({draw} as string) as x;")
    texts = [r[1] for r in rows[-1].fetchall()]
    assert len(texts) == 300
    assert [t for t in texts if not _shortest(dialect, t)] == []


def test_column_free_operand_drawn_per_row() -> None:
    """SQLite evaluates a subquery with no column once per query, so an
    operand with none is left inline and still drawn on every row."""
    executor = _executor(Dialects.SQLITE, [str(k) for k in range(50)])
    rows = executor.execute_text(
        "select id, cast(random(1000000) / 7.0 as string) as x;"
    )
    assert len({r[1] for r in rows[-1].fetchall()}) > 1


def _sqlite_sql(expr: str) -> str:
    executor = _executor(Dialects.SQLITE, [""])
    return executor.generate_sql(f"select id, {expr} as x order by id asc;")[-1]


@pytest.mark.parametrize("target", ["date", "datetime", "timestamp", "int"])
def test_sqlite_program_linear_in_sql(target: str) -> None:
    """A flattened binding inlines every level back into the next: the
    datetime read compiled to 1,866 instructions where bound it is 291."""
    sql = _sqlite_sql(f"cast(s as {target})")
    conn = sqlite3.connect(":memory:")
    conn.execute("create table reads (id int, s varchar, i int)")
    assert len(conn.execute(f"EXPLAIN {sql}").fetchall()) * 4 < len(sql)


OLD_SQLITE = Path("/usr/bin/python3")
RUN = """
import json, sqlite3, sys
strings, sql = json.load(sys.stdin)
conn = sqlite3.connect(":memory:")
conn.execute("create table reads (id int, s varchar, i int)")
conn.executemany("insert into reads values (?, ?, NULL)", enumerate(strings))
print(json.dumps([r[1] for r in conn.execute(sql).fetchall()]))
"""


def _old_sqlite_version() -> tuple[int, ...] | None:
    if not OLD_SQLITE.exists():
        return None
    script = "import sqlite3; print(sqlite3.sqlite_version)"
    done = subprocess.run(
        [str(OLD_SQLITE), "-I", "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    if done.returncode:
        return None
    return tuple(int(p) for p in done.stdout.strip().split("."))


@pytest.mark.parametrize(
    "expr",
    [
        "cast(s as datetime)",
        "cast(s as timestamp)",
        "cast(s as date)",
        "cast(cast(s as datetime) as string)",
        "cast(cast(cast(s as datetime) as string) as datetime)",
    ],
)
def test_sqlite_reads_parse_before_3_46(expr: str) -> None:
    version = _old_sqlite_version()
    if version is None or version >= (3, 46):
        pytest.skip("no SQLite before 3.46 installed")
    strings = _strings(random.Random(0))
    executor = _executor(Dialects.SQLITE, strings)
    expected = [
        r[1]
        for r in executor.execute_text(f"select id, {expr} as x order by id asc;")[
            -1
        ].fetchall()
    ]
    done = subprocess.run(
        [str(OLD_SQLITE), "-I", "-c", RUN],
        input=json.dumps([strings, _sqlite_sql(expr)]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout) == expected
