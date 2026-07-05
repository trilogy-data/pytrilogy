"""Shared harness for the join matrix.

One fixed pair of fact models whose data is deliberately adversarial:

- keys EXCLUSIVE to each side (left 3, right 4) — distinguishes LEFT / FULL /
  accidental INNER narrowing or widening;
- DUPLICATE key rows on each side (left key 1, right key 2) with power-of-two /
  power-of-hundred values — any pre-aggregation fan-out or row loss changes a
  sum detectably;
- a NULL key row on each side — pins null-safe equality (NULL keys are valid
  members and must match NULL = NULL, never silently drop).

Expected rows are COMPUTED from the same row data (python oracle), not
hand-written, so every cell of the matrix shares one source of truth.

Contract for every cell: the generated SQL contains no sentinel
(INVALID_REFERENCE_BUG), and execution returns exactly the oracle rows.
Known-broken cells are strict xfails pointing at their bug write-up.
"""

from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# (id, key, val) — see module docstring for why these shapes. The NULL-key row
# is only included (and the key column only declared `?` nullable) when a cell
# opts into the nullable axis: an undeclared-nullable column with NULL data is
# a *model* error, not a planner case.
LEFT_ROWS: list[tuple[int, int | None, int]] = [
    (1, 1, 1),
    (2, 1, 2),
    (3, 2, 4),
    (4, 3, 8),
]
RIGHT_ROWS: list[tuple[int, int | None, int]] = [
    (1, 1, 100),
    (2, 2, 200),
    (3, 2, 400),
    (4, 4, 800),
]
LEFT_NULL_ROW: tuple[int, int | None, int] = (5, None, 16)
RIGHT_NULL_ROW: tuple[int, int | None, int] = (5, None, 1600)


def _datasource_sql(rows: list[tuple[int, int | None, int]]) -> str:
    parts = []
    for i, k, v in rows:
        key = "cast(null as int)" if k is None else str(k)
        parts.append(f"select {i} i, {key} k, {v} v")
    return " union all ".join(parts)


def _model(
    side: str,
    rows: list[tuple[int, int | None, int]],
    source: str,
    nullable: bool,
) -> str:
    prefix = "?" if nullable else ""
    return (
        f"key {side}_id int;\n"
        f"property {side}_id.{side}_key int;\n"
        f"property {side}_id.{side}_val int;\n"
        f"datasource {source} (i: {side}_id, k: {prefix}{side}_key,"
        f" v: {side}_val) grain ({side}_id)\n"
        f"query '''{_datasource_sql(rows)}''';\n"
    )


def matrix_rows(nullable: bool) -> tuple[list[tuple], list[tuple]]:
    if nullable:
        return LEFT_ROWS + [LEFT_NULL_ROW], RIGHT_ROWS + [RIGHT_NULL_ROW]
    return LEFT_ROWS, RIGHT_ROWS


def write_models(tmp_path: Path, nullable: bool = False) -> Path:
    left_rows, right_rows = matrix_rows(nullable)
    (tmp_path / "left_fact.preql").write_text(_model("l", left_rows, "lsrc", nullable))
    (tmp_path / "right_fact.preql").write_text(
        _model("r", right_rows, "rsrc", nullable)
    )
    return tmp_path


def make_engine(workdir: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workdir)
    )


def run_cell(workdir: Path, query: str) -> list[tuple]:
    """Execute one matrix cell under the matrix contract and return its rows,
    normalized to a canonical sort (None keys last)."""
    engine = make_engine(workdir)
    statements = engine.parse_text(query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
    return sort_rows(rows)


def sort_rows(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=lambda r: tuple((x is None, x) for x in r))


# ---------------------------------------------------------------------------
# Python oracle: aggregate each side per (possibly transformed) join key, then
# relate the two aggregate maps with the requested join semantics. Null-safe
# equality: None is an ordinary key value.
# ---------------------------------------------------------------------------


def aggregate(rows: list[tuple[int, int | None, int]], key_fn=None) -> dict:
    out: dict = {}
    for _, k, v in rows:
        kk = k if key_fn is None or k is None else key_fn(k)
        out[kk] = out.get(kk, 0) + v
    return out


def expected_full(left: dict, right: dict) -> list[tuple]:
    keys = set(left) | set(right)
    return sort_rows([(k, left.get(k), right.get(k)) for k in keys])


def expected_left(left: dict, right: dict) -> list[tuple]:
    return sort_rows([(k, left[k], right.get(k)) for k in left])


def expected_equal(left: dict, right: dict) -> list[tuple]:
    """An EQUAL declaration (non-partial `merge`) narrows to INNER when both
    sides prove complete; on data violating the declaration the violating
    (side-exclusive) rows drop — the ruled semantics for a lying declaration."""
    keys = set(left) & set(right)
    return sort_rows([(k, left[k], right[k]) for k in keys])
