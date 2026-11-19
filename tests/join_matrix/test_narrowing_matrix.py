"""Narrowing proofs: declared domain relations drive join narrowing
(docs/subset_union_join_design.md).

These cells use HONEST data — both sides carry the same key set — because
narrowing is only row-neutral when the declaration holds; the adversarial
rows of the main matrix violate an EQUAL declaration by construction (those
cells rule intersection semantics — lying declaration = author error).

Pinned contracts:
- EQUAL (non-partial `merge`) is one key: it renders INNER, with the
  oracle's rows.
- `equal join a = b` is the same declaration scoped to one query and narrows
  the same way.
- UNION (`union join`) never narrows — the declaration says the domains may
  diverge even when today's data happens to match.
"""

from pathlib import Path

from tests.join_matrix.harness import (
    aggregate,
    expected_full,
    make_engine,
    sort_rows,
)
from trilogy.parsing.parse_engine_v2 import clear_parse_cache

# Same key set {1, 2, 3} on both sides (duplicates kept to guard fan-out).
HONEST_LEFT: list[tuple[int, int | None, int]] = [
    (1, 1, 1),
    (2, 1, 2),
    (3, 2, 4),
    (4, 3, 8),
]
HONEST_RIGHT: list[tuple[int, int | None, int]] = [
    (1, 1, 100),
    (2, 2, 200),
    (3, 2, 400),
    (4, 3, 800),
]

HEAD = "import left_fact as a;\nimport right_fact as b;\n"
SELECT = "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv;"
UNION_SELECT = "select a.l_key, sum(a.l_val) as lv, sum(b.r_val) as rv"


def _write_models(tmp_path: Path) -> Path:
    from tests.join_matrix.harness import _model

    (tmp_path / "left_fact.preql").write_text(
        _model("l", HONEST_LEFT, "lsrc", nullable=False)
    )
    (tmp_path / "right_fact.preql").write_text(
        _model("r", HONEST_RIGHT, "rsrc", nullable=False)
    )
    return tmp_path


def _run(workdir: Path, query: str) -> tuple[str, list[tuple]]:
    clear_parse_cache()
    try:
        engine = make_engine(workdir)
        statements = engine.parse_text(query)
        sql = engine.generate_sql(statements[-1])[-1]
        rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
    finally:
        clear_parse_cache()
    return sql, sort_rows(rows)


def _oracle() -> list[tuple]:
    return expected_full(aggregate(HONEST_LEFT), aggregate(HONEST_RIGHT))


EQUAL_QUERY = HEAD + "merge b.r_key into a.l_key;\n" + SELECT
EQUAL_JOIN_QUERY = HEAD + UNION_SELECT + " equal join a.l_key = b.r_key;"
UNION_QUERY = HEAD + UNION_SELECT + " union join a.l_key = b.r_key;"


def test_equal_declaration_narrows_to_inner(tmp_path: Path):
    sql, rows = _run(_write_models(tmp_path), EQUAL_QUERY)
    assert "FULL JOIN" not in sql, sql
    assert "INNER JOIN" in sql, sql
    assert rows == _oracle(), rows


def test_scoped_equal_join_narrows_to_inner(tmp_path: Path):
    sql, rows = _run(_write_models(tmp_path), EQUAL_JOIN_QUERY)
    assert "FULL JOIN" not in sql, sql
    assert "INNER JOIN" in sql, sql
    assert rows == _oracle(), rows


def test_union_declaration_never_narrows(tmp_path: Path):
    sql, rows = _run(_write_models(tmp_path), UNION_QUERY)
    assert "FULL JOIN" in sql, sql
    assert rows == _oracle(), rows
