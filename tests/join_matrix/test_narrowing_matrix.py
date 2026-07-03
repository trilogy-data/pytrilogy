"""Narrowing proofs: declared domain relations drive join narrowing
(docs/subset_union_join_design.md).

These cells use HONEST data — both sides carry the same key set — because
narrowing is only sound when the declaration holds; the adversarial rows of
the main matrix violate an EQUAL declaration by construction (which is why
`narrow_equal_domain_joins` defaults off until the SUBSET/UNION default flip).

Pinned contracts:
- EQUAL (non-partial `merge`) + narrowing flag: FULL renders INNER, rows
  unchanged vs the un-narrowed plan.
- UNION (`union join`) never narrows, flag or not — the declaration says the
  domains may diverge even when today's data happens to match.
- flag off (default): EQUAL keeps today's FULL.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import (
    aggregate,
    expected_full,
    make_engine,
    sort_rows,
)
from trilogy.constants import CONFIG
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


def _run(workdir: Path, query: str, narrow: bool) -> tuple[str, list[tuple]]:
    CONFIG.optimizations.narrow_equal_domain_joins = narrow
    clear_parse_cache()
    try:
        engine = make_engine(workdir)
        statements = engine.parse_text(query)
        sql = engine.generate_sql(statements[-1])[-1]
        rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
    finally:
        CONFIG.optimizations.narrow_equal_domain_joins = False
        clear_parse_cache()
    return sql, sort_rows(rows)


def _oracle() -> list[tuple]:
    return expected_full(aggregate(HONEST_LEFT), aggregate(HONEST_RIGHT))


EQUAL_QUERY = HEAD + "merge b.r_key into a.l_key;\n" + SELECT
UNION_QUERY = HEAD + UNION_SELECT + " union join a.l_key = b.r_key;"


def test_equal_declaration_narrows_to_inner(tmp_path: Path):
    sql, rows = _run(_write_models(tmp_path), EQUAL_QUERY, narrow=True)
    assert "FULL JOIN" not in sql, sql
    assert "INNER JOIN" in sql, sql
    assert rows == _oracle(), rows


def test_equal_declaration_default_keeps_full(tmp_path: Path):
    sql, rows = _run(_write_models(tmp_path), EQUAL_QUERY, narrow=False)
    assert "FULL JOIN" in sql, sql
    assert rows == _oracle(), rows


@pytest.mark.parametrize("narrow", [False, True])
def test_union_declaration_never_narrows(tmp_path: Path, narrow: bool):
    sql, rows = _run(_write_models(tmp_path), UNION_QUERY, narrow=narrow)
    assert "FULL JOIN" in sql, sql
    assert rows == _oracle(), rows


def test_equal_narrowing_row_identical(tmp_path: Path):
    """The perf contract is row-neutral on honest data: narrowed and
    un-narrowed plans return identical rows."""
    workdir = _write_models(tmp_path)
    _, base = _run(workdir, EQUAL_QUERY, narrow=False)
    _, narrowed = _run(workdir, EQUAL_QUERY, narrow=True)
    assert base == narrowed
