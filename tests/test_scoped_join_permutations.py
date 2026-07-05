"""Row-level permutation matrix for scoped joins — the oracle for the
substitution-vs-identity boundary.

Two mechanisms relate scoped-join keys (see ``Factory.__init__`` in build.py):

* **substitution** — used when the key equality holds on EVERY output row
  (global ``merge``, the FULL canonical-key registry, dependent-grain collapse).
  One logical key renders from one physical column.
* **identity + pseudonym + coalesce** — used when the correspondence MAY be
  absent on some rows (LEFT / FULL across distinct physical columns). The output
  key is a row-by-row ``coalesce`` of both columns, so both must survive.

The chosen mechanism is a function of JOIN TYPE, not key kind: a root-keyed FULL
still needs coalesce, a derived-keyed merge still substitutes. These tests pin
that for ROOT keys across populations. (Query-scoped ``inner join`` is no longer
supported — express an intersection as a LEFT/FULL join plus a filter.)

Coverage elsewhere (not duplicated here):
* derived/BASIC keys, N-way, chained, mixed, disjoint groups — test_join_merge_parity.py
* rowset keys (distinct + shared base) — test_scoped_join_rowset_outer_blend.py, test_scoped_join.py
* build-env contracts + FULL registry — test_scoped_join.py

Projecting the *source* side of a FULL scoped join on a ROOT key coalesces
correctly (FULL binding-keyed sources stay on the identity path, so both members
coalesce regardless of which is projected). See
``test_full_source_side_projection_coalesces``.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

A_MODEL = (
    "key kid int;\nproperty kid.m_a float;\n"
    "datasource at (k: kid, m: m_a) grain (kid) address a_tbl;\n"
)
B_MODEL = (
    "key kid int;\nproperty kid.m_b float;\n"
    "datasource bt (k: kid, m: m_b) grain (kid) address b_tbl;\n"
)
# b with a (string) property on its key, for the grain-seam tests.
B_LABEL_MODEL = (
    "key kid int;\nproperty kid.label string;\n"
    "datasource bt (k: kid, l: label) grain (kid) address b_tbl;\n"
)

_IMPORTS = "import a as a;\nimport b as b;\n"


def _values(rows: list[tuple]) -> str:
    return ",".join("(" + ",".join(repr(v) for v in r) + ")" for r in rows)


def _load(eng: Executor, ddl: str, rows: list[tuple]) -> None:
    eng.execute_raw_sql(ddl)
    if rows:
        tbl = ddl.split()[2]
        eng.execute_raw_sql(f"insert into {tbl} values {_values(rows)}")


def _engine(tmp_path: Path, b_model: str) -> Executor:
    (tmp_path / "a.preql").write_text(A_MODEL)
    (tmp_path / "b.preql").write_text(b_model)
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )


def _rows(eng: Executor, tmp_path: Path, text: str) -> list[tuple]:
    eng.environment = Environment(working_path=tmp_path)
    return [tuple(r) for r in eng.execute_text(text)[-1].fetchall()]


# (a_rows, b_rows) per population shape. m_a / m_b are floats.
POPULATIONS: dict[str, tuple[list[tuple], list[tuple]]] = {
    "partial_overlap": ([(1, 10.0), (2, 20.0)], [(2, 200.0), (3, 300.0)]),
    "disjoint": ([(1, 10.0)], [(2, 200.0)]),
    "empty_right": ([(1, 10.0), (2, 20.0)], []),
    "empty_left": ([], [(2, 200.0), (3, 300.0)]),
}

# Expected rows of `SELECT b.kid, a.m_a, b.m_b {JT} JOIN a.kid = b.kid`, ordered
# by the (coalesced) key. LEFT preserves the `a` (left operand) side; FULL is the
# union with the canonical key coalescing both sides.
MATRIX: dict[tuple[str, str], list[tuple]] = {
    ("partial_overlap", "LEFT"): [(1, 10.0, None), (2, 20.0, 200.0)],
    ("partial_overlap", "FULL"): [
        (1, 10.0, None),
        (2, 20.0, 200.0),
        (3, None, 300.0),
    ],
    ("disjoint", "LEFT"): [(1, 10.0, None)],
    ("disjoint", "FULL"): [(1, 10.0, None), (2, None, 200.0)],
    ("empty_right", "LEFT"): [(1, 10.0, None), (2, 20.0, None)],
    ("empty_right", "FULL"): [(1, 10.0, None), (2, 20.0, None)],
    ("empty_left", "LEFT"): [],
    ("empty_left", "FULL"): [(2, None, 200.0), (3, None, 300.0)],
}


@pytest.mark.parametrize("population,jt", list(MATRIX))
def test_root_key_two_way_matrix(tmp_path: Path, population: str, jt: str):
    a_rows, b_rows = POPULATIONS[population]
    eng = _engine(tmp_path, B_MODEL)
    _load(eng, "create table a_tbl (k int, m float)", a_rows)
    _load(eng, "create table b_tbl (k int, m float)", b_rows)
    text = (
        _IMPORTS + f"{jt} JOIN a.kid = b.kid\n"
        "SELECT b.kid, a.m_a, b.m_b ORDER BY b.kid asc;\n"
    )
    assert _rows(eng, tmp_path, text) == MATRIX[(population, jt)]


def test_source_side_key_projection_left(tmp_path: Path):
    # projecting the authored SOURCE side (a.kid) is correct for LEFT (a is the
    # preserved operand, so a.kid is present on every row).
    eng = _engine(tmp_path, B_MODEL)
    _load(eng, "create table a_tbl (k int, m float)", [(1, 10.0), (2, 20.0)])
    _load(eng, "create table b_tbl (k int, m float)", [(2, 200.0), (3, 300.0)])
    text = (
        _IMPORTS + "LEFT JOIN a.kid = b.kid\n"
        "SELECT a.kid, a.m_a, b.m_b ORDER BY a.kid asc;\n"
    )
    assert _rows(eng, tmp_path, text) == [(1, 10.0, None), (2, 20.0, 200.0)]


def test_full_source_side_projection_coalesces(tmp_path: Path):
    # `FULL JOIN a.kid = b.kid; SELECT a.kid` yields the coalesced key (the
    # b-only row 3 surfaces) — the join asserts a.kid and b.kid are one logical
    # key. FULL binding-keyed sources stay on the identity path (their binding is
    # NOT substituted to the canonical), so the keypair is distinct (`a.k = b.k`)
    # and the merge node coalesces both members regardless of which is projected.
    eng = _engine(tmp_path, B_MODEL)
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    _load(eng, "create table a_tbl (k int, m float)", [(1, 10.0), (2, 20.0)])
    _load(eng, "create table b_tbl (k int, m float)", [(2, 200.0), (3, 300.0)])
    text = (
        _IMPORTS + "FULL JOIN a.kid = b.kid\n"
        "SELECT a.kid, a.m_a, b.m_b ORDER BY a.kid asc;\n"
    )
    assert _rows(eng, tmp_path, text) == [
        (1, 10.0, None),
        (2, 20.0, 200.0),
        (3, None, 300.0),
    ]


@pytest.mark.parametrize(
    "jt,expected",
    [
        # LEFT preserves the `a` side; the b-only key 3 drops.
        ("LEFT", [(1, 10.0, None), (2, 20.0, "two")]),
        # FULL: a-only (1) gets a NULL label, b-only (3) surfaces with its label
        # AND a NULL measure — the property keyed on the merged key resolves
        # across the joined datasources without re-completing the key.
        ("FULL", [(1, 10.0, None), (2, 20.0, "two"), (3, None, "three")]),
    ],
)
def test_property_on_merged_key_through_outer(tmp_path: Path, jt: str, expected: list):
    # the grain-collapse seam: a property (b.label) keyed on the join key,
    # projected through an OUTER join. Substitution rewrites its grain to the
    # canonical key; the OUTER path must still resolve + null it correctly.
    eng = _engine(tmp_path, B_LABEL_MODEL)
    _load(eng, "create table a_tbl (k int, m float)", [(1, 10.0), (2, 20.0)])
    _load(eng, "create table b_tbl (k int, l varchar)", [(2, "two"), (3, "three")])
    text = (
        _IMPORTS + f"{jt} JOIN a.kid = b.kid\n"
        "SELECT b.kid, a.m_a, b.label ORDER BY b.kid asc;\n"
    )
    assert _rows(eng, tmp_path, text) == expected


def test_join_keyword_tracks_authored_type(tmp_path: Path):
    # the rendered SQL join keyword tracks the authored join type (no silent
    # promotion/demotion), and FULL coalesces the (canonical) key.
    eng = _engine(tmp_path, B_MODEL)
    _load(eng, "create table a_tbl (k int, m float)", [(1, 10.0)])
    _load(eng, "create table b_tbl (k int, m float)", [(1, 100.0)])
    base = _IMPORTS + "{jt} JOIN a.kid = b.kid\nSELECT b.kid, a.m_a, b.m_b;\n"
    left = eng.generate_sql(base.format(jt="LEFT"))[-1]
    eng.environment = Environment(working_path=tmp_path)
    full = eng.generate_sql(base.format(jt="FULL"))[-1]
    assert "LEFT OUTER JOIN" in left
    assert "FULL JOIN" in full.upper() and "coalesce" in full.lower()
