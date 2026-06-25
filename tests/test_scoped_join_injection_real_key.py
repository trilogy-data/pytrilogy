"""Companion to test_scoped_join_injection_through_derivation, but the scoped
join is on a REAL (multi-row) key column rather than a constant `1`. The constant
case drags in single-row/cross-join handling that is orthogonal to the actual
"derivation over a rowset output must carry the scoped-join key" mechanism; this
matrix isolates that mechanism on a normal join key.

Same two axes as the constant matrix:
  - rowset vs non-rowset operands
  - raw reference vs a BASIC derivation (`x * 2`) over one operand

Cases 1-3 are the invariant; case 4 (rowset + BASIC) is the q66 target.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

# Non-rowset: x (a_id grain) and y (b_id grain) are unconnected except via the
# real key columns ak/bk, related by a scoped join.
NONROWSET = """
key a_id int;
property a_id.x float;
property a_id.ak int;
key b_id int;
property b_id.y float;
property b_id.bk int;
datasource at (id: a_id, xv: x, akv: ak) grain (a_id)
query '''select 1 id, 10.0 xv, 100 akv''';
datasource bt (id: b_id, yv: y, bkv: bk) grain (b_id)
query '''select 1 id, 3.0 yv, 100 bkv''';
"""

# Rowset: same shape, but x/y and the real join key are rowset outputs.
ROWSET = """
key a_id int;
property a_id.x float;
property a_id.ak int;
key b_id int;
property b_id.y float;
property b_id.bk int;
datasource at (id: a_id, xv: x, akv: ak) grain (a_id)
query '''select 1 id, 10.0 xv, 100 akv''';
datasource bt (id: b_id, yv: y, bkv: bk) grain (b_id)
query '''select 1 id, 3.0 yv, 100 bkv''';
rowset ra <- select x, ak;
rowset rb <- select y, bk;
"""


def _gen(decl: str, sel: str) -> str:
    return Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(
        decl + sel
    )[-1]


def test_1_nonrowset_raw_injects_join():
    sql = _gen(NONROWSET, "select x, y inner join ak = bk;")
    assert "JOIN" in sql


def test_2_nonrowset_basic_injects_join():
    sql = _gen(NONROWSET, "select x * 2 as r, y inner join ak = bk;")
    assert "JOIN" in sql


def test_3_rowset_raw_injects_join():
    sql = _gen(ROWSET, "select ra.x, rb.y inner join ra.ak = rb.bk;")
    assert "JOIN" in sql


def test_4_rowset_basic_injects_join():
    sql = _gen(ROWSET, "select ra.x * 2 as r, rb.y inner join ra.ak = rb.bk;")
    assert "JOIN" in sql
