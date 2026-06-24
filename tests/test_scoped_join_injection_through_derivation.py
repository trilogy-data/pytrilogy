"""Isolation harness for q66: a scoped-join "bridge" concept should be equally
discoverable through a BASIC derivation as it is for a raw reference — and it is,
EXCEPT when the joined concepts are rowset outputs.

Four parallel cases, same scoped-join-injection shape, varying two axes:
  - rowset vs non-rowset operands
  - raw reference vs a BASIC derivation (`x * 2`) over one operand

The two operands sit at distinct grains and are unconnected except through an
injected join key (a constant), related by a scoped `inner join`. Resolving the
query requires discovery to inject that join key.

Result (the bug): cases 1-3 resolve; case 4 (rowset + BASIC) raises
DisconnectedConcepts. Case 2 passing proves a BASIC derivation does NOT break
scoped-join injection in general — the break is rowset-specific: a datasource node
co-locates a datasource's columns (so a derivation over one column still reaches
the join key), but a rowset has no such node, so its outputs aren't co-located and
the join key is unreachable from a derivation over a sibling output.

Cases 1-3 are the invariant that must stay green; case 4 is the q66 target.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

# Non-rowset: x (a_id grain) and y (b_id grain) are unconnected except via the
# constant join keys ak/bk, related by a scoped join.
NONROWSET = """
key a_id int;
property a_id.x float;
property a_id.ak int;
key b_id int;
property b_id.y float;
property b_id.bk int;
datasource at (id: a_id, xv: x, k: ak) grain (a_id)
query '''select 1 id, 10.0 xv, 1 k''';
datasource bt (id: b_id, yv: y, k: bk) grain (b_id)
query '''select 1 id, 3.0 yv, 1 k''';
"""

# Rowset: same shape, but x/y and the join key are rowset outputs.
ROWSET = """
key a_id int;
property a_id.x float;
key b_id int;
property b_id.y float;
datasource at (id: a_id, xv: x) grain (a_id) query '''select 1 id, 10.0 xv''';
datasource bt (id: b_id, yv: y) grain (b_id) query '''select 1 id, 3.0 yv''';
rowset ra <- select x, 1 as join_key;
rowset rb <- select y, 1 as join_key;
"""


def _gen(decl: str, sel: str) -> str:
    return Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(
        decl + sel
    )[-1]


def test_1_nonrowset_raw_injects_join():
    sql = _gen(NONROWSET, "select x, y inner join ak = bk;")
    assert "JOIN" in sql


def test_2_nonrowset_basic_injects_join():
    # A BASIC derivation over one operand must resolve identically to the raw case.
    sql = _gen(NONROWSET, "select x * 2 as r, y inner join ak = bk;")
    assert "JOIN" in sql


def test_3_rowset_raw_injects_join():
    sql = _gen(ROWSET, "select ra.x, rb.y inner join ra.join_key = rb.join_key;")
    assert "JOIN" in sql


def test_4_rowset_basic_injects_join():
    # q66 target: a BASIC derivation over a rowset output must carry the rowset's
    # scoped-join key the same way the raw reference does.
    from trilogy.hooks.query_debugger import DebuggingHook
    DebuggingHook()
    sql = _gen(
        ROWSET, "select ra.x * 2 as r, rb.y inner join ra.join_key = rb.join_key;"
    )
    assert "JOIN" in sql
