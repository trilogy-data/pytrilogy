"""A named rollup rowset's grouping id is part of each row's identity: a leaf
with a data NULL and a subtotal share their visible keys. A window over the
rollup beside a projection derived from that id (an alias, an expression, a
hidden field under HAVING) must keep the id on the rollup's rows rather than
sourcing it alone and cross-joining it back (tpc-ds q67, 2026-11-21)."""

import pytest

from trilogy import Dialects, Environment

_MODEL = """
key txn int;
property txn.cat string;
property txn.cls string?;
property txn.qty int;

datasource facts (t: txn, c: cat, k: cls, q: qty)
grain (txn)
query '''
select 1 t, 'a' c, 'x' k, 10 q union all
select 2 t, 'a' c, 'y' k, 5 q union all
select 3 t, 'a' c, null k, 7 q union all
select 4 t, 'b' c, 'x' k, 3 q
''';

with r as rollup((
    select cat, cls, sum(qty) as total, grouping_id(cat, cls) as gid
), cat, cls);
"""

_RANK = "rank(r.cat, r.cls) over (order by r.total desc) as rnk"

# (cat, cls, gid, total, rank by total desc)
_ROWS = [
    (None, None, 3, 25, 1),
    ("a", None, 1, 22, 2),
    ("a", "x", 0, 10, 3),
    ("a", None, 0, 7, 4),
    ("a", "y", 0, 5, 5),
    ("b", None, 1, 3, 6),
    ("b", "x", 0, 3, 6),
]


def _run(query: str) -> list[tuple]:
    env, _ = Environment().parse(_MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.execute_text(query)[-1].fetchall()


def _key(row: tuple) -> tuple:
    return tuple((v is None, v) for v in row)


def _expect(project) -> list[tuple]:
    return sorted((project(*row) for row in _ROWS), key=_key)


@pytest.mark.parametrize(
    "gid_item,project",
    [
        ("r.gid", lambda c, k, g, t, n: (c, k, g, n)),
        ("r.gid as g", lambda c, k, g, t, n: (c, k, g, n)),
        ("r.gid + 1 as g", lambda c, k, g, t, n: (c, k, g + 1, n)),
        ("--r.gid as g", lambda c, k, g, t, n: (c, k, n)),
    ],
)
def test_grouping_id_projection_beside_rollup_window(gid_item, project):
    rows = _run(f"select r.cat, r.cls, {gid_item}, {_RANK};")
    assert sorted(rows, key=_key) == _expect(project)


def test_hidden_grouping_id_with_having_on_rollup_window():
    rows = _run(f"select r.cat, r.cls, --r.gid, {_RANK} having rnk <= 100;")
    assert sorted(rows, key=_key) == _expect(lambda c, k, g, t, n: (c, k, n))


@pytest.mark.parametrize(
    "gid_item,tail,project",
    [
        ("r.gid", "", lambda c, k, g, t, n: (c, k, t, g, n)),
        ("--r.gid", " having rnk <= 100", lambda c, k, g, t, n: (c, k, t, n)),
    ],
)
def test_grouping_id_beside_renamed_rollup_columns(gid_item, tail, project):
    rows = _run(
        f"select r.cat as c, r.cls as k, r.total as t, {gid_item}, {_RANK}{tail};"
    )
    assert sorted(rows, key=_key) == _expect(project)
