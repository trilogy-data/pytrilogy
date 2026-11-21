"""Reduced shapes of the `windows` oracle family, each failing on the release
before the one that fixed it, with rows checked by hand against the six
trees."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

SHAPES = [
    # a window reading a bare aggregate reads one row per group (331): false
    # holds 3 and 2, true 5 and 7, unknown nothing
    (
        "select tall, lag min(height) order by tall asc nulls last as v0;",
        [(False, None), (True, 2), (None, 5)],
    ),
    (
        (
            "select tag, count(tree_id) as c, lag(count(tree_id)) over (order by tag"
            " asc) as v0;"
        ),
        [("x", 4, None), ("y", 2, 4)],
    ),
    # ...partitioned, with a frame to the current group (A's unknown form
    # first, then its 8; B's 7 then 2)
    (
        (
            "select city, tree_form, sum(height) as c, max(sum(height)) over"
            " (partition by city order by tree_form asc nulls first) as v0;"
        ),
        [
            ("A", "broadleaf", 8, 8),
            ("A", None, None, None),
            ("B", "broadleaf", 7, 7),
            ("B", "linden", 2, 7),
        ],
    ),
    # ...and over a derived key (287): (B, true) 7, (A, true) 8, (B, false) 2,
    # (A, unknown) in that order
    (
        (
            "select city, height > 2 as big, lag 2 sum(height) order by big desc"
            " nulls last, city desc nulls first as v0;"
        ),
        [("A", None, 8), ("A", True, None), ("B", False, 7), ("B", True, None)],
    ),
    # a value by (city, tree_form) pairs with the trees a window numbers on
    # both keys (3769): A's broadleaf trees hold two tags, the rest one
    (
        (
            "select tree_id, count_distinct(tag) by city, tree_form + 1 as v0,"
            " row_number tree_id order by sum(height) desc, tree_id asc as v2;"
        ),
        [
            ("a1", 3, 3),
            ("a2", 3, 5),
            ("a3", 3, 2),
            ("a4", 2, 6),
            ("b1", 2, 1),
            ("b2", 2, 4),
        ],
    ),
    # a set supplies no select output (1465): its feeder holds Ulmus alone
    (
        (
            "select species, tree_form, count(tree_id ? tree_id in (tree_id ?"
            " tree_form is null)) as v0;"
        ),
        [
            ("Acer", "broadleaf", 0),
            ("Quercus", "broadleaf", 0),
            ("Tilia", "linden", 0),
            ("Ulmus", None, 1),
        ],
    ),
    # a set beside the filtered rows is no grain of them (2257): the min is
    # still computed over Acer's three trees
    (
        (
            "select species, tree_form, min(height ? species in (species ?"
            " tree_form = 'linden')) as v0;"
        ),
        [
            ("Acer", "broadleaf", None),
            ("Quercus", "broadleaf", None),
            ("Tilia", "linden", 2),
            ("Ulmus", None, None),
        ],
    ),
    # a set is no row parent of a derived key: both tags are in A, so every
    # tree counts
    (
        "select height > 2 as big, count(tree_id ? tag in (tag ? city = 'A')) as v0;",
        [(True, 3), (False, 1), (None, 2)],
    ),
    # a set written in the select reads the rows the WHERE kept: no city
    # keeps more than two trees past 2
    (
        (
            "select city, count(tree_id ? species in (species ? count(tree_id) by"
            " city > 2)) as v0 where height > 2;"
        ),
        [("A", 0), ("B", 0)],
    ),
    # ...under a WHERE no scan carries too (3871): only Tilia's min is 2, and
    # its one tree is beside no other species
    (
        (
            "select count(tree_id ? city in (city ? species != 'Tilia')) as v0 where"
            " min(height) by species = 2;"
        ),
        [(0,)],
    ),
    # ...and its own gate reads them: in A only Quercus reaches past 4
    (
        (
            "select species, count(tree_id ? species in (species ? max(height) by"
            " species > 4)) as v0 where city = 'A';"
        ),
        [("Acer", 0), ("Quercus", 1), ("Ulmus", 0)],
    ),
    # ...as a set in the HAVING does: Tilia and Ulmus are out, and of the
    # kept trees only b1 is in B, tagged y like a2
    (
        (
            "select tree_id, max(height) as v0 where max(height) by species != 2"
            " having tag in (tag ? city = 'B');"
        ),
        [("a2", None), ("b1", 7)],
    ),
    # a summary's partials hold no set: both memberships read `city`, the
    # WHERE's keeps every tree and the select's leaves A's
    (
        (
            "select species, sum(height ? not (city in (city ? city = 'B'))) as v2"
            " where city in (city ? tag = 'y');"
        ),
        [("Acer", 3), ("Quercus", 5), ("Tilia", None), ("Ulmus", None)],
    ),
    # a one-row total filters no key (9129): only B's two species are kept
    (
        (
            "select species, tree_form, max(max(height)) over () as v0 where city ="
            " 'B';"
        ),
        [("Acer", "broadleaf", 7), ("Tilia", "linden", 7)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", SHAPES)
def test_window_family_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
