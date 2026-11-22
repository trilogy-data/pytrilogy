"""Reduced shapes of the `filters` oracle family (a `?` as a select target),
each failing on the release before the one that fixed it, with rows checked
by hand against the six trees."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

SHAPES = [
    # the rows a filtered key rejects are one NULL row of their group (651):
    # A's two unmeasured trees, B's 2
    (
        "select city, tree_id ? height > 2 as f;",
        [("A", "a1"), ("A", "a3"), ("A", None), ("B", "b1"), ("B", None)],
    ),
    # ...through a lookup (1961): only the Ulmus has no form
    (
        "select city, species ? coalesce(tree_form, 'none') = 'none' as f;",
        [("A", "Ulmus"), ("A", None), ("B", None)],
    ),
    # ...and as a group key of an aggregate (2219): the two A Acers are 3
    # and unmeasured
    (
        "select species, tree_id ? city = 'B' as f, min(height) as v1;",
        [
            ("Acer", "b1", 7),
            ("Acer", None, 3),
            ("Quercus", None, 5),
            ("Tilia", "b2", 2),
            ("Ulmus", None, None),
        ],
    ),
    # filtered values alone are the rows the condition keeps, a membership
    # included (1604)
    ("select city ? not (city in (city ? city = 'B')) as f;", [("A",)]),
    (
        "select tree_id ? species in (species ? tag = 'y') as f;",
        [("a1",), ("a2",), ("b1",)],
    ),
    # a set is no finer than the column it gates (2470): the WHERE drops the
    # formless Ulmus, and both cities keep a tree over 2
    (
        (
            "select tree_id, city ? city in (city ? height > 2) as f where"
            " avg(height) by tree_form >= 1;"
        ),
        [("a1", "A"), ("a2", "A"), ("a3", "A"), ("b1", "B"), ("b2", "B")],
    ),
    # ...alone: the species of at most two trees hold no y tag
    (
        (
            "select tree_id ? species in (species ? tag = 'y') as f where"
            " count(tree_id) by species <= 2;"
        ),
        [],
    ),
    # a total gated by a total is a scalar of its one row (1725): the
    # shortest tree is 2
    (
        "select tag, (count(tree_id) by *) ? min(height) by * <= 5 as f;",
        [("x", 6), ("y", 6)],
    ),
    (
        "select tag, (count(tree_id) by *) ? min(height) by * > 5 as f;",
        [("x", None), ("y", None)],
    ),
    # a HAVING on a filtered column collapsed to its grain is a HAVING (3297)
    (
        (
            "select species, tag, species ? tree_form = 'broadleaf' as f having f is"
            " not null;"
        ),
        [("Acer", "x", "Acer"), ("Acer", "y", "Acer"), ("Quercus", "x", "Quercus")],
    ),
    # a WHERE aggregate only a set's condition reads still filters the rows
    # (2669): A's heights sum to 8, and of its trees two are Acers
    (
        (
            "select tree_id, species ? species in (species ? count(tree_id) by"
            " species >= 2) as f where sum(height) by city <= 8;"
        ),
        [("a1", "Acer"), ("a2", "Acer"), ("a3", None), ("a4", None)],
    ),
    # a WHERE aggregate reaches a filtered column past the set it reads
    # (6065): only the broadleaves hold both tags, and both cities stay in
    # the set
    (
        (
            "select height ? city in (city ? count(tree_id) by city, tree_form != 10)"
            " as f where count_distinct(tag) by * = count_distinct(tag) by tree_form"
            " or sum(height) by city, tree_form != count_distinct(tag) by city;"
        ),
        [(3,), (5,), (7,), (None,)],
    ),
    # a value by (city, tree_form) pairs with groups keyed by a filtered column
    # through the filtered rows, on both keys (543): A's broadleaves reach 5,
    # its Ulmus has no form's maximum
    (
        (
            "select city, species, height ? species != 'Tilia' as f, min(height) as"
            " v1, max(height) by city, tree_form as v2;"
        ),
        [
            ("A", "Acer", 3, 3, 5),
            ("A", "Acer", None, None, 5),
            ("A", "Quercus", 5, 5, 5),
            ("A", "Ulmus", None, None, None),
            ("B", "Acer", 7, 7, 7),
            ("B", "Tilia", None, 2, 2),
        ],
    ),
    # a window's groups hold no column a derived or filtered key was read off
    # (2680): the three tall trees are one group
    (
        (
            "select height > 2 as big, tree_form ? tree_form = 'linden' as f, species"
            " ? tree_form = 'x' as g, max(max(height)) over (partition by g order by"
            " big desc nulls last, f desc nulls last) as w;"
        ),
        [(True, None, None, 7), (False, "linden", None, 7), (None, None, None, 7)],
    ),
    # ...but a value its grain determines rides through them (19498): each
    # form's maximum beside a window over species and a filtered tree
    (
        (
            "select species, tree_id ? avg(height) by tree_form >= 0 as f,"
            " min(min(height) by species, f) over (order by species asc nulls last, f"
            " asc nulls last) as w, max(height) by tree_form as v2;"
        ),
        [
            ("Acer", "a1", 3, 7),
            ("Acer", "a2", 3, 7),
            ("Acer", "b1", 3, 7),
            ("Quercus", "a3", 3, 7),
            ("Tilia", "b2", 2, 2),
            ("Ulmus", None, 2, None),
        ],
    ),
    # a window numbers the groups of a filtered key, its NULL group once
    # (4224): y's 7 and unmeasured tree are one group
    (
        (
            "select tag, height ? height between 2 and 5 as f, row_number tag order"
            " by tag desc nulls last, f asc nulls last as r;"
        ),
        [("x", 2, 2), ("x", 3, 3), ("x", 5, 4), ("x", None, 5), ("y", None, 1)],
    ),
    # an aggregate grouped by a filtered key is no partial of the aggregate
    # the key reads (1074): the Ulmus' form has no minimum
    (
        (
            "select species, tree_id ? min(height) by tree_form < 10 as f,"
            " min(height) as v1;"
        ),
        [
            ("Acer", "a1", 3),
            ("Acer", "a2", None),
            ("Acer", "b1", 7),
            ("Quercus", "a3", 5),
            ("Tilia", "b2", 2),
            ("Ulmus", None, None),
        ],
    ),
    # a filter carrying a row column below its grain pairs with another row
    # stream on that grain only once deduplicated to it (35245): B's one
    # Acer is tall
    (
        (
            "select city, height > 2 as big, tree_form ? tree_form in"
            " (tree_form ? height > 2) as v0;"
        ),
        [
            ("A", None, "broadleaf"),
            ("A", None, None),
            ("A", True, "broadleaf"),
            ("B", False, None),
            ("B", True, "broadleaf"),
        ],
    ),
    (
        (
            "select city, upper(tag) as big, tree_form ? tree_form in"
            " (tree_form ? height > 2) as v0;"
        ),
        [
            ("A", "X", "broadleaf"),
            ("A", "X", None),
            ("A", "Y", "broadleaf"),
            ("B", "X", None),
            ("B", "Y", "broadleaf"),
        ],
    ),
    # ...and keeps the columns of its own rows when nothing else holds them
    # all (31959, which a first version split over two streams)
    (
        (
            "select city, tag, tree_form ? tree_form in (tree_form ? tag = 'x')"
            " as v0, max(height) by city as v1;"
        ),
        [
            ("A", "x", "broadleaf", 5),
            ("A", "x", None, 5),
            ("A", "y", "broadleaf", 5),
            ("B", "x", "linden", 7),
            ("B", "y", "broadleaf", 7),
        ],
    ),
    # a filtered key collapsed to its keys keeps the select's other key
    # (38603): no city holds every form
    (
        (
            "select city, tree_form, city ? sum(height) by * is null as v0,"
            " count(tree_id) by tree_form as v2;"
        ),
        [
            ("A", "broadleaf", None, 4),
            ("A", None, None, 1),
            ("B", "broadleaf", None, 4),
            ("B", "linden", None, 1),
        ],
    ),
    # ...and a filter read off its condition's aggregate alone holds no
    # other key: the WHERE-kept rows bridge it (20955; the formless Ulmus
    # has no maximum)
    (
        (
            "select city, tree_form, city ? sum(height) by city - count(tree_id)"
            " by city = 2 as v0 where max(height) by tree_form is not null;"
        ),
        [("A", "broadleaf", None), ("B", "broadleaf", None), ("B", "linden", None)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", SHAPES)
def test_filter_family_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
