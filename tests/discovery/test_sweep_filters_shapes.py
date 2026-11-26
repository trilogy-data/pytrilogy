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
    # a property shown without its key is its distinct values, a rename too:
    # grouped past it by its hidden key, a form showed once per species
    # (25880, reduced; no `?` needed)
    (
        (
            "select tree_form as v0 where tree_form in (tree_form ? tree_form ="
            " 'broadleaf') or tag = 'x';"
        ),
        [("broadleaf",), ("linden",), (None,)],
    ),
    # a filter's keys stay on the rows it filters (48567, reduced): moved to
    # a scan of the species table by the WHERE's set, they left it nothing to
    # pair on, and every form met every value of it
    (
        (
            "select tree_form, city ? city = 'A' as v0 where species in"
            " (species ? count(tree_id) by species < 3);"
        ),
        [("broadleaf", "A"), (None, "A"), ("linden", None)],
    ),
    # ...a derived key's inputs too (46449)
    (
        (
            "select case when tree_form = 'broadleaf' then 'b' else 'o' end as fc,"
            " city ? city = 'A' as v0 where species in (species ? count(tree_id)"
            " by species < 3);"
        ),
        [("b", "A"), ("o", "A"), ("o", None)],
    ),
    # a WHERE hosted on the aggregate a filter's condition reads filters the
    # filter's rows too (59782, reduced): the Quercus's 5 is no kept height
    (
        (
            "select height ? count(tree_id) by city < 10 as v0 where min(height)"
            " by species < 4;"
        ),
        [(2,), (3,), (7,), (None,)],
    ),
    (
        (
            "select height ? count(tree_id) by city, tree_form < 10 as v0 where"
            " min(height) by tree_form - min(height) by species < 0;"
        ),
        [(5,)],
    ),
    # ...and what its condition reads (41909, reduced): `tree_form` moved to
    # the species scan, the per-tree maximum met it on nothing (a keyless join)
    (
        (
            "select tree_id, max(height) ? tree_form = 'linden' as v0,"
            " min(height) by species as v1;"
        ),
        [
            ("a1", None, 3),
            ("a2", None, 3),
            ("a3", None, 5),
            ("a4", None, None),
            ("b1", None, 3),
            ("b2", 2, 2),
        ],
    ),
    # a filter its keys decide is shown once per value, as a property is
    # (46449): grouped by its hidden `species`, (A, b, NULL) showed twice
    (
        (
            "select city, case when tree_form = 'broadleaf' then 'b' else 'o' end"
            " as fc, tree_form ? species in (species ? coalesce(tree_form, 'none')"
            " = 'none') as v0 where tag in (tag ? height between 3 and 7);"
        ),
        [("A", "b", None), ("A", "o", None), ("B", "b", None), ("B", "o", None)],
    ),
    # a filter an aggregate answers is a value of the select's rows (20955,
    # reduced): read off the city sum alone it held no form, and city A met
    # the linden the WHERE keeps for B
    (
        (
            "select city, tree_form, city ? sum(height) by city > 2 as v0 where"
            " max(height) by tree_form is not null;"
        ),
        [("A", "broadleaf", "A"), ("B", "broadleaf", "B"), ("B", "linden", "B")],
    ),
    (
        (
            "select city, tree_form, city ? sum(height) by city - count(tree_id)"
            " by city = 2 as v0 where max(height) by tree_form is not null;"
        ),
        [("A", "broadleaf", None), ("B", "broadleaf", None), ("B", "linden", None)],
    ),
    # ...beside a second filter (6259): every form met every city
    (
        (
            "select tree_form, city ? max(height) by city <= 10 as v0, tree_form ?"
            " min(height) by species != 0 as v1;"
        ),
        [
            ("broadleaf", "A", "broadleaf"),
            (None, "A", None),
            ("broadleaf", "B", "broadleaf"),
            ("linden", "B", "linden"),
        ],
    ),
    # ...and with the shown keys those determine (14639, reduced): keyed by
    # `species` alone, it was paired on `tree_form` off the set's own rows
    (
        (
            "select city, species, tree_form, city ? count_distinct(tag) by city"
            " - count_distinct(tag) by * < 2 as v0, height ? count(tree_id) by"
            " city, species <= max(height) by tree_form or species in (species ?"
            " not (species != 'Tilia')) as v1;"
        ),
        [
            ("A", "Acer", "broadleaf", "A", 3),
            ("A", "Acer", "broadleaf", "A", None),
            ("A", "Quercus", "broadleaf", "A", 5),
            ("A", "Ulmus", None, "A", None),
            ("B", "Acer", "broadleaf", "B", 7),
            ("B", "Tilia", "linden", "B", 2),
        ],
    ),
    # a filtered aggregate keyed by a key the select does not show (79687,
    # raised a keyless join): Ulmus's form is the NULL group, one tree
    (
        (
            "select species, (count(tree_id) by tree_form) ? tree_form in"
            " (tree_form ? species != 'Tilia') as v0, max(count(tree_id) by"
            " species) over () as v1;"
        ),
        [
            ("Acer", 4, 3),
            ("Quercus", 4, 3),
            ("Ulmus", 1, 3),
            ("Tilia", None, 3),
        ],
    ),
    # a filter the select does not show keeps its own rows: A's Acers, the
    # species of three trees
    (
        (
            "select tree_id, height where city = 'A' and count(tree_id) by species"
            " in (count(tree_id) by species ? species = 'Acer');"
        ),
        [("a1", 3), ("a2", None)],
    ),
    # one merge built twice joins its dimension once (61695, an ambiguous
    # duplicate alias): both copies paired the peel with their own side
    (
        (
            "select case when tree_form = 'broadleaf' then 'b' else 'o' end as fc,"
            " species ? not (species in (species ? count(tree_id) by city, species"
            " >= 8)) as v0, height ? (height is not null or sum(height) by species"
            " != 8) as v1, count(tree_id) as v2;"
        ),
        [
            ("b", "Acer", 3, 1),
            ("b", "Acer", 7, 1),
            ("b", "Acer", None, 1),
            ("b", "Quercus", 5, 1),
            ("o", "Tilia", 2, 1),
            ("o", "Ulmus", None, 1),
        ],
    ),
    # a set holds no select keys (87783, reduced): no species has 6 trees,
    # and the keys came off the set's empty rows
    (
        (
            "select city, species, (count(tree_id) by city) ? city in (city ?"
            " count(tree_id) by species > 5) as v0;"
        ),
        [
            ("A", "Acer", None),
            ("A", "Quercus", None),
            ("A", "Ulmus", None),
            ("B", "Acer", None),
            ("B", "Tilia", None),
        ],
    ),
    # an aggregate only a set reads parents no derived key (98603, reduced):
    # the form of a city's trees met each of that city's tags
    (
        (
            "select coalesce(tree_form, 'none') as form0, tag ? city in (city ?"
            " count(tree_id) by city, tree_form < 10) as v0;"
        ),
        [("broadleaf", "x"), ("broadleaf", "y"), ("none", "x"), ("linden", "x")],
    ),
    # a WHERE gate meets a filtered column's rows on its whole grain (85660,
    # reduced): paired on `city` alone, B's y tree came back
    (
        (
            "select city, tag, (max(height) by city) ? tag = 'x' as v0 where"
            " min(height) by city, species < 6;"
        ),
        [("A", "x", 5), ("A", "y", None), ("B", "x", 2)],
    ),
    (
        (
            "select city, tag, (max(height) by city) ? tag = 'x' as v0 where"
            " min(height) by city, species < sum(height) by tree_form;"
        ),
        [("A", "x", 5), ("A", "y", None), ("B", "y", None)],
    ),
    # a filtered key identifies no row (89848, reduced): the GROUP BY was
    # dropped as if the membership's set were the rows
    (
        (
            "select species, tag, tree_id ? count(tree_id) by species < 0 as v0"
            " where species in (species ? species = 'Acer');"
        ),
        [("Acer", "x", None), ("Acer", "y", None)],
    ),
    # a WHERE aggregate only a set's aggregate reads, under a WHERE that reads
    # a set (82889, reduced, raised): only B has at most three trees
    (
        (
            "select city, city ? city in (city ? avg(height) by city > 1) as v0"
            " where tag in (tag ? tag = 'x') and count(tree_id) by city <= 3;"
        ),
        [("B", "B")],
    ),
    # a WHERE hosted on a filter's condition alone reaches its rows (2922):
    # Tilia averages 2 and the Ulmus has no average
    (
        (
            "select species ? avg(height) by * < 10 as f where avg(height) by"
            " city, species != 2;"
        ),
        [("Acer",), ("Quercus",)],
    ),
    # a total splits no key (111272, reduced): collapsed to its city, the
    # filter met the forms on nothing
    (
        (
            "select city, tree_form, city ? count_distinct(tag) by * < 0 as v0"
            " where avg(height) by species != 1;"
        ),
        [("A", "broadleaf", None), ("B", "broadleaf", None), ("B", "linden", None)],
    ),
    (
        (
            "select tree_form, city ? avg(height) by * <= 8 as f where"
            " count(tree_id) by species = 1;"
        ),
        [("broadleaf", "A"), ("linden", "B"), (None, "A")],
    ),
    # a set's input hosts a WHERE beside the rows, not instead of them
    # (117080, reduced): no city's shortest tree is under 2
    (
        (
            "select tree_form ? not (species in (species ? count(tree_id) by city"
            " = 0)) as v0 where min(height) by city < 2;"
        ),
        [],
    ),
    # a condition's aggregates meet on each row's keys (114764, reduced): paired
    # on the form alone, B's broadleaf sum met Quercus's minimum
    (
        (
            "select city, tag, tree_form ? species in (species ? sum(height) by"
            " city, tree_form > min(height) by species) as v0 where avg(height)"
            " by city, species != 3;"
        ),
        [("A", "x", None), ("B", "x", None), ("B", "y", None)],
    ),
    # a filter of a gated aggregate holds that aggregate's groups (113128,
    # reduced, raised): B alone has at most three trees
    (
        (
            "select case when tree_form = 'broadleaf' then 'b' else 'o' end as fc,"
            " (sum(height) by fc) ? max(height) by * > 3 as v0 where"
            " count(tree_id) by city <= 3;"
        ),
        [("b", 7), ("o", 2)],
    ),
    # a partial's chain is grouped by no reader of its head (109332, reduced,
    # recursed): no species outnumbers its form's shortest tree
    (
        (
            "select city, tree_form, tag ? tree_form in (tree_form ? count(tree_id)"
            " by species > min(height) by tree_form) as v1, min(height) as v2,"
            " min(height) by city, tree_form as v3;"
        ),
        [
            ("A", "broadleaf", None, 3, 3),
            ("A", None, None, None, None),
            ("B", "broadleaf", None, 7, 7),
            ("B", "linden", None, 2, 2),
        ],
    ),
    # a set feeds a group beside it no column (123427, reduced): the WHERE's
    # count by city was dropped from the select's count as one the set
    # supplied, and recounted per row as 1; A's Acers fell
    (
        (
            "select city, tag, (count(tree_id) by city) ? city in (city ?"
            " min(height) by city, tree_form > 0) as v0 where count(tree_id) by"
            " city >= count(tree_id) by city, species;"
        ),
        [("A", "x", 4), ("A", "y", 4), ("B", "x", 2), ("B", "y", 2)],
    ),
    # a set planned alone gates its own rows (128462, reduced): the WHERE
    # keeps A's Acers and B's Tilia, so the set is linden; B's Acer leaked in
    (
        (
            "select city, tag, species ? tree_form in (tree_form ?"
            " count_distinct(tag) by species < 6 and city = 'B') as v1 where"
            " min(height) by city, species <= 3;"
        ),
        [("A", "x", None), ("A", "y", None), ("B", "x", "Tilia")],
    ),
    # ...and one at the gate's own grain is gated by its pairing (138625,
    # reduced): a2's NULL height was filtered as a minimum of 8
    (
        (
            "select tree_id, tag ? city in (city ? count(tree_id) by species >"
            " min(height) by city, species) as v0 where min(height) by city,"
            " species != 8;"
        ),
        [("a1", None), ("a2", None), ("a3", None), ("b1", None), ("b2", None)],
    ),
    # a select target re-sourced at FINAL reads the WHERE-kept set (130249,
    # reduced): only B's Acer passes, so no city has a kept linden
    (
        (
            "select city ? city in (city ? tree_form = 'linden') as v0, tag ?"
            " tree_form in (tree_form ? height > 3) as v1, count(tree_id) by * as"
            " v3 where min(height) by city, species >= 6 having v3 <= 1;"
        ),
        [(None, "y", 1)],
    ),
    # a set's input supplies no select key (129123, reduced): Acer once per
    # city
    (
        (
            "select species, (sum(height) by tree_form) ? tree_form in (tree_form"
            " ? count_distinct(tag) by city, species > 6) as v0, sum(height) by"
            " tree_form as v1;"
        ),
        [
            ("Acer", None, 15),
            ("Quercus", None, 15),
            ("Tilia", None, 2),
            ("Ulmus", None, None),
        ],
    ),
    # a set gated off the summary reads the WHERE's own twin of a shown
    # aggregate (62004, reduced): only the Ulmus sum is NULL, and Acer holds
    # the only species with two tags
    (
        (
            "select species ? species in (species ? count(tag) > 1) as v0,"
            " sum(height) by city, species as m where sum(height) by city,"
            " species > 0;"
        ),
        [("Acer", 3), ("Acer", 7), (None, 2), (None, 5)],
    ),
    # a WHERE applied below a count by city filters no tree joined back to
    # it on city (151597, reduced): only the formless Ulmus passes
    (
        (
            "select species, tree_form, tag ? count(tag) by city < 1 as v0,"
            " count(tree_id) as v1 where max(height) by city, tree_form is null;"
        ),
        [("Ulmus", None, None, 1)],
    ),
    (
        (
            "select species, tree_form, tag ? not (tag in (tag ?"
            " count_distinct(tag) by city < 1)) as v0, max(height) as v1 where"
            " avg(height) by city, tree_form is null;"
        ),
        [("Ulmus", None, "x", None)],
    ),
    # ...but a side regrouped to the keys it was joined on holds only kept
    # groups (43195, reduced): no form has three trees
    (
        (
            "select city, tag, city ? city in (city ? species != 'Tilia') as v1,"
            " avg(max(height)) over (partition by v1) as v2 where count(tree_id)"
            " by tree_form = 3;"
        ),
        [],
    ),
    # a stored count the scan carries for a set pins none of its rows
    # (152317, reduced): A's Acer once
    (
        (
            "select city, species, city ? city in (city ? min(height) by species"
            " != 8) as v0, (max(height) by city) ? city in (city ? count(tree_id)"
            " by city, species = 1) as v1;"
        ),
        [
            ("A", "Acer", "A", 5),
            ("A", "Quercus", "A", 5),
            ("A", "Ulmus", "A", 5),
            ("B", "Acer", "B", 7),
            ("B", "Tilia", "B", 7),
        ],
    ),
    # a filter meeting the WHERE's host only through its set's condition
    # still owes the gate (160158, reduced): the unmeasured Ulmus is no row
    (
        (
            "select species ? tree_form in (tree_form ? sum(height) by * != 3) as"
            " v0, count(tree_id) by * as v1 where min(height) by species is not"
            " null;"
        ),
        [("Acer", 5), ("Quercus", 5), ("Tilia", 5)],
    ),
    # a filtered value built per tree is no lookup on its species
    # (167663, reduced): four broadleaf trees, the x ones 3 and 5
    (
        (
            "select tree_form, species ? max(height) by species < 5 as v0,"
            " count(tree_id) as v1, sum(height ? tag = 'x') as v2 where species"
            " in (species ? height is not null);"
        ),
        [("broadleaf", None, 4, 8), ("linden", "Tilia", 1, 2)],
    ),
    # a row value keeps the key its scan read only for the WHERE (178393,
    # reduced): every tree passes, and big pairs with v0 per tree, not crossed
    (
        (
            "select height > 2 as big, tree_form ? species in (species ? species"
            " != 'Tilia') as v0 where tree_id in (tree_id ? count(tree_id) by"
            " city < 8);"
        ),
        [(True, "broadleaf"), (None, "broadleaf"), (None, None), (False, None)],
    ),
    # a window reads the rows a grouping parent's gate kept (196634,
    # reduced): no species averages over 6; over 4, Acer and Quercus trees
    (
        (
            "select city, tree_form, city ? city = 'B' as v0, sum(max(height) by"
            " city, tree_form, v0) over (order by city) as v2 where avg(height) by"
            " species > 6;"
        ),
        [],
    ),
    (
        (
            "select city, tree_form, city ? city = 'B' as v0, sum(max(height) by"
            " city, tree_form, v0) over (order by city) as v2 where avg(height) by"
            " species > 4;"
        ),
        [("A", "broadleaf", None, 5), ("B", "broadleaf", "B", 12)],
    ),
    # a filtered aggregate keyed by fewer shown keys than its rows is read
    # off those rows (190095, reduced): four (city, form) pairs, not six
    (
        (
            "select city, tree_form, (count(tree_id) by tree_form) ? tree_form !="
            " 'x' as v0, max(height) by city as v1;"
        ),
        [
            ("A", "broadleaf", 4, 5),
            ("A", None, None, 5),
            ("B", "broadleaf", 4, 7),
            ("B", "linden", 1, 7),
        ],
    ),
    # a filter folded into its predicate's aggregate is no GROUP BY key
    # (196668, reduced)
    (
        (
            "select upper(tree_form) as fc, tree_form ? min(height) by tree_form"
            " != 0 as v0, city as v1;"
        ),
        [
            ("BROADLEAF", "broadleaf", "A"),
            ("BROADLEAF", "broadleaf", "B"),
            ("LINDEN", "linden", "B"),
            (None, None, "A"),
        ],
    ),
    # a total in the WHERE beside a membership gates the rows (182211,
    # reduced): B's two trees, each carrying a tag A has
    (
        (
            "select height ? tag in (tag ? count(tree_id) by city >= 1) as v0"
            " where max(height) by * <= max(height) by city and tag in (tag ?"
            " city = 'A');"
        ),
        [(2,), (7,)],
    ),
    # a FINAL taking `tree_form` off rows at `(city, species)` dedups them
    # (11995)
    (
        (
            "select city, tree_form, (max(height) by tree_form) ? tree_form in"
            " (tree_form ? max(height) by tree_form - max(height) by city,"
            " species < 1) as v0;"
        ),
        [
            ("A", "broadleaf", 7),
            ("A", None, None),
            ("B", "broadleaf", 7),
            ("B", "linden", 2),
        ],
    ),
    # a gate's grain is every aggregate its atom reads (190661, reduced): the
    # WHERE keeps a1, a2 and b2, and only a1 is over 2
    (
        (
            "select city, city ? city in (city ? sum(height) by city > 0 and"
            " height > 2) as v2 where sum(height) by city - max(height) by"
            " city, species > 3;"
        ),
        [("A", "A"), ("B", None)],
    ),
    # a WHERE column a select's set reads past stays on the rows (201221,
    # reduced): the WHERE keeps the unmeasured Ulmus alone, so the set is empty
    (
        (
            "select species, tree_form, tag ? tag in (tag ? min(height) by"
            " species is not null) as v0 where tree_form is null;"
        ),
        [("Ulmus", None, None)],
    ),
    # a filter's rows are keyed by the keys it outputs (201879, reduced): no
    # tree of B's is broadleaf beside A's maximum
    (
        (
            "select city, height > 2 as big, tree_form ? species in (species ?"
            " height is null) as v0, max(height) by city as v1;"
        ),
        [
            ("A", None, "broadleaf", 5),
            ("A", None, None, 5),
            ("A", True, "broadleaf", 5),
            ("A", True, None, 5),
            ("B", False, None, 7),
            ("B", True, "broadleaf", 7),
        ],
    ),
    # a filter over the gate's own groups passed it (217166, reduced): only
    # A's formless Ulmus has no height, and the set of tags is empty
    (
        (
            "select city, tree_form, city ? not (city in (city ? height is not"
            " null)) as v0, (count(tree_id) by city, tree_form) ? city in ('B')"
            " as v1 where (tag in (tag ? min(height) by city, species = 10) or"
            " min(height) by city, tree_form is null);"
        ),
        [("A", None, "A", None)],
    ),
    # a derived key pairs a filter on its inputs (222515, reduced): Tilia's max
    # is under 4, so only B's trees show their city
    (
        (
            "select coalesce(tree_form, 'none') as form0, city ? count(tree_id) by"
            " city > 5 or city in (city ? max(height) by species < 4) as v0;"
        ),
        [("broadleaf", "B"), ("broadleaf", None), ("linden", "B"), ("none", None)],
    ),
    # a gate that holds on NULL passes no padding (226656, reduced): the
    # unmeasured Ulmus alone sums to NULL
    (
        (
            "select species, (sum(height) by species) ? species = 'Acer' as v0,"
            " tag ? (species in (species ? max(height) by city > 3) or city ="
            " 'Z') as v1 where sum(height) by tree_form is null;"
        ),
        [("Ulmus", None, None)],
    ),
    # the all-rows marker demands no select column of the scan (225484,
    # reduced, raised `Could not resolve connections`): only A has more trees
    # than the shortest height, and its heights sum to 8
    (
        (
            "select tag, species ? species in (species ? sum(height) by * >= 8)"
            " as v0 where min(height) by * < count(tree_id) by city;"
        ),
        [("x", "Acer"), ("x", "Quercus"), ("x", "Ulmus"), ("y", "Acer")],
    ),
    # a dedup under a row count reads the all-rows marker its scan hides
    # (130215, raised on the summary model): no tree's height passes either gate
    (
        (
            "select city, species, height ? sum(height) by city, species <="
            " count(tree_id) by city, species as v0, height ? (species != 'Tilia'"
            " and city in (city ? count(tree_id) by * = 3)) as v1 where"
            " min(height) by city, species >= count_distinct(tag) by *;"
        ),
        [
            ("A", "Acer", None, None),
            ("A", "Quercus", None, None),
            ("B", "Acer", None, None),
            ("B", "Tilia", None, None),
        ],
    ),
    # a WHERE total goes with its atom's other value onto a set's rows
    # (235307, reduced): the kept trees are A's Ulmus and both of B's, and
    # only B's Acer stands between 3 and 7
    (
        (
            "select tag ? tag in (tag ? height between 3 and 7 and sum(height)"
            " by tree_form != 3) as v2 where count_distinct(tag) by * >"
            " count_distinct(tag) by city, tree_form;"
        ),
        [("y",)],
    ),
    # a window reading a filter's rows is gated as an aggregate is (240133,
    # reduced): the formless Ulmus has no maximum, so A numbers two species
    (
        (
            "select city, species, species ? avg(height) by * is not null as v0,"
            " row_number() over (partition by city order by species) as v2"
            " where max(height) by city, tree_form != 1;"
        ),
        [
            ("A", "Acer", "Acer", 1),
            ("A", "Quercus", "Quercus", 2),
            ("B", "Acer", "Acer", 1),
            ("B", "Tilia", "Tilia", 2),
        ],
    ),
    # two windows' partitions pair on the select's grain (257753, reduced):
    # B has no Ulmus
    (
        (
            "select city, tag, species ? species = 'Ulmus' as v1,"
            " min(max(height)) over (partition by city, tag) as v2,"
            " min(sum(height)) over (partition by tag, v1) as v3;"
        ),
        [
            ("A", "x", "Ulmus", 5, None),
            ("A", "x", None, 5, 2),
            ("A", "y", None, None, 7),
            ("B", "x", None, 2, 2),
            ("B", "y", None, 7, 7),
        ],
    ),
    # a set's grain names no column reading another set (256563, reduced,
    # raised `maximum recursion`): the kept trees are a1, a3 and b1, B's is an
    # Acer and none is a linden
    (
        (
            "select tree_id ? species in (species ? city = 'B') as v0, city ?"
            " city in (city ? tree_form = 'linden') as v1 where height > 2 and"
            " count(tree_id) by tree_form > 3;"
        ),
        [("a1", None), ("b1", None), (None, None)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", SHAPES)
def test_filter_family_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
