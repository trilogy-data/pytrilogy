"""`formal/Trilogy/Cover.lean`: which queries a count summary covers. Each
shape is grouped by the rule that makes it a re-sum of the summary, and runs
under every filter the summary's grain determines; it must answer as the tree
rows do, and read the summary exactly when the rule says it can."""

import itertools

import pytest

from trilogy import Dialects

TREES = """
select 1 as tree_id, 'A' as city, 'Acer rubrum' as species, 4.0 as dbh, date '1990-05-01' as plant_date
union all select 2, 'A', 'Acer rubrum', 14.0, date '2010-05-01'
union all select 3, 'A', 'Acer saccharum', null, null
union all select 4, 'A', 'Opuntia ficus', 30.0, date '2010-06-01'
union all select 5, 'B', 'Opuntia ficus', 8.0, date '2001-01-01'
union all select 6, 'B', 'Acer rubrum', 40.0, date '1950-01-01'
union all select 7, 'B', 'Tilia cordata', 9.0, date '2010-02-01'
"""

ENRICHMENT = """select 'Acer rubrum' as species, ['red maple'] as common_names, [1, 2] as native_ecoregions, 'Native' as bucket
union all select 'Acer saccharum', ['sugar maple'], [1], 'Native'
union all select 'Opuntia ficus', [], [3], 'Introduced'
union all select 'Tilia cordata', ['linden', 'lime'], [1, 1], null"""

BASE = f"""
key city string;
key tree_id int;
key species string;
property tree_id.dbh float;
property tree_id.plant_date date;
key ecoregion_id int;
property ecoregion_id.biome string;
property species.common_names list<string>;
property species.native_ecoregions list<int>;
property species.bucket string;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;
auto plant_year <- year(plant_date);

datasource trees (tree_id, city, species, dbh: ?dbh, plant_date: ?plant_date)
grain (tree_id)
query '''{TREES}''';

datasource enrichment (species, common_names, native_ecoregions, bucket: ?bucket)
grain (species)
query '''{ENRICHMENT}''';

datasource eco (ecoregion_id, biome) grain (ecoregion_id)
query '''select 1 as ecoregion_id, 'forest' as biome union all select 2, 'forest'
union all select 3, 'desert' ''';
"""

SUMMARY = f"""
auto rollup_count <- count(tree_id);
datasource summary_species (city, species, rollup_count) grain (city, species)
query '''select city, species, count(*) as rollup_count from ({TREES}) group by 1, 2''';
datasource summary_year (city, species, plant_year: ?plant_year, rollup_count)
grain (city, species, plant_year)
query '''select city, species, year(plant_date) as plant_year, count(*) as rollup_count
from ({TREES}) group by 1, 2, 3''';
"""

GENUS = "split(species, ' ')[1]"

FILTERS = {
    "none": "",
    "city": "city = 'A'",
    "species": "species != 'Tilia cordata'",
    "property": "bucket = 'Native'",
    "year": "plant_year > 2000",
}

# `Cover.target_tuple_count`, `Cover.duplicate_target_slice`,
# `Cover.target_by_value`: a target is any function of the summary key.
VALUE = {
    "rename_key": "species as s, count(tree_id) as n",
    "rename_derived": "plant_year as y, count(tree_id) as n",
    "rename_city": "city as c, count(tree_id) as n",
    "key_and_rename": "species, species as s, count(tree_id) as n",
    "key_twice": "species as v, species as l, count(tree_id) as n",
    "scalar_twice": f"{GENUS} as v, {GENUS} as l, count(tree_id) as n",
    "scalar_of_key": f"{GENUS} as g, count(tree_id) as n",
    "scalar_of_stored": "plant_year::int as y, count(tree_id) as n",
    "scalar_of_stored_twice": "plant_year::string as a, plant_year::string as b, count(tree_id) as n",
    "date_part": "plant_date.year as y, count(tree_id) as n",
    "stored_and_inline": "plant_year, year(plant_date) as y, count(tree_id) as n",
    "scalar_beside_key": "upper(species) as u, species as s, count(tree_id) as n",
    "property_of_rename": "species as s, bucket, count(tree_id) as n",
}

# `Cover.filtered_count_exact`, `Cover.case_count_exact`: a condition on the
# key inside the aggregate.
FILTERED = {
    "key": "count(tree_id ? species = 'Acer rubrum') as n",
    "key_by_key": f"species, count(tree_id ? {GENUS} = 'Acer') as n",
    "stored": "species, count(tree_id ? plant_year > 2000) as n",
    "property": "count(tree_id ? bucket = 'Native') as n",
    "membership": "count(tree_id ? species in ('Acer rubrum', 'Opuntia ficus')) as n",
    "or": "count(tree_id ? bucket = 'Native' or bucket = 'Introduced') as n",
    "ratio": "count(tree_id ? bucket = 'Native')::float / count(tree_id)::float as r",
    "beside_plain": "city, count(tree_id ? bucket = 'Native') as k, count(tree_id) as n",
    "list_property": "count(tree_id ? common_names[1] = 'red maple') as n",
}

# `Cover.count_distinct_of_key_exact`: the counted key is unique per row.
DISTINCT = {
    "total": "count_distinct(tree_id) as n",
    "by_key": "species, count_distinct(tree_id) as n",
    "by_stored": "plant_year, count_distinct(tree_id) as n",
    "filtered_ratio": (
        "(count_distinct(tree_id ? bucket = 'Native') / count_distinct(tree_id))::float as r"
    ),
}

# `Cover.ratio_parts_exact`: a total beside any target of the key.
SIBLINGS = {
    "key": "species, count(tree_id) as n, count(tree_id)::float / (count(tree_id) by *)::float as pct",
    "rename": "species as s, count(tree_id) as n, count(tree_id)::float / (count(tree_id) by *)::float as pct",
    "scalar": f"{GENUS} as g, count(tree_id) as n, count(tree_id)::float / (count(tree_id) by *)::float as pct",
    "coalesce": (
        "coalesce(common_names[1], species) as d, count(tree_id) as n, "
        "count(tree_id)::float / (count(tree_id) by *)::float as pct"
    ),
    "total_only": "coalesce(common_names[1], species) as d, count(tree_id) by * as t",
}

# `Cover.exists_by_value`: no aggregate, values of key functions.
EXISTENCE = {
    "key": "species",
    "key_and_property": "species, common_names",
    "property": "common_names[1] as c",
    "scalar": f"{GENUS} as g",
    "stored": "plant_year",
}

# `Cover.keyed_relation_count`: a count by a value the key relates to.
RELATION = {
    "member": "ecoregion_id, count(tree_id) as n",
    "member_property": "biome, count(tree_id) as n",
}

# `Cover.case_on_aggregate_rows`, `Cover.finer_grain_read_fans_out`: a scalar
# of an aggregate's grain is read on its rows, not from a finer table.
OVER_AGGREGATE = {
    "case": "sum(case when bucket = 'Native' then count(tree_id) by bucket end) as n",
    "case_else": (
        "sum(case when bucket = 'Native' then count(tree_id) by bucket else 0 end) as n"
    ),
    "max": "max(case when bucket = 'Native' then count(tree_id) by bucket end) as n",
    "scalar": (
        "sum(case when upper(bucket) = 'NATIVE' then count(tree_id) by bucket end) as n"
    ),
    "key": (
        "sum(case when species = 'Acer rubrum' then count(tree_id) by species end) as n"
    ),
    "key_property": (
        "sum(case when common_names[1] = 'red maple' then count(tree_id) by species end) as n"
    ),
    "bare": "sum(count(tree_id) by bucket) as n",
}

# Shapes no rule covers: a condition, target or count finer than the key.
UNSERVABLE = {
    "finer_condition": "species, count(tree_id ? dbh > 10) as n",
    "finer_target": "dbh, count(tree_id) as n",
    "distinct_non_key": "count_distinct(dbh) as n",
    "distinct_fanned": "ecoregion_id, count_distinct(tree_id) as n",
    "finer_existence": "dbh",
}

# Shapes a rule covers that the planner does not serve yet: each is a miss
# (read from the trees with the right answer), never a wrong summary answer.
# `(rule, shape) -> (reason, filters)`; no filters means every filter.
JOINED_CONDITION = (
    "rule 2: a condition on a property of the summary key needs its join below "
    "the re-aggregating scan, which sees only the summary's own columns"
)
PINNED_TOTAL = (
    "rule 4: under a WHERE on a stored column, a total beside a scalar of the "
    "key right-joins the summary to the trees' distinct keys"
)
NO_EXISTENCE_PROOF = (
    "rule 5: a selection with no aggregate is sourced from its columns' "
    "bindings; the summary is not offered as proof of existence"
)
OPEN: dict[tuple[str, str], tuple[str, tuple[str, ...] | None]] = {
    **{
        ("filtered", shape): (JOINED_CONDITION, None)
        for shape in ("property", "or", "ratio", "beside_plain", "list_property")
    },
    ("distinct", "filtered_ratio"): (
        "rule 2: count_distinct(k ? c) is count(k ? c), a filtered count",
        None,
    ),
    ("value", "scalar_of_key"): (
        "rule 1: a scalar of a key under a WHERE on a stored column",
        ("year",),
    ),
    ("value", "scalar_twice"): (
        "rule 1: a scalar of a key under a WHERE on a stored column",
        ("year",),
    ),
    ("siblings", "scalar"): (PINNED_TOTAL, ("year",)),
    ("siblings", "total_only"): (
        "rule 4: a total alone beside a derived dimension is not rolled",
        None,
    ),
    **{
        ("existence", shape): (
            NO_EXISTENCE_PROOF,
            {
                "key_and_property": ("none", "species", "property", "year"),
                "property": ("none", "species", "property", "year"),
                "stored": None,
            }.get(shape, ("none", "species", "property")),
        )
        for shape in EXISTENCE
    },
    ("over_aggregate", "key_property"): (
        "rule 7: a species property inside the CASE is not read beside the summary",
        None,
    ),
    **{
        ("relation", shape): (
            "rule 6: a summary is never joined through the fan-out it is keyed on",
            None,
        )
        for shape in RELATION
    },
}


def _run(model: str, query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    return sql, sorted(rows, key=repr)


def _query(select: str, where: str) -> str:
    return f"select {select}{' where ' + where if where else ''};"


def _assert_same_answer(query: str, served: bool) -> None:
    _, without = _run(BASE, query)
    sql, with_summary = _run(BASE + SUMMARY, query)
    assert with_summary == without, sql
    assert ('"summary_' in sql and '"trees"' not in sql) == served, sql


def _marks(rule: str, shape: str, where: str) -> list:
    reason, filters = OPEN.get((rule, shape), ("", ()))
    if filters is None or where in filters:
        return [pytest.mark.xfail(strict=True, reason=reason)]
    return []


def _cases(rule: str, shapes: dict[str, str]) -> list:
    return [
        pytest.param(
            shapes[shape],
            FILTERS[where],
            id=f"{shape}-{where}",
            marks=_marks(rule, shape, where),
        )
        for shape, where in itertools.product(shapes, FILTERS)
    ]


@pytest.mark.parametrize("select,where", _cases("value", VALUE))
def test_target_is_a_value_of_the_key(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize("select,where", _cases("filtered", FILTERED))
def test_key_condition_inside_the_aggregate(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize("select,where", _cases("distinct", DISTINCT))
def test_distinct_count_of_the_counted_key(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize("select,where", _cases("siblings", SIBLINGS))
def test_total_beside_a_target(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


# Two species sharing a display name: a plan that groups the partials by
# species, not by the target, answers one row per species.
SHARED_NAMES = BASE.replace("['sugar maple']", "['red maple']").replace(
    "['linden', 'lime']", "['red maple', 'lime']"
)
SHARED_TARGETS = {
    "siblings": SIBLINGS["coalesce"],
    "filtered": (
        "coalesce(common_names[1], species) as d, "
        "count(tree_id ? bucket = 'Native') as k, count(tree_id) by * as t"
    ),
    "sum": "common_names[1] as d, sum(count(tree_id) by species) as n",
}


@pytest.mark.parametrize(
    "select,where",
    [
        pytest.param(select, where, id=f"{shape}-{key}")
        for (shape, select), (key, where) in itertools.product(
            SHARED_TARGETS.items(), FILTERS.items()
        )
    ],
)
def test_target_shared_by_two_keys(select: str, where: str):
    query = _query(select, where)
    _, without = _run(SHARED_NAMES, query)
    sql, with_summary = _run(SHARED_NAMES + SUMMARY, query)
    assert with_summary == without, sql
    assert len({row[0] for row in without}) == len(without)


@pytest.mark.parametrize("select,where", _cases("existence", EXISTENCE))
def test_existence_reads_the_summary(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize("select,where", _cases("relation", RELATION))
def test_relation_keyed_on_the_grain(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize("select,where", _cases("over_aggregate", OVER_AGGREGATE))
def test_scalar_of_an_aggregates_grain(select: str, where: str):
    _assert_same_answer(_query(select, where), served=True)


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            (
                "auto c <- case when bucket = 'Native' then count(tree_id) by bucket end;"
                " select sum(c) as n;"
            ),
            [(4,)],
        ),
        (
            (
                "auto ub <- upper(bucket);"
                " auto c <- case when ub = 'NATIVE' then count(tree_id) by bucket end;"
                " select sum(c) as n;"
            ),
            [(4,)],
        ),
        (
            (
                "select sum(case when bucket = 'Native' then count(tree_id) by bucket end)"
                " as n where city = 'A';"
            ),
            [(3,)],
        ),
        (
            (
                "auto cb <- count(tree_id) by bucket;"
                " select bucket, sum(case when bucket = 'Native' then cb end) as n;"
            ),
            [("Introduced", None), ("Native", 4), (None, None)],
        ),
        (
            (
                "auto cbc <- count(tree_id) by bucket, city;"
                " select city, sum(case when bucket = 'Native' then cbc end) as n;"
            ),
            [("A", 3), ("B", 1)],
        ),
        (
            (
                "auto cb <- count(tree_id) by bucket;"
                " select avg(case when bucket is null then 0 else cb end) as n;"
            ),
            [(2.0,)],
        ),
        (
            "select split(species, ' ')[1] as g where plant_year > 2000;",
            [("Acer",), ("Opuntia",), ("Tilia",)],
        ),
        (
            "select common_names[1] as c where plant_year > 2000;",
            [("linden",), ("red maple",), (None,)],
        ),
    ],
)
def test_tree_plan_answer(query: str, expected: list[tuple]):
    assert _run(BASE, query)[1] == expected


SHAPES = {
    "value": VALUE,
    "filtered": FILTERED,
    "distinct": DISTINCT,
    "siblings": SIBLINGS,
    "existence": EXISTENCE,
    "relation": RELATION,
    "over_aggregate": OVER_AGGREGATE,
}


@pytest.mark.parametrize(
    "select,where",
    [
        pytest.param(SHAPES[rule][shape], FILTERS[where], id=f"{rule}-{shape}-{where}")
        for (rule, shape), (_, filters) in OPEN.items()
        for where in (filters or FILTERS)
    ],
)
def test_open_shape_answers_as_the_trees(select: str, where: str):
    """A shape the planner does not serve yet still answers right."""
    query = _query(select, where)
    assert _run(BASE + SUMMARY, query)[1] == _run(BASE, query)[1]


@pytest.mark.parametrize("select,where", _cases("unservable", UNSERVABLE))
def test_finer_shape_reads_the_trees(select: str, where: str):
    _assert_same_answer(_query(select, where), served=False)


@pytest.mark.parametrize(
    "query",
    [
        # a partial grouped by a stored derived column reads it off the summary
        "auto c1 <- count(tree_id) by species, plant_year; select species, sum(c1) as n;",
        # a total rewritten to the re-sum of a demanded partial, inside an expression
        "auto cb <- count(tree_id) by species; select sum(cb) / count(tree_id) as r;",
        (
            "auto cb <- count(tree_id) by bucket; select sum(case when bucket = 'Native'"
            " then cb end) / count(tree_id) as r where city = 'A';"
        ),
        # a distinct count read as the filtered count it became
        (
            "select (count_distinct(tree_id ? plant_year > 2000)"
            " / count_distinct(tree_id))::float as r;"
        ),
    ],
)
def test_rewritten_partials_read_the_summary(query: str):
    _assert_same_answer(query, served=True)
