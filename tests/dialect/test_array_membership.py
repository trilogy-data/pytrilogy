"""Membership against an array renders in each dialect's own language.

Two shapes share one code path. A literal value list splats to the portable
`in (a, b, c)` everywhere; an array-*valued* RHS unnests to one row per element
and matches by identity, so only the source producing those rows is per-dialect.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import Rendering
from trilogy.core.exceptions import UnsupportedDialectFeature
from trilogy.parser import parse
from trilogy.render import get_dialect_generator

ARRAY_VALUED = """
const zips <- '24128,76232,65084';
key cust_zip string;
datasource customers (
    cust_zip
)
grain (cust_zip)
address customers;

auto qual <- cust_zip ? cust_zip in split(zips, ',');
where cust_zip in qual
select cust_zip;
"""

VALUE_LIST = """
key cust_zip string;
datasource customers (
    cust_zip
)
grain (cust_zip)
address customers;

where cust_zip in ('24128', '76232')
select cust_zip;
"""

ARRAY_CONSTANT = """
const zips <- ['24128', '76232'];
const probe <- '24128';
where probe in zips
select probe;
"""

# The fragment each dialect uses to expose one element per row of an array
# read from the row being filtered, so membership is tested per row.
ROW_FORMS = {
    Dialects.DUCK_DB: "(select unnest(STRING_SPLIT(",
    Dialects.POSTGRES: "(select unnest(string_to_array(",
    Dialects.CLICKHOUSE: "(select arrayJoin(splitByString(",
    Dialects.BIGQUERY: "from unnest(split(",
    Dialects.SNOWFLAKE: "from table(flatten(input => split(",
    Dialects.TRINO: "from unnest(split(",
}

# The same fragment when the array stands alone and needs no source to join.
# Terminated at `where` so no dialect's form is a prefix of another's.
STANDALONE_FORMS = {
    Dialects.DUCK_DB: (
        "from (select unnest(:zips) as unnest_member) as unnest_members where"
    ),
    Dialects.POSTGRES: (
        "from (select unnest(:zips) as unnest_member) as unnest_members where"
    ),
    Dialects.CLICKHOUSE: (
        "from (select arrayJoin(:zips) as unnest_member) as unnest_members where"
    ),
    Dialects.BIGQUERY: "from unnest(:zips) as unnest_member where",
    Dialects.SNOWFLAKE: "from table(flatten(input => :zips)) as unnest_members where",
    Dialects.TRINO: "from unnest(:zips) as unnest_members(unnest_member) where",
}

NO_ARRAY_DIALECTS = [Dialects.SQL_SERVER, Dialects.SQLITE, Dialects.MYSQL]


def _render(dialect: Dialects, model: str, parameters: bool = True) -> str:
    env = Environment()
    _, statements = parse(model, env)
    renderer = get_dialect_generator(
        dialect, rendering=Rendering(parameters=parameters)
    )
    return "\n".join(
        renderer.compile_statements(*renderer.generate_queries(env, statements))
    )


@pytest.mark.parametrize("dialect,expected", list(ROW_FORMS.items()))
def test_array_valued_membership_uses_the_native_unnest(dialect, expected):
    sql = _render(dialect, ARRAY_VALUED)
    assert expected in sql, sql
    for form in set(ROW_FORMS.values()) - {expected}:
        assert form not in sql, sql


@pytest.mark.parametrize("dialect", NO_ARRAY_DIALECTS)
def test_array_valued_membership_raises_without_an_array_type(dialect):
    with pytest.raises(UnsupportedDialectFeature, match="has no array type"):
        _render(dialect, ARRAY_VALUED)


@pytest.mark.parametrize("dialect", [*ROW_FORMS, *NO_ARRAY_DIALECTS, Dialects.PRESTO])
def test_value_list_membership_is_portable(dialect):
    """A literal list needs no array anywhere — including on the dialects that
    have no array type at all."""
    sql = _render(dialect, VALUE_LIST)
    assert "in ('24128','76232')" in sql
    assert "in [" not in sql
    assert "ARRAY_CONSTRUCT" not in sql
    assert "ARRAY[" not in sql


@pytest.mark.parametrize("dialect", [*ROW_FORMS, *NO_ARRAY_DIALECTS])
def test_inlined_array_constant_is_a_value_list(dialect):
    """An array constant rendered inline is a literal list — splat it, don't ask
    the dialect for an array. That is what keeps this portable everywhere."""
    sql = _render(dialect, ARRAY_CONSTANT, parameters=False)
    assert "in ('24128','76232')" in sql
    assert "unnest" not in sql.lower()
    assert "flatten" not in sql.lower()


def test_snowflake_casts_the_variant_member_to_the_probe_type():
    """FLATTEN yields a VARIANT — uncast, the predicate is valid SQL that
    matches nothing. Executed in tests/engine/snowflake."""
    sql = _render(Dialects.SNOWFLAKE, ARRAY_VALUED)
    assert "cast(unnest_members.value as string)" in sql, sql


@pytest.mark.parametrize("dialect,expected", list(STANDALONE_FORMS.items()))
def test_bound_array_parameter_uses_the_native_unnest(dialect, expected):
    """Bound rather than inlined, the same constant is an opaque array value and
    has to go through the dialect's own unnest."""
    sql = _render(dialect, ARRAY_CONSTANT)
    assert expected in sql, sql
    for form in set(STANDALONE_FORMS.values()) - {expected}:
        assert form not in sql, sql


LIST_PROPERTY = """
key tree_id string;
key species string;
property species.genus string;
property species.bloom_months list<int>;

datasource trees (
    tree_id,
    species,
)
grain (tree_id)
query \'\'\'
select 't1' as tree_id, 'Prunus avium' as species
union all select 't2', 'Quercus robur'
union all select 't3', 'Acer rubrum'
\'\'\';

datasource species_info (
    species,
    genus,
    bloom_months,
)
grain (species)
query \'\'\'
select 'Prunus avium' as species, 'Prunus' as genus, [4, 5] as bloom_months
union all select 'Quercus robur', 'Quercus', [5]
union all select 'Acer rubrum', 'Acer', [3, 4]
\'\'\';
"""


@pytest.mark.parametrize(
    "where,expected",
    [
        ("4 in bloom_months", ["t1", "t3"]),
        ("contains(bloom_months, 4)", ["t1", "t3"]),
        ("4 not in bloom_months", ["t2"]),
        ("4 in bloom_months and genus = 'Prunus'", ["t1"]),
        ("4 in bloom_months and genus = 'Quercus'", []),
        ("5 in bloom_months and genus != 'Prunus'", ["t2"]),
    ],
)
def test_membership_in_a_list_property_is_row_scoped(where, expected):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(LIST_PROPERTY)
    for select in ("tree_id", "tree_id, bloom_months"):
        rows = executor.execute_text(
            f"select {select} where {where} order by tree_id asc;"
        )[-1].fetchall()
        assert [row[0] for row in rows] == expected, select


DERIVED_LIST = """
key id int;
property id.tags string;
property id.grp string;
datasource t (id, tags, grp) grain (id) query '''
select 1 as id, 'a,b' as tags, 'x' as grp
union all select 2, 'c', 'x'
union all select 3, 'b,d', 'y'
''';
auto tag_list <- split(tags, ',');
auto grp_ids <- array_agg(id) by grp;
auto b_ids <- id ? 'b' in tag_list;
"""


@pytest.mark.parametrize(
    "query,expected",
    [
        ("select id where 'b' in split(tags, ',')", [(1,), (3,)]),
        ("select id where 'b' in tag_list", [(1,), (3,)]),
        ("select id where 'b' not in tag_list", [(2,)]),
        ("select id where 3 in grp_ids", [(3,)]),
        ("select id where id in grp_ids", [(1,), (2,), (3,)]),
        ("select id where id in b_ids", [(1,), (3,)]),
        ("select grp, count(id) as c where 'b' in tag_list", [("x", 1), ("y", 1)]),
    ],
)
def test_membership_in_a_derived_list_is_row_scoped(query, expected):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(DERIVED_LIST)
    rows = executor.execute_text(f"{query} order by 1 asc;")[-1].fetchall()
    assert [tuple(row) for row in rows] == expected


def _built_where(model: str, query: str):
    from trilogy.core.models.build import Factory

    env = Environment()
    env.parse(model)
    _, statements = env.parse(query)
    return Factory(environment=env).build(statements[-1].where_clause.conditional)


def test_list_membership_builds_as_a_row_comparison():
    from trilogy.core.models.build import BuildComparison, BuildSubselectComparison

    built = _built_where(LIST_PROPERTY, "select tree_id where 4 in bloom_months;")
    assert type(built) is BuildComparison
    assert not any(built.existence_arguments)
    assert {c.address for c in built.row_arguments} == {"local.bloom_months"}

    built = _built_where(DERIVED_LIST, "select id where id in b_ids;")
    assert isinstance(built, BuildSubselectComparison)
    assert any(built.existence_arguments)
