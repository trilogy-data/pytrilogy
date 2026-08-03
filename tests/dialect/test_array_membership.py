"""Membership against an array renders in each dialect's own language.

Two shapes share one code path. A literal value list splats to the portable
`in (a, b, c)` everywhere; an array-*valued* RHS unnests to one row per element
and matches by identity, so only the source producing those rows is per-dialect.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import Rendering
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

# The FROM fragment each dialect uses to expose one array element per row, when
# the array is a column that has to be read from a source alongside it.
CORRELATED_FORMS = {
    Dialects.DUCK_DB: "(select unnest(",
    Dialects.POSTGRES: "(select unnest(",
    Dialects.CLICKHOUSE: "(select arrayJoin(",
    Dialects.BIGQUERY: ", unnest(",
    Dialects.SNOWFLAKE: ", lateral flatten(input => ",
    Dialects.TRINO: "cross join unnest(",
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


@pytest.mark.parametrize("dialect,expected", list(CORRELATED_FORMS.items()))
def test_array_valued_membership_uses_the_native_unnest(dialect, expected):
    sql = _render(dialect, ARRAY_VALUED)
    assert expected in sql, sql
    for form in set(CORRELATED_FORMS.values()) - {expected}:
        assert form not in sql, sql


@pytest.mark.parametrize("dialect", NO_ARRAY_DIALECTS)
def test_array_valued_membership_raises_without_an_array_type(dialect):
    with pytest.raises(NotImplementedError, match="does not support array membership"):
        _render(dialect, ARRAY_VALUED)


@pytest.mark.parametrize(
    "dialect", [*CORRELATED_FORMS, *NO_ARRAY_DIALECTS, Dialects.PRESTO]
)
def test_value_list_membership_is_portable(dialect):
    """A literal list needs no array anywhere — including on the dialects that
    have no array type at all."""
    sql = _render(dialect, VALUE_LIST)
    assert "in ('24128','76232')" in sql
    assert "in [" not in sql
    assert "ARRAY_CONSTRUCT" not in sql
    assert "ARRAY[" not in sql


@pytest.mark.parametrize("dialect", [*CORRELATED_FORMS, *NO_ARRAY_DIALECTS])
def test_inlined_array_constant_is_a_value_list(dialect):
    """An array constant rendered inline is a literal list — splat it, don't ask
    the dialect for an array. That is what keeps this portable everywhere."""
    sql = _render(dialect, ARRAY_CONSTANT, parameters=False)
    assert "in ('24128','76232')" in sql
    assert "unnest" not in sql.lower()
    assert "flatten" not in sql.lower()


@pytest.mark.parametrize("dialect,expected", list(STANDALONE_FORMS.items()))
def test_bound_array_parameter_uses_the_native_unnest(dialect, expected):
    """Bound rather than inlined, the same constant is an opaque array value and
    has to go through the dialect's own unnest."""
    sql = _render(dialect, ARRAY_CONSTANT)
    assert expected in sql, sql
    for form in set(STANDALONE_FORMS.values()) - {expected}:
        assert form not in sql, sql
