from pytest import raises

from trilogy.constants import Parsing
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentOptions,
)
from trilogy.parsing.parse_engine import (
    NameShadowError,
    parse_text,
)


def test_import_shows_source():

    env = Environment(
        config=EnvironmentOptions(
            import_resolver=DictImportResolver(
                content={
                    "test": """
import test_dep as test_dep;
key x int;
datasource test (
x: x)
grain(x)
query '''
select 1 as x
union all
select 11 as x
''' TYPO
""",
                    "test_dep": """
key x int;
""",
                }
            )
        )
    )
    assert isinstance(env.config.import_resolver, DictImportResolver)

    with raises(Exception, match="Unable to import 'test', parsing error") as e:
        env.parse(
            """
        import test;
                
    select x % 10 -> x_mod_10;
                
                
    """
        )
        assert "TYPO" in str(e.value)
        assert 1 == 0


def test_concept_shadow_warning():
    x = """
key scalar int;    
property scalar.int_array list<int>;

key split <- unnest(int_array);

datasource avalues (
    int_array: int_array,
	scalar: scalar
    ) 
grain (scalar) 
query '''(
select [1,2,3,4] as int_array, 2 as scalar
union all
select [5,6,7,8] as int_array, 4 as scalar
)''';

SELECT
    int_array,
    1+2->scalar
;
"""
    with raises(NameShadowError) as e:
        env, parsed = parse_text(
            x, parse_config=Parsing(strict_name_shadow_enforcement=True)
        )
        assert "abc" in str(e)
    x = """
key scalar int;    
property scalar.int_array list<int>;

key split <- unnest(int_array);

datasource avalues (
    int_array: int_array,
	scalar: scalar
    ) 
grain (scalar) 
query '''(
select [1,2,3,4] as int_array, 2 as scalar
union all
select [5,6,7,8] as int_array, 4 as scalar
)''';

SELECT
    int_array,
    sum(scalar)->scalar
;
"""
    with raises(InvalidSyntaxException):
        env, parsed = parse_text(
            x, parse_config=Parsing(strict_name_shadow_enforcement=True)
        )


def test_parsing_bad_order():
    x = """
key scalar int;    
property scalar.int_array list<int>;

key split <- unnest(int_array);

datasource avalues (
    int_array: int_array,
	scalar: scalar
    ) 
grain (scalar) 
query '''(
select [1,2,3,4] as int_array, 2 as scalar
union all
select [5,6,7,8] as int_array, 4 as scalar
)''';

SELECT
    int_array,
    sum(scalar)->scalar
order 
    int_array asc
;
"""
    with raises(InvalidSyntaxException) as e:

        env, parsed = parse_text(
            x, parse_config=Parsing(strict_name_shadow_enforcement=True)
        )
        assert "^" in str(e)
