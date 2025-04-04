from pytest import raises

from trilogy import Dialects
from trilogy.constants import MagicConstants, Parsing
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.functions import argument_to_purpose, function_args_to_output_purpose
from trilogy.core.models.author import Comparison
from trilogy.core.models.core import (
    DataType,
    TupleWrapper,
)
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentOptions,
)
from trilogy.core.statements.author import SelectStatement, ShowStatement
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.dialect.base import BaseDialect
from trilogy.parsing.parse_engine import (
    ParseError,
    arg_to_datatype,
    parse_text,
)


def test_in():
    _, parsed = parse_text(
        "const order_id <- 3; SELECT order_id  WHERE order_id IN (1,2,3);"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        TupleWrapper,
    ), type(right)
    assert right[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "(1,2,3)".strip()

    _, parsed = parse_text(
        "const order_id <- 3; SELECT order_id  WHERE order_id IN (1,);"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        TupleWrapper,
    ), type(right)
    assert right[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "(1)".strip()


def test_not_in():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id NOT IN (1,2,3);"
    )
    query: ProcessedQuery = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(right, TupleWrapper), type(right)
    assert right[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "(1,2,3)".strip()


def test_is_not_null():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id is not null;"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(right, MagicConstants), type(right)
    rendered = BaseDialect().render_expr(right)
    assert rendered == "null"


def test_sort():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id desc;"
    )

    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id DESC;"
    )
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id DesC;"
    )


def test_arg_to_datatype():
    assert arg_to_datatype(1.00) == DataType.FLOAT
    assert arg_to_datatype("test") == DataType.STRING


def test_argument_to_purpose(test_environment: Environment):
    assert argument_to_purpose(1.00) == Purpose.CONSTANT
    assert argument_to_purpose("test") == Purpose.CONSTANT
    assert argument_to_purpose(test_environment.concepts["order_id"]) == Purpose.KEY
    assert (
        function_args_to_output_purpose(
            [
                "test",
                1.00,
            ]
        )
        == Purpose.CONSTANT
    )
    assert (
        function_args_to_output_purpose(
            ["test", 1.00, test_environment.concepts["order_id"]]
        )
        == Purpose.PROPERTY
    )
    unnest_env, parsed = parse_text("const random <- unnest([1,2,3,4]);")
    assert (
        function_args_to_output_purpose([unnest_env.concepts["random"]])
        == Purpose.PROPERTY
    )


def test_show(test_environment):
    _, parsed = parse_text(
        "const order_id <- 4; SHOW SELECT order_id  WHERE order_id is not null;"
    )
    query = parsed[-1]
    assert isinstance(query, ShowStatement)
    assert (
        query.content.output_components[0].address
        == test_environment.concepts["order_id"].address
    )


def test_conditional(test_environment):
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id =4 and order_id = 10;"
    )
    query = parsed[-1]
    assert isinstance(query, SelectStatement)
    assert query.where_clause.conditional.operator == BooleanOperator.AND


def test_as_transform(test_environment):
    _, parsed = parse_text("const order_id <- 4; SELECT order_id as new_order_id;")
    query = parsed[-1]
    assert isinstance(query, SelectStatement)


def test_bq_address():
    _, parsed = parse_text(
        """key user_pseudo_id int;
key event_time int;
property event_time.event_date string;

datasource fundiverse (
    event_date: event_date,
    user_pseudo_id: user_pseudo_id,
    event_time: event_time,
)
grain (event_time)
address `preqldata.analytics_411641820.events_*`
;"""
    )
    query = parsed[-1]
    assert query.address.quoted is True
    assert query.address.location == "preqldata.analytics_411641820.events_*"


def test_purpose_and_keys():
    env, parsed = parse_text(
        """key id int;
property id.name string;

auto name_alphabetical <- row_number id order by name asc;


select
    id,
    name,
    row_number id order by name asc -> name_alphabetical_2
    ;
"""
    )

    for name in [
        "name_alphabetical",
    ]:
        assert f"local.{name}" in env.concepts
        assert env.concepts[name].purpose == Purpose.PROPERTY
        assert env.concepts[name].keys == {env.concepts["id"].address}


def test_purpose_and_derivation():
    env, parsed = parse_text(
        """key id int;
key other_id int;
property <id, other_id>.join_id <- id*10+other_id;


select 
    join_id
;
"""
    )

    for name in ["join_id"]:
        assert env.concepts[name].purpose == Purpose.PROPERTY
        assert env.concepts[name].keys == {
            env.concepts["id"].address,
            env.concepts["other_id"].address,
        }


def test_output_purpose():
    env, parsed = parse_text(
        """key id int;
property id.name string;

auto name_alphabetical <- row_number id order by name asc;


rowset test<- select
    name,
    row_number id order by name asc -> name_alphabetical_2
    ;

auto test_name_count <- count(test.name);

select 
    test_name_count;
"""
    )
    # assert output_purpose == Purpose.METRIC
    for name in ["test_name_count"]:
        assert env.concepts[name].purpose == Purpose.METRIC


def test_between():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id BETWEEN 3 and 5;"
    )
    query: ProcessedQuery = parsed[-1]
    left = query.where_clause.conditional.left
    assert isinstance(
        left,
        Comparison,
    ), type(left)
    assert left.operator == ComparisonOperator.GTE
    assert left.right == 3

    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        Comparison,
    ), type(right)
    assert right.operator == ComparisonOperator.LTE
    assert right.right == 5


def test_the_comments():
    _, parsed = parse_text(
        """const
         # comment here?
           order_id <- 4; SELECT 
        # TOOD - add in more columns?
        order_id   # this is the order id
        WHERE 
        # order_id should not be null
        order_id
        # in this comp
          is not 
        null; # nulls are the worst
        
        """
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(right, MagicConstants), type(right)
    rendered = BaseDialect().render_expr(right)
    assert rendered == "null"


def test_purpose_nesting():
    env, parsed = parse_text(
        """key year int;
"""
    )

    env2: Environment = Environment()
    env2.add_import("dates", env)

    env2, _ = parse_text(
        """
property <dates.year>.generation <-
CASE WHEN dates.year BETWEEN 1883 AND 1900 THEN 'Lost Generation'
        WHEN dates.year BETWEEN 1901 AND 1927 THEN 'The Greatest Generation'
        WHEN dates.year BETWEEN 1928 AND 1945 THEN 'The Silent Generation'
        WHEN dates.year BETWEEN 1946 AND 1964 THEN 'Baby Boomer'
        WHEN dates.year BETWEEN 1965 AND 1980 THEN 'Generation X'
        WHEN dates.year BETWEEN 1981 AND 1996 THEN 'Millennials'
        WHEN dates.year BETWEEN 1997 AND 2012 THEN 'Generation Z'
        ELSE 'Unknown'
    END;
    """,
        env2,
    )

    assert env2.concepts["dates.generation"].purpose == Purpose.PROPERTY


def test_rawsql():
    env, parsed = parse_text(
        """
raw_sql('''select 1''');

select 1 as test;

"""
    )
    assert parsed[0].text == "select 1"


def test_circular_aliasing():
    from trilogy.hooks.query_debugger import DebuggingHook

    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    test_case = """key composite_id string;

property composite_id.first <- split(composite_id, '-')[1];
property composite_id.second <- split(composite_id, '-')[2];

auto composite_id_alt <- concat(first, '-', second);

merge composite_id_alt into composite_id;

datasource random (
    first:first,
    second:second
)
grain (composite_id)
query '''
select '123' as first, 'abc' as second
'''
;

datasource metrics (
    composite_id: composite_id,
)
grain (composite_id)
query '''
select '123-abc' as composite_id
'''
;


select first, second;


"""
    executor.parse_text(test_case)

    results = executor.execute_text(test_case)[0].fetchall()

    assert results == [("123", "abc")]


def test_circular_aliasing_inverse():
    from trilogy.hooks.query_debugger import DebuggingHook

    executor = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])
    test_case = """key composite_id string;

property composite_id.first <- split(composite_id, '-')[1];
property composite_id.second <- split(composite_id, '-')[2];

auto composite_id_alt <- concat(first, '-', second);

merge composite_id_alt into composite_id;


datasource metrics (
    first:first,
    second:second
)
grain (composite_id)
query '''
select '123' as first, 'abc' as second
'''
;


select composite_id;


"""
    executor.parse_text(test_case)

    assert (
        "local.composite_id_alt"
        in executor.environment.concepts["local.composite_id"].pseudonyms
    )
    assert (
        "local.composite_id"
        in executor.environment.alias_origin_lookup["local.composite_id_alt"].pseudonyms
    )
    results = executor.execute_text(test_case)[0].fetchall()

    assert results == [("123-abc",)]


def test_map_definition():
    env, parsed = parse_text(
        """
key id int;
property id.labels map<string, int>;

"""
    )
    assert env.concepts["labels"].datatype.key_type == DataType.STRING
    assert env.concepts["labels"].datatype.value_type == DataType.INTEGER


def test_map_string_access():
    env, parsed = parse_text(
        """
const labels <- { 'a': 1, 'b': 2, 'c': 3 };


select
    labels['a'] as label_a,
;

"""
    )
    assert env.concepts["labels"].datatype.key_type == DataType.STRING
    assert env.concepts["labels"].datatype.value_type == DataType.INTEGER


def test_empty_string():
    env, parsed = parse_text(
        """
const labels <- '';


"""
    )
    assert env.concepts["labels"].datatype == DataType.STRING


def test_struct_attr_access():
    text = """
const labels <- struct(a=1, b=2, c=3);


select
    labels,
    labels.a as label_a
;

"""
    env, parsed = parse_text(text)
    assert env.concepts["labels"].datatype.fields_map["a"] == 1

    assert env.concepts["labels.a"].concept_arguments[0].name == "labels"

    results = Dialects.DUCK_DB.default_executor().execute_text(text)[0]

    assert results.fetchall()[0] == (
        {"a": 1, "b": 2, "c": 3},
        1,
    )


def test_datasource_colon():
    text = """
key x int;
key y int;

datasource test (
x:x,
y:y)
grain(x)
address `abc:def`
;


select x;
"""
    env, parsed = parse_text(text)

    results = Dialects.DUCK_DB.default_executor().generate_sql(text)[0]

    assert '"abc:def" as test' in results

    text = """
key x int;
key y int;

datasource test (
x:x,
y:y)
grain(x)
address abcdef
;


select x;
"""
    env, parsed = parse_text(text)

    results = Dialects.DUCK_DB.default_executor().generate_sql(text)[0]

    assert "abcdef as test" in results, results


def test_datasource_where_equivalent():
    text = """
key x int;
key y int;

datasource test (
x:x,
y:~y)
grain(x)
complete where y > 10
address `abc:def`
;


"""
    env, parsed = parse_text(text)

    ds = parsed[-1]
    assert ds.non_partial_for.conditional.right == 10


def test_datasource_quoted():
    text = """
key x int;
key y int;

datasource test (
`x 2`:x,
`y`:~y)
grain(x)
complete where y > 10
address `abc:def`
;


"""
    env, parsed = parse_text(text)

    ds = parsed[-1]
    assert ds.columns[0].alias == "x 2"


def test_datasource_from_persist():
    text = """
key x int;
key y int;

datasource test (
x:x,
y:y)
grain(x)
address `abc:def`
;

persist alias into tbl_alias from
select
x,
y
where y>10;



"""
    env, parsed = parse_text(text)

    ds: Datasource = parsed[-1].datasource
    assert ds.non_partial_for.conditional.right == 10
    assert not ds.where


def test_filter_concise():
    text = """
key x int;
key y int;

datasource test (
x:x,
y:y)
grain(x)
address `abc:def`
;

auto filtered_test <- x ? y > 10;

select filtered_test;
"""
    env, parsed = parse_text(text)

    results = Dialects.DUCK_DB.default_executor().generate_sql(text)[0]

    assert "filtered_test" in results, results


def test_unnest_parsing():
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
)''';
"""

    env, parsed = parse_text(x)
    assert env.concepts["split"].datatype == DataType.INTEGER


def test_non_file_imports():

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
''';
""",
                    "test_dep": """
key x int;
""",
                }
            )
        )
    )
    assert isinstance(env.config.import_resolver, DictImportResolver)
    env.parse(
        """
import std.geography;
import test;

key fun_lat float::latitude;
               
select x % 10 -> x_mod_10;
               
            
"""
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
    sum(scalar)->scalar
;
"""
    with raises(ParseError):
        env, parsed = parse_text(
            x, parse_config=Parsing(strict_name_shadow_enforcement=True)
        )
