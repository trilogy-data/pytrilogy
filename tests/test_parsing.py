from preql.core.enums import Purpose
from preql.core.models import (
    DataType,
    Parenthetical,
    ProcessedQuery,
    ShowStatement,
    SelectStatement,
    Environment,
)
from preql.core.functions import argument_to_purpose, function_args_to_output_purpose
from preql.parsing.parse_engine import (
    arg_to_datatype,
    parse_text,
)
from preql.constants import MagicConstants
from preql.dialect.base import BaseDialect


def test_in():
    _, parsed = parse_text(
        "const order_id <- 3; SELECT order_id  WHERE order_id IN (1,2,3);"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        Parenthetical,
    ), type(right)
    assert right.content[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "( 1,2,3 )".strip()


def test_not_in():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id NOT IN (1,2,3);"
    )
    query: ProcessedQuery = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        Parenthetical,
    ), type(right)
    assert right.content[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "( 1,2,3 )".strip()


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
    assert query.address.location == "`preqldata.analytics_411641820.events_*`"


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

    for name in ["name_alphabetical", "name_alphabetical_2"]:
        assert name in env.concepts
        assert env.concepts[name].purpose == Purpose.PROPERTY
        assert env.concepts[name].keys == (env.concepts["id"],)


def test_output_purpose():

    env, parsed = parse_text(
        """key id int;
property id.name string;

auto name_alphabetical <- row_number id order by name asc;


rowset test<- select
    name,
    row_number id order by name asc -> name_alphabetical_2
    ;

select 
    count(test.name) -> test_name_count;
"""
    )
    # assert output_purpose == Purpose.METRIC
    for name in ["test_name_count"]:
        assert name in env.concepts
        assert env.concepts[name].purpose == Purpose.METRIC
