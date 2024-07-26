# from trilogy.compiler import compile
from trilogy.core.models import Grain
from trilogy.core.models import SelectStatement
from trilogy.core.query_processor import process_query
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.parser import parse


def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");
key post_id int;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
;


    """
    env, parsed = parse(declarations)

    q1 = """select
    # a comment
    user_id,
    about_me,
    count(post_id)->post_count
;"""
    env, parse_one = parse(q1, environment=env)

    select: SelectStatement = parse_one[-1]
    assert select.grain == Grain(components=[env.concepts["user_id"]])

    q2 = """select
    about_me,
    post_count
;"""
    env, parse_two = parse(q2, environment=env)

    select: SelectStatement = parse_two[-1]
    assert select.grain == Grain(components=[env.concepts["about_me"]])


def test_double_aggregate():
    declarations = """
    key user_id int metadata(description="the description");
    property user_id.display_name string metadata(description="The display name ");
    property user_id.about_me string metadata(description="User provided description");
    key post_id int;


    datasource posts (
        user_id: user_id,
        id: post_id
        )
        grain (post_id)
        address bigquery-public-data.stackoverflow.post_history
    ;


    datasource users (
        id: user_id,
        display_name: display_name,
        about_me: about_me,
        )
        grain (user_id)
        address bigquery-public-data.stackoverflow.users
    ;


        """
    env, parsed = parse(declarations)

    q1 = """
    metric post_count<- count(post_id);
    metric distinct_post_count <- count_distinct(post_id);
    
    metric user_count <- count(user_id);
    
    select
        post_count,
        distinct_post_count,
        user_count
    ;"""
    env, parsed = parse(q1, environment=env)
    select: SelectStatement = parsed[-1]

    query = process_query(statement=select, environment=env)

    generator = BigqueryDialect()
    generator.compile_statement(query)


def test_modifiers():
    q1 = """
    const a <- 1;
    const b <- 2;
    
    select
        a,
        --b,
    where b =2
    ;"""
    env, parsed = parse(q1)
    select: SelectStatement = parsed[-1]
    assert select.hidden_components == [env.concepts["b"]]
    assert select.output_components == [env.concepts["a"], env.concepts["b"]]
    query = process_query(statement=select, environment=env)

    generator = BigqueryDialect()

    text = generator.compile_statement(query)
    assert "2 = 2" in text
    assert "as `b`" not in text
