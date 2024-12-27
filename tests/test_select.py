# from trilogy.compiler import compile
from trilogy import Dialects, BoundEnvironment
from trilogy.core.parse_models import Grain, SelectStatement
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
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;

    auto post_count <- count(post_id);

    """
    env, parsed = parse(declarations)

    q1 = """select
    # a comment
    user_id,
    about_me,
    post_count
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
    assert (
        select.grain.components
        == Grain(components=[env.concepts["about_me"]]).components
    )


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
        address `bigquery-public-data.stackoverflow.post_history`
    ;


    datasource users (
        id: user_id,
        display_name: display_name,
        about_me: about_me,
        )
        grain (user_id)
        address `bigquery-public-data.stackoverflow.users`
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
    assert select.hidden_components == set([env.concepts["b"].address])
    assert select.output_components == [env.concepts["a"], env.concepts["b"]]
    query = process_query(statement=select, environment=env)

    generator = BigqueryDialect()

    text = generator.compile_statement(query)
    assert ":b = 2" in text
    assert "as `b`" not in text


def test_having_without_select():
    q1 = """
    const a <- 1;
    const b <- 2;
    
    select
        a,

    having b =2
    ;"""
    failed = False
    try:
        env, parsed = parse(q1)
    except Exception as e:
        assert "that is not in the select projection in the HAVING clause" in str(e)
        failed = True
    assert failed


def test_local_select_concepts():
    q1 = """

key id int;
datasource local (

    id: id
    )
    grain (id)
query '''

SELECT 1 as id
''';


select id + 2 as three;

"""

    env, parsed = parse(q1)

    result = Dialects.DUCK_DB.default_executor(environment=env).execute_text(q1)[-1]
    assert result.fetchone().three == 3


def test_select_from_components():
    env = BoundEnvironment()
    q1 = """

key id int;
property id.class int;
property id.name string;

select
    class,
    upper(id.name)-> upper_name,
    count(id) ->class_id_count,
;
"""
    env, statements = env.parse(q1)

    select: SelectStatement = statements[-1]

    assert select.grain.components == {"local.class", "local.upper_name"}
    assert select.local_concepts["local.class_id_count"].grain.components == {
        "local.class",
        "local.upper_name",
    }

    # SelectStatement.from_inputs(
    #     environment=env,
    #     selection=[SelectItem(concept=env.concepts["id"]),
    #                SelectItem(concept=env.concepts["id.class"])],
    #     input_components=[],
    # )
