# from preql.compiler import compile
from preql.core.models import Select, Grain, Window, WindowOrder, GrainWindow
from preql.parser import parse
from preql.core.hooks import GraphHook

# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query
from preql.parser import parse
from preql.dialect.sql_server import SqlServerDialect

def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

key top_users <- top 10 user_id by post_count;


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


select
    top_users,
    post_count
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    target_grain = Grain(components=[env.concepts["top_users"]])

    print(select.grain)
    print(target_grain)
    print('----')
    assert select.grain == target_grain
    for x in select.output_components:
        print(x.address)
        print([str(z) for z in x.input])
    assert env.concepts["post_count"].with_grain(env.concepts['top_users']) in (select.output_components)
    query = process_query(statement=select, environment=env, hooks=[GraphHook()])

    generator = SqlServerDialect()
    sql = generator.compile_statement(query)
    print(sql)
