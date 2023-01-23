# # from preql.compiler import compile
# from preql.core.models import Select, Grain, Window, WindowOrder, GrainWindow
# from preql.parser import parse
# from preql.core.hooks import GraphHook
#
# # from preql.compiler import compile
# from preql.core.models import Select, Grain, ProcessedQuery, QueryDatasource
# from preql.core.query_processor import process_query
# from preql.parser import parse
# from preql.dialect.sql_server import SqlServerDialect
#
# def test_select():
#     from preql.constants import logger
#     from logging import StreamHandler, DEBUG
#     logger.setLevel(DEBUG)
#     logger.addHandler(StreamHandler())
#     declarations = """
# key user_id int metadata(description="the description");
# property user_id.display_name string metadata(description="The display name ");
# property user_id.about_me string metadata(description="User provided description");
#
#
# key post_id int;
# metric post_count <-count(post_id);
#
#
# datasource posts (
#     user_id: user_id,
#     id: post_id
#     )
#     grain (post_id)
#     address bigquery-public-data.stackoverflow.post_history
# ;
#
# select
#     user_id,
#     count(post_id) -> user_post_count
# ;
#
# metric avg_user_post_count <- avg(user_post_count);
#
#
# datasource users (
#     id: user_id,
#     display_name: display_name,
#     about_me: about_me,
#     )
#     grain (user_id)
#     address bigquery-public-data.stackoverflow.users
# ;
#
#
# select
#     avg_user_post_count
# ;
#
#
#     """
#     env, parsed = parse(declarations)
#
#     cte= QueryDatasource()
#
#     output_columns = [env.concepts['top_users']]
#
#     query = ProcessedQuery(output_columns)
#
#     generator = SqlServerDialect()
#     sql = generator.compile_statement(query)
#     print(sql)
