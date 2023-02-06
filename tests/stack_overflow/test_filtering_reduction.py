from os.path import dirname

from preql.core.models import Select, Grain, Environment
from preql.parser import parse
from preql.core.query_processor import process_query


def test_filtering_reduction():
    QUERY = """
import so_concepts.badge as badge;
import so_concepts.question as question;
import so_concepts.user as user;
import so_concepts.answer as answer;
import so_concepts.user_metrics as user_metrics;
import so_concepts.tag as tag;

SELECT
    tag.name,
    question.count,
where 
    user.location = 'Germany'
order by
    question.count desc
     limit 10
    ;"""
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    select: Select = parsed[-1]
    query = process_query(statement=select, environment=env)
    # cte_post_tags_users_at_tag_name_9745365236361274
    for query in query.ctes:
        print(query.name)
        if query.name.startswith("cte_post_tags_users_at_tag_name_"):
            print(query.grain)
            print(query.group_to_grain)
            for x in query.output_columns:
                print(x)
