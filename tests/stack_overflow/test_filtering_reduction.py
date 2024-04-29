from os.path import dirname

from preql.core.models import Select, Environment
from preql.core.query_processor import process_query
from preql.parser import parse
from preql.hooks.query_debugger import DebuggingHook


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
    env, parsed = parse(
        QUERY,
        environment=Environment(working_path=dirname(__file__)),
    )
    select: Select = parsed[-1]
    # for item in select.selection:
    #    if item.content
    #    assert item.content.grain == Grain(components=[env.concepts["tag.name"]])
    process_query(statement=select, environment=env, hooks=[DebuggingHook()])


def test_filtering_reduction_two():
    QUERY = """
import so_concepts.badge as badge;
import so_concepts.question as question;
import so_concepts.user as user;
import so_concepts.answer as answer;
import so_concepts.user_metrics as user_metrics;
import so_concepts.tag as tag;

SELECT
    question.top_ids,
    answer.comment_count
order
    by answer.comment_count desc
limit 10
    ;"""
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    select: Select = parsed[-1]
    process_query(statement=select, environment=env)
