# from trilogy.compiler import compile
from os.path import dirname

from trilogy.core.models.author import Grain
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import SelectStatement
from trilogy.parser import parse

QUERY = """import concepts.core as core;

select
    core.user_id,
    core.about_me,
    core.display_name,
    count(core.post_id)->user_post_count,
    avg(core.post_length)-> user_avg_post_length
ORDER BY
    user_post_count desc
 limit 10;


auto user_badge_count <- count(core.badge_id) by core.user_id;

select
    core.user_id,
    core.display_name,
    user_badge_count
order by
    user_badge_count desc
 limit 10;



select
    core.badge_name,
    core.badge_id,
    sum(user_badge_count)-> total_badge_user_award_count
order by
    total_badge_user_award_count desc
 limit 10;"""


def test_select() -> None:
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    select: SelectStatement = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["core.badge_id"]])
