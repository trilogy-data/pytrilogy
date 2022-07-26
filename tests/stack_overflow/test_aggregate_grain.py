
from preql.parser import parse
# from preql.compiler import compile
from os.path import dirname, join
from preql.parsing.exceptions import ParseError
from preql.core.models import Select, Grain



QUERY = '''import concepts.core as core;

select
    core.user_id,
    core.about_me,
    core.display_name,
    count(core.post_id)->user_post_count,
    avg(core.post_length)-> user_avg_post_length
ORDER BY
    user_post_count desc
 limit 10;


select
    core.user_id,
    core.display_name,
    count(core.badge_id) -> user_badge_count
order by
    user_badge_count desc
 limit 10;



select
    core.badge_name,
    sum(user_badge_count)-> total_badge_user_award_count
order by
    user_badge_count desc
 limit 10;'''



def test_select():

    env, parsed = parse(QUERY)
    select:Select = parsed[-1]


    assert select.grain == Grain(components=[env.concepts['core.badge_id']])
