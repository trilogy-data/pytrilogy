# from preql.compiler import compile
from os.path import dirname

from preql.core.models import Environment
from preql.core.enums import Modifier
from preql.parser import parse
from preql.parsing.render import render_environment

QUERY = """import concepts.core as core;
import so_concepts.circular as circular;

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
    core.badge_id,
    sum(user_badge_count)-> total_badge_user_award_count
order by
    user_badge_count desc
 limit 10;"""

CIRC_QUERY = """import so_concepts.circular as c1;
import so_concepts.circular_dep as c2;"""


def test_select():
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    rendered = render_environment(env)
    assert rendered.startswith("import concepts.core as core;")


def test_circular():
    env, parsed = parse(
        CIRC_QUERY, environment=Environment(working_path=dirname(__file__))
    )
    # raise ValueError(env.concepts.keys())
    assert env.concepts["c1.id"]
    assert env.concepts["c2.id"]
    validated = False
    for n, datasource in env.datasources.items():
        for z in datasource.columns:
            self = z.concept
            if z.concept.namespace != datasource.namespace:
                continue
            other = env.concepts[z.concept.address]
            assert self.name == other.name
            assert self.datatype == other.datatype
            assert self.purpose == other.purpose
            assert self.namespace == other.namespace
            assert self.grain.set == other.grain.set
            assert self.grain == other.grain
            assert self.keys == other.keys
            assert z.concept == env.concepts[z.concept.address]
            validated = True
    assert validated


def test_partial():
    env, parsed = parse(
        CIRC_QUERY, environment=Environment(working_path=dirname(__file__))
    )
    # raise ValueError(env.concepts.keys())
    p_candidate = [c for c in env.datasources["c1.posts"].columns if c.alias == "id2"]
    assert Modifier.PARTIAL in p_candidate[0].modifiers
    assert p_candidate[0].is_complete is False
