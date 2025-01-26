# from trilogy.compiler import compile
from os.path import dirname

from trilogy.core.enums import Modifier
from trilogy.core.models.environment import Environment
from trilogy.parser import parse
from trilogy.parsing.render import render_environment
from trilogy.parsing.parse_engine import MAX_PARSE_DEPTH

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

 
 auto user_badge_count<-count(core.badge_id) by core.user_id;

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

CIRC_QUERY = """import so_concepts.circular as c1;
import so_concepts.circular_dep as c2;"""


def test_select():
    env, parsed = parse(QUERY, environment=Environment(working_path=dirname(__file__)))
    rendered = render_environment(env)
    assert rendered.startswith("import concepts.core as core;")


def test_import_violation():
    env = Environment(working_path=dirname(__file__))

    # dupe additions result in nothing
    env.add_file_import(path="so_concepts.circular", alias="c1")
    env.add_file_import(path="so_concepts.circular", alias="c1")
    # new path is new import
    env.add_file_import(path="so_concepts.circular_dep", alias="c1")

    assert len(env.imports["c1"]) == 2


def test_circular_base():
    env = Environment(working_path=dirname(__file__))

    # first import

    # import posts.c1, with c1.id
    # which imports c2, as c1.c2, giving c1.c2.id
    # which imports c1 c, giving c1.c2.c.id
    # that imports c2, with c2.id

    env.add_file_import(path="so_concepts.circular", alias="c1")
    assert env.concepts["c1.id"]
    assert env.concepts["c1.c2.id"]
    validated = False
    assert len(env.datasources) == MAX_PARSE_DEPTH
    for n, datasource in env.datasources.items():
        for z in datasource.columns:
            self = z.concept
            if z.concept.namespace != datasource.namespace:
                continue
            if z.concept.address not in env.concepts:
                continue
            other = env.concepts[z.concept.address]
            assert self.name == other.name
            assert self.datatype == other.datatype
            assert self.namespace == other.namespace
            assert z.concept == env.concepts[z.concept.address]
            validated = True
    assert validated


def test_circular():
    env, parsed = parse(
        CIRC_QUERY, environment=Environment(working_path=dirname(__file__))
    )
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    assert env.concepts["c1.id"]
    assert env.concepts["c2.id"]
    validated = False
    for n, datasource in env.datasources.items():
        for z in datasource.columns:
            self = z.concept
            if z.concept.namespace != datasource.namespace:
                continue
            if z.concept.address not in env.concepts:
                continue
            other = env.concepts[z.concept.address]
            assert self.name == other.name
            assert self.datatype == other.datatype
            assert self.namespace == other.namespace
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
