from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.hooks import DebuggingHook
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import get_union_sources
from trilogy.core.models.build import Factory

working_path = Path(__file__).parent

def test_union_node():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    env.parse(text)

    factory = Factory(environment=env)

    datasources = [factory.build(x) for x in env.datasources.values()]
    union = get_union_sources(datasources=datasources, concepts = [factory.build(env.concepts['team_name'])])
    assert len(union) == 1, "Union sources should return a single source for team_name"
    union_ds = union[0][0]
    

def test_adhoc01():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    engine.generate_sql(text)[0]


def test_adhoc02():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    with raises(UnresolvableQueryException):
        engine.generate_sql(text)[0]


def test_adhoc03():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc03.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    select = queries[-1]
    for aggregate in [
        # 'home_wins',
        # 'away_wins',
        "home_games",
        "away_games",
    ]:
        print(select.local_concepts[aggregate])
        assert select.local_concepts[aggregate].grain.components == {
            "game_tall.team.name"
        }, env.concepts[aggregate]
    generated = engine.generate_sql(text)[0]
    assert "on 1=1" not in generated, generated


def test_adhoc04():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc04.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    generated = engine.generate_sql(text)[0]

    assert '"game_tall_is_home" = False THEN 1 ELSE NULL END' in generated, generated


def test_adhoc05():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc05.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    assert env.concepts["team_id"].grain.components == {"game_tall.team.id"}
    assert env.concepts["win_rate_difference"].grain.components == {
        "game_tall.team.id"
    }, env.concepts["win_rate_difference"]
    assert queries[-1].grain.components == {
        "game_tall.team.id",
    }, queries[-1].grain.components
    engine.generate_sql(text)[0]


def test_adhoc06():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc06.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    with raises(NotImplementedError):
        engine.generate_sql(text)[0]
