from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.hooks import DebuggingHook
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import get_union_sources
from trilogy.core.models.build import Factory
from trilogy.core.processing.nodes.union_node import UnionNode
from trilogy.core.processing.node_generators.select_merge_node import create_union_datasource
working_path = Path(__file__).parent

def test_union_node():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    env.parse(text)

    factory = Factory(environment=env)

    datasources = [factory.build(x) for x in env.datasources.values()]
    team_name = factory.build(env.concepts['team_name'])
    union = get_union_sources(datasources=datasources, concepts = [team_name])
    assert len(union) == 1, "Union sources should return a single source for team_name"
    datasource = union[0]
    build_env = factory.build(env)

    bcandidate, group = create_union_datasource(
        datasource=datasource,
        all_concepts=[team_name],
        accept_partial=False,
        environment=build_env,
        depth=0,
        conditions=None,)
    
    assert bcandidate.partial_concepts == []
    

def test_adhoc01():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    sql = engine.generate_sql(text)[0]
    assert """SELECT
    "home_team_games_sr"."game_id" as "id",
    "home_team_games_sr"."h_points_game" as "points_game",
    "home_team_games_sr"."h_name" as "team_name",
    "home_team_games_sr"."h_id" as "team_id"
FROM
    "bigquery-public-data"."ncaa_basketball"."mbb_games_sr" as "home_team_games_sr""" in sql
    assert """SELECT
    "away_team_games_sr"."game_id" as "id",
    "away_team_games_sr"."a_points_game" as "points_game",
    "away_team_games_sr"."a_name" as "team_name",
    "away_team_games_sr"."a_id" as "team_id"
FROM
    "bigquery-public-data"."ncaa_basketball"."mbb_games_sr" as "away_team_games_sr""" in sql

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

    engine: Executor = Dialects.BIGQUERY.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)

    select = queries[-1]
    # for aggregate in [
    #     # 'home_wins',
    #     # 'away_wins',
    #     "home_games",
    #     "away_games",
    # ]:
    #     print(select.local_concepts[aggregate])
    #     assert select.local_concepts[aggregate].grain.components == {
    #         "game_tall.team.name"
    #     }, env.concepts[aggregate]
    generated = engine.generate_sql(text)[0]
    assert "on 1=1"  in generated, generated


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
