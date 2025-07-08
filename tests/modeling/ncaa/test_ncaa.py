import re
from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import Factory
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.node_generators.select_merge_node import (
    create_union_datasource,
)
from trilogy.hooks import DebuggingHook

working_path = Path(__file__).parent


def test_union_node():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    env.parse(text)

    factory = Factory(environment=env)

    datasources = [factory.build(x) for x in env.datasources.values()]
    team_name = factory.build(env.concepts["team_name"])
    union = get_union_sources(datasources=datasources, concepts=[team_name])
    assert len(union) == 1, "Union sources should return a single source for team_name"
    datasource = union[0]
    build_env = factory.build(env)

    bcandidate, group = create_union_datasource(
        datasource=datasource,
        all_concepts=[team_name],
        accept_partial=False,
        environment=build_env,
        depth=0,
        conditions=None,
    )

    assert bcandidate.partial_concepts == []


def test_adhoc01():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    sql = engine.generate_sql(text)[0]
    assert (
        """SELECT
    "home_team_games_sr"."game_id" as "id",
    "home_team_games_sr"."h_points_game" as "points_game",
    "home_team_games_sr"."h_name" as "team_name",
    "home_team_games_sr"."h_id" as "team_id"
FROM
    "bigquery-public-data"."ncaa_basketball"."mbb_games_sr" as "home_team_games_sr"""
        in sql
    )
    assert (
        """SELECT
    "away_team_games_sr"."game_id" as "id",
    "away_team_games_sr"."a_points_game" as "points_game",
    "away_team_games_sr"."a_name" as "team_name",
    "away_team_games_sr"."a_id" as "team_id"
FROM
    "bigquery-public-data"."ncaa_basketball"."mbb_games_sr" as "away_team_games_sr"""
        in sql
    )


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
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc06.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    with raises(NotImplementedError):
        engine.generate_sql(text)[0]


def test_adhoc07():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc07.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    assert env.concepts["scoring_criteria"].grain.components == {
        "player.id"
    }, env.concepts["scoring_criteria"].grain.components
    generated = engine.generate_sql(text)[0]

    # Regex pattern to validate the SQL CTE structure
    target = r"""(?i)\s*WITH\s+
    (\w+)\s+as\s*\(\s*
    SELECT\s+
    "game_events"\."game_id"\s+as\s+"game_id",\s*
    "game_events"\."player_full_name"\s+as\s+"player_full_name"\s+
    FROM\s+
    "bigquery-public-data"\."ncaa_basketball"\."mbb_pbp_sr"\s+as\s+"game_events"\s+
    GROUP\s+BY\s+
    "game_events"\."game_id",\s*
    "game_events"\."player_full_name"\s*\),\s*
    (\w+)\s+as\s*\(\s*
    SELECT\s+
    "game_events"\."player_full_name"\s+as\s+"player_full_name",\s*
    sum\("game_events"\."points_scored"\)\s+as\s+"_virt_agg_sum_\d+"\s+
    FROM\s+
    "bigquery-public-data"\."ncaa_basketball"\."mbb_pbp_sr"\s+as\s+"game_events"\s+
    GROUP\s+BY\s+
    "game_events"\."player_full_name"\s*\),\s*
    (\w+)\s+as\s*\(\s*
    SELECT\s+
    "\1"\."player_full_name"\s+as\s+"player_full_name",\s*
    CASE\s+
    WHEN\s+count\("\1"\."game_id"\)\s*>\s*10\s+THEN\s+1\s+
    ELSE\s+0\s+
    END\s+as\s+"eligible",\s*
    count\("\1"\."game_id"\)\s+as\s+"_virt_agg_count_\d+"\s+
    FROM\s+
    "\1"\s+
    GROUP\s+BY\s+
    "\1"\."player_full_name"\s*\),\s*
    (\w+)\s+as\s*\(\s*
    SELECT\s+
    "\2"\."_virt_agg_sum_\d+"\s+as\s+"_virt_agg_sum_\d+",\s*
    "\3"\."_virt_agg_count_\d+"\s+as\s+"_virt_agg_count_\d+",\s*
    "\3"\."eligible"\s+as\s+"eligible",\s*
    "\3"\."player_full_name"\s+as\s+"player_full_name"\s+
    FROM\s+
    "\3"\s+
    INNER\s+JOIN\s+"\2"\s+on\s+"\3"\."player_full_name"\s*=\s*"\2"\."player_full_name"\s*\)\s*
    SELECT\s+
    "\4"\."player_full_name"\s+as\s+"player_full_name",\s*
    rank\(\)\s+over\s*\(\s*order\s+by\s+"\4"\."eligible"\s+desc,\s*"\4"\."_virt_agg_sum_\d+"\s*/\s*"\4"\."_virt_agg_count_\d+"\s+desc\s*\)\s+as\s+"player_rank"\s+
    FROM\s+
    "\4"\s+
    LIMIT\s*\(\s*100\s*\)\s*"""

    assert re.match(target, generated, re.VERBOSE)


def test_adhoc08():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc08.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    generated = engine.generate_sql(text)[0]
    assert (
        '("abundant"."shot_subtype" = "juicy"."shot_subtype" or ("abundant"."shot_subtype" is null and "juicy"."shot_subtype" is null))'
        in generated
    ), generated
