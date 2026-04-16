import re
from logging import INFO
from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import BuildUnionDatasource, Factory
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
        datasource=BuildUnionDatasource(children=datasource),
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
    # is_home = true means the away source (complete where is_home = false) is mutually
    # exclusive with the query filter; only the home branch should appear in the SQL.
    assert (
        """bigquery-public-data"."ncaa_basketball"."mbb_games_sr" as "home_team_games_sr"""
        in sql
    )
    assert "away_team_games_sr" not in sql
    assert "'NCAA MEN'" in sql


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
        "local.home_games",
        "local.away_games",
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

    # Validate the window's order-by references a CASE on the count and a
    # sum/count ratio. The merge_aggregate optimization may fold the upstream
    # MERGE CTE into the window, so we don't pin to a specific CTE name.
    pattern = re.compile(
        r'rank\(\) over \(order by CASE\s+WHEN\s+"\w+"\."_virt_agg_count_\d+"\s*>\s*10\s+THEN 1\s+ELSE 0\s+END desc,\s*"\w+"\."_virt_agg_sum_\d+"\s*/\s*"\w+"\."_virt_agg_count_\d+" desc\s*\) as "player_rank"',
        re.MULTILINE,
    )
    assert pattern.search(generated), generated.strip()


def test_adhoc08():
    DebuggingHook()
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc08.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    generated = engine.generate_sql(text)[0]
    pattern = r'"(\w+)"\."shot_subtype" is not distinct from "(\w+)"\."shot_subtype"'
    assert re.search(pattern, generated), generated


def test_adhoc9():
    DebuggingHook(INFO)
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc09.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    generated = engine.generate_sql(text)[0]
    pattern = r'"wakeful"."_virt_agg_sum_1889332829440409" as "_virt_agg_sum_1889332829440409"'

    assert not re.search(pattern, generated), generated


def test_adhoc10():
    DebuggingHook(INFO)
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc10.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    engine.generate_sql(text)[0]


def test_adhoc11():
    DebuggingHook(INFO)
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc11.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    env, queries = env.parse(text)
    generated = engine.generate_sql(text)[0]

    assert "UNION ALL" in generated, generated
