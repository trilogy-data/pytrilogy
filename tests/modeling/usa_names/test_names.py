from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook


def test_ranking():
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    query = """import names as names;

select
    names.year,
    names.name,
    rank names.name by sum(names.births) by names.name desc  as name_rank
order by
    names.year asc
limit 100
;"""

    sql = exec.generate_sql(query)[0]

    assert 'sum(names_usa_names."number") as' in sql, sql


def test_ranking_inclusion():
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    query = """import names as names;

import names as names;

select
    names.year,
    names.name,
    sum(names.births) ->total_births,
    rank names.name by sum(names.births) by names.name desc as name_rank
order by
    name_rank asc
;"""

    sql = exec.generate_sql(query)[0]

    assert env.concepts["name_rank"].keys == set(["names.name"]), env.concepts[
        "name_rank"
    ].keys
    assert (
        sql.strip()
        == """WITH 
wakeful as (
SELECT
    names_usa_names."name" as "names_name",
    sum(names_usa_names."number") as "_virt_agg_sum_7286114413769231"
FROM
    "bigquery-public-data.usa_names.usa_1910_current" as names_usa_names
GROUP BY 
    names_usa_names."name"),
highfalutin as (
SELECT
    names_usa_names."name" as "names_name",
    names_usa_names."year" as "names_year",
    sum(names_usa_names."number") as "total_births"
FROM
    "bigquery-public-data.usa_names.usa_1910_current" as names_usa_names
GROUP BY 
    names_usa_names."name",
    names_usa_names."year"),
cheerful as (
SELECT
    rank() over (order by wakeful."_virt_agg_sum_7286114413769231" desc ) as "name_rank",
    wakeful."_virt_agg_sum_7286114413769231" as "_virt_agg_sum_7286114413769231",
    wakeful."names_name" as "names_name"
FROM
    wakeful),
thoughtful as (
SELECT
    cheerful."_virt_agg_sum_7286114413769231" as "_virt_agg_sum_7286114413769231",
    cheerful."name_rank" as "name_rank",
    cheerful."names_name" as "names_name"
FROM
    cheerful)
SELECT
    highfalutin."names_year" as "names_year",
    highfalutin."names_name" as "names_name",
    highfalutin."total_births" as "total_births",
    thoughtful."name_rank" as "name_rank"
FROM
    thoughtful
    INNER JOIN highfalutin on thoughtful."names_name" = highfalutin."names_name"
ORDER BY 
    thoughtful."name_rank" asc""".strip()
    )
