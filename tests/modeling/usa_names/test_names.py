import re
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

    assert 'sum("names_usa_names"."number") as' in sql, sql


def test_ranking_inclusion():
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    query = """import names as names;
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
    "names_usa_names"."name" as "names_name",
    sum("names_usa_names"."number") as "_virt_agg_sum_7286114413769231"
FROM
    "bigquery-public-data"."usa_names"."usa_1910_current" as "names_usa_names"
GROUP BY 
    "names_usa_names"."name"),
highfalutin as (
SELECT
    "names_usa_names"."name" as "names_name",
    "names_usa_names"."year" as "names_year",
    sum("names_usa_names"."number") as "total_births"
FROM
    "bigquery-public-data"."usa_names"."usa_1910_current" as "names_usa_names"
GROUP BY 
    "names_usa_names"."name",
    "names_usa_names"."year"),
cheerful as (
SELECT
    "wakeful"."names_name" as "names_name",
    rank() over (order by "wakeful"."_virt_agg_sum_7286114413769231" desc ) as "name_rank"
FROM
    "wakeful")
SELECT
    "highfalutin"."names_year" as "names_year",
    "highfalutin"."names_name" as "names_name",
    "highfalutin"."total_births" as "total_births",
    "cheerful"."name_rank" as "name_rank"
FROM
    "highfalutin"
    INNER JOIN "cheerful" on "highfalutin"."names_name" = "cheerful"."names_name"
ORDER BY 
    "cheerful"."name_rank" asc""".strip()
    )


def test_aggregate_filter_anonymous():
    query = """
    import names;
    where abs(sum(births? gender = 'M') by name - sum(births? gender = 'F') by name) < (.05*sum(births) by name)
    select
    state,
    percent_of_total
    ;
    """
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]

    pattern = r"""[a-z]+ as \(
SELECT
    (["a-z]+)\."name" as "name",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."births"\) as "_virt_agg_sum_\d+"
FROM
    \1
GROUP BY 
    \1\."name"\)"""
    assert re.search(pattern, sql, re.DOTALL) is not None


def test_aggregate_filter():
    query = """
    import names;
    where abs(group(male_births) by name - group(female_births) by name) < (.05*sum(births) by name)
    select
    state,
    percent_of_total
    ;
    """
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]
    pattern = r"""[a-z]+ as \(
SELECT
    (["a-z]+)\."name" as "name",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."births"\) as "_virt_agg_sum_\d+"
FROM
    \1
GROUP BY 
    \1\."name"\)"""
    assert re.search(pattern, sql, re.DOTALL) is not None


def test_aggregate_filter_short_syntax():
    query = """
    import names;
    where abs(group male_births by name - group female_births by name) < (.05*sum(births) by name)
    select
    state,
    percent_of_total
    ;
    """
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]
    pattern = r"""[a-z]+ as \(
SELECT
    (["a-z]+)\."name" as "name",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."_virt_filter_births_\d+"\) as "_virt_agg_sum_\d+",
    sum\(\1\."births"\) as "_virt_agg_sum_\d+"
FROM
    \1
GROUP BY 
    \1\."name"\)"""
    assert re.search(pattern, sql, re.DOTALL) is not None


def test_group_by_with_existing():
    query = """
    import names;
    select
    year,
    name,
    total_births,
    rank name by group total_births by name desc as name_rank
having 
    name_rank <6
order by
    year asc
;
    """
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]

    assert (
        """FROM
    "bigquery-public-data"."usa_names"."usa_1910_current" as "usa_names"
GROUP BY 
    "usa_names"."name"),"""
        in sql
    ), sql


def test_multi_window():
    query = """
    import names;
    where year between 1940 and 1950
    select
        name,
        state,
        sum(births) as all_births,
        rank name over state by all_births desc as state_rank,
        rank name by sum(births) by name desc as all_rank
    having 
        state_rank != all_rank
        and all_rank <10
    order by
        all_rank asc;
        """
    env = Environment(working_path=Path(__file__).parent)

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]
    #     assert (
    #         """cooperative as (
    # SELECT
    #     thoughtful."_virt_agg_sum_6723478476084862" as "_virt_agg_sum_6723478476084862",
    #     thoughtful."name" as "name"
    #     rank() over (order by thoughtful."_virt_agg_sum_6723478476084862" desc ) as "all_rank",
    # FROM
    #     thoughtful)"""
    #         in sql
    #     ), sql
    pattern = r"""[a-z]+ as \(
SELECT
    (["a-z]+)\."_virt_agg_sum_\d+" as "_virt_agg_sum_\d+",
    \1\."name" as "name",
    rank\(\) over \(order by \1\."_virt_agg_sum_\d+" desc \) as "all_rank"
FROM
    \1\)"""

    assert re.search(pattern, sql, re.DOTALL) is not None, sql


def test_row_number_proper_join():
    query = """
import names as names;

where names.year between 1980 and 1989
and names.state in ('AL','AR','FL','GA','KY','LA','MS','NC','OK','SC','TN','TX','VA','WV')
select
    names.name,
    names.state,
    sum(names.births) as total_births,
    rank names.name over names.state by sum(names.births) desc as rank_by_births,
    case when rank_by_births <= 5 then 'common' else 'unique' end as name_rarity
order by names.state asc, total_births desc;

    """
    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(query)[0]
    assert env.concepts["rank_by_births"].keys == set(
        ["names.state", "names.name"]
    ), env.concepts["rank_by_births"].keys

    pattern = r"""INNER JOIN "highfalutin" on "questionable"."names_name" = "highfalutin"."names_name" AND "questionable"."names_state" = "highfalutin"."names_state"""
    assert re.search(pattern, sql, re.DOTALL) is not None


def test_inline_filter_or():
    query = """
import names;

select
    year,
    sum(births ? name = 'Luke' or name = 'Leia' or name = 'Anakin') as star_wars_births,
    sum(births ? not (name = 'Luke' or name = 'Leia' or name = 'Anakin')) as other_births
order by
    year asc;
"""

    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    exec.generate_sql(query)[0]


def test_filter_with_expression():
    query = """
import names;

select
    year,
    sum(births ? name) as non_null_names
order by
    year asc;
"""

    env = Environment(working_path=Path(__file__).parent)
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    exec.generate_sql(query)[0]
