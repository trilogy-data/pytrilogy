from trilogy import Dialects
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.models.build import BuildWindowItem
from trilogy.core.processing.concept_strategies_v3 import (
    generate_graph,
    search_concepts,
    History,
)
from trilogy.core.processing.utility import concept_to_relevant_joins
from trilogy.core.query_processor import get_query_datasources, process_query
from trilogy.core.statements.author import SelectStatement, WindowItem
from trilogy.dialect import duckdb
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.parser import parse


def test_select() -> None:
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_id.user_rank <- rank user_id by post_count desc;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;


select
    user_id,
    user_rank,
    post_count
where 
    user_rank<10
limit 100
;


    """
    orig_env, parsed = parse(declarations)
    history = History(base_environment=orig_env)
    env = orig_env.materialize_for_select()
    select: SelectStatement = parsed[-1]

    assert isinstance(env.concepts["user_rank"].lineage, BuildWindowItem)

    ds = search_concepts(
        [env.concepts["post_count"], env.concepts["user_id"]],
        history=history,
        environment=env,
        g=generate_graph(env),
        depth=0,
    ).resolve()
    # ds.validate()
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))

    get_query_datasources(environment=orig_env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=orig_env)
    query.ctes[0]

    generator = BigqueryDialect()
    generator.compile_statement(query)


def test_rank_by():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.country string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_id.user_country_rank <- rank user_id over country;

property user_id.rank_derived <- 100 + row_number user_id over country;

datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: country,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;


select
    user_id,
    user_country_rank,
    rank_derived,
    post_count
where 
    user_country_rank<10
limit 100
;


    """
    env, parsed = parse(declarations)
    orig_env = env
    history = History(base_environment=orig_env)
    env = env.materialize_for_select()
    select: SelectStatement = parsed[-1]

    assert env.concepts["rank_derived"].keys == {
        env.concepts["user_id"].address,
    }
    assert concept_to_relevant_joins(
        [env.concepts[x] for x in ["user_id", "rank_derived"]]
    ) == [env.concepts["user_id"]]
    assert isinstance(orig_env.concepts["user_country_rank"].lineage, WindowItem)

    get_query_datasources(environment=orig_env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=orig_env)
    query.ctes[0]

    generator = BigqueryDialect()
    compiled = generator.compile_statement(query)
    assert "rank() over (partition" in compiled


def test_const_by():

    declarations = """
const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

select x, z 
order by x asc;"""
    org_env, parsed = parse(declarations)
    history = History(base_environment=org_env)
    env = org_env.materialize_for_select()
    select: SelectStatement = parsed[-1]
    x = env.concepts["x"]
    assert x.granularity == Granularity.MULTI_ROW

    z = env.concepts["z"]
    assert z.purpose == Purpose.PROPERTY
    assert set([x.address for x in z.lineage.concept_arguments]) == set(
        [
            x.address,
        ]
    )
    assert z.keys == {
        x.address,
    }

    ds = search_concepts(
        [z.with_grain(x), x],
        history=history,
        environment=env,
        g=generate_graph(env),
        depth=0,
    ).resolve()

    assert x.address in ds.output_concepts
    assert z.address in ds.output_concepts

    assert x.derivation == Derivation.UNNEST
    assert z.derivation != Derivation.CONSTANT

    generator = duckdb.DuckDBDialect()
    query = process_query(statement=select, environment=org_env)
    compiled = generator.compile_statement(query)
    assert "unnest" in compiled
    exec = Dialects.DUCK_DB.default_executor(environment=org_env)
    results = exec.execute_text(declarations)
    select = results[-1]
    assert [row.x for row in select] == [1, 2, 2, 3]


def test_inline_window():
    declarations = """
const x <- unnest([1,2,3,4]);


select 
    x, 
    row_number x order by x asc -> z,
order by x desc;"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.execute_text(declarations)
    select = results[-1].fetchall()
    assert [row.x for row in select] == [4, 3, 2, 1]
    assert [row.z for row in select] == [4, 3, 2, 1]
