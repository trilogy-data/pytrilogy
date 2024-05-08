from preql.core.models import Select, WindowItem
from preql.core.enums import PurposeLineage, Granularity, Purpose
from preql.core.processing.concept_strategies_v3 import search_concepts, generate_graph
from preql.core.query_processor import process_query, get_query_datasources
from preql.dialect.bigquery import BigqueryDialect
from preql.dialect import duckdb
from preql.parser import parse
from preql import Dialects


def test_select() -> None:
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_rank <- rank user_id by post_count desc;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
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
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    assert isinstance(env.concepts["user_rank"].lineage, WindowItem)

    ds = search_concepts(
        [env.concepts["post_count"], env.concepts["user_id"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
    ).resolve()
    # ds.validate()
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))

    get_query_datasources(environment=env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=env)
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

property user_country_rank <- rank user_id over country;

property rank_derived <- 100 + row_number user_id over country;

datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: country,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
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
    select: Select = parsed[-1]

    assert isinstance(env.concepts["user_country_rank"].lineage, WindowItem)

    get_query_datasources(environment=env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=env)
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
    env, parsed = parse(declarations)
    select: Select = parsed[-1]
    x = env.concepts["x"]
    assert x.granularity == Granularity.MULTI_ROW

    z = env.concepts["z"]
    assert z.purpose == Purpose.PROPERTY

    ds = search_concepts(
        [z.with_grain(x), x], environment=env, g=generate_graph(env), depth=0
    ).resolve()

    assert x in ds.output_concepts
    assert z in ds.output_concepts

    assert x.derivation == PurposeLineage.UNNEST
    assert z.derivation != PurposeLineage.CONSTANT

    generator = duckdb.DuckDBDialect()
    query = process_query(statement=select, environment=env)
    compiled = generator.compile_statement(query)
    assert "unnest" in compiled
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    results = exec.execute_text(declarations)
    select = results[-1]
    assert [row.x for row in select] == [1, 2, 2, 3]
