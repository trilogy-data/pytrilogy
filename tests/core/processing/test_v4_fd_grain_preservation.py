from decimal import Decimal

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import Factory, get_canonical_pseudonyms
from trilogy.core.processing.concept_strategies_v4 import (
    V4History,
)
from trilogy.core.processing.concept_strategies_v4 import (
    search_concepts as search_concepts_v4,
)
from trilogy.core.processing.nodes import History
from trilogy.parser import parse_text

_MODEL = """
key line_id int;
key date_id int;
property line_id.date_id int;
property line_id.amount numeric(10,2);
property date_id.week_seq int;
property date_id.day_of_week int;

datasource sales (
    lid: line_id,
    did: date_id,
    amt: amount,
)
grain (line_id)
query '''
select 1 lid, 10 did, 5.0 amt union all
select 2 lid, 11 did, 7.0 amt union all
select 3 lid, 12 did, 9.0 amt
''';

datasource dates (
    did: date_id,
    wk: week_seq,
    dow: day_of_week,
)
grain (date_id)
query '''
select 10 did, 100 wk, 0 dow union all
select 11 did, 100 wk, 1 dow union all
select 12 did, 101 wk, 0 dow
''';

auto sunday_sales <- sum(
    case
        when day_of_week = 0 then amount
        else 0.0::numeric(10,2)
    end
) by week_seq;
"""

_QUERY = """
select
    week_seq,
    sunday_sales,
order by week_seq asc;
"""

_SCOPED_JOIN_MODEL = """
key date_id int;
property date_id.week_seq int;
property date_id.day_of_week int;

key catalog_order int;
key catalog_item int;
property <catalog_order,catalog_item>.catalog_date_id int;
property <catalog_order,catalog_item>.catalog_amount numeric(10,2);

key web_order int;
key web_item int;
property <web_order,web_item>.web_date_id int;
property <web_order,web_item>.web_amount numeric(10,2);

datasource catalog_sales (
    co: catalog_order,
    ci: catalog_item,
    cd: catalog_date_id,
    ca: catalog_amount,
)
grain (catalog_order, catalog_item)
query '''
select 1 co, 10 ci, 100 cd, 5.0 ca
''';

datasource web_sales (
    wo: web_order,
    wi: web_item,
    wd: web_date_id,
    wa: web_amount,
)
grain (web_order, web_item)
query '''
select 2 wo, 20 wi, 100 wd, 7.0 wa
''';

datasource dates (
    did: date_id,
    wk: week_seq,
    dow: day_of_week,
)
grain (date_id)
query '''
select 100 did, 500 wk, 0 dow
''';
"""

_SCOPED_JOIN_QUERY = """
left join catalog_date_id = date_id
left join web_date_id = date_id
select
    week_seq,
    sum(
        case
            when day_of_week = 0 then catalog_amount
            else 0.0::numeric(10,2)
        end
    ) by week_seq -> cat_sales,
    sum(
        case
            when day_of_week = 0 then web_amount
            else 0.0::numeric(10,2)
        end
    ) by week_seq -> web_sales_out;
"""


def _engine():
    env = Environment()
    env, _ = env.parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def _scoped_join_build_info():
    env = Environment()
    env, _ = env.parse(_SCOPED_JOIN_MODEL)
    _, parsed = parse_text(_SCOPED_JOIN_QUERY, env)
    statement = parsed[-1]
    scoped_joins = [
        (join.source_address, join.target_address, join.join_type)
        for join in statement.join_clauses
    ]
    history = History(base_environment=env)
    history.build_caches.scoped_joins = scoped_joins
    history.build_caches.pseudonym_map = get_canonical_pseudonyms(env)
    factory = Factory(
        environment=env,
        build_cache=history.build_caches.build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        grain_build_cache=history.build_caches.grain_build_cache,
        pseudonym_map=history.build_caches.pseudonym_map,
        scoped_joins=scoped_joins,
    )
    built = factory.build(statement.as_lineage(env))
    build_env = env.materialize_for_select(
        built.local_concepts,
        build_cache=history.build_caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=factory.grain_build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        datasource_build_cache=history.build_caches.datasource_build_cache,
        scoped_joins=scoped_joins,
    )
    return search_concepts_v4(
        list(built.output_components),
        V4History(base_environment=env, build_caches=history.build_caches),
        build_env,
        0,
        generate_graph(build_env),
        conditions=[],
    )


def test_fd_dimension_on_row_grain_aggregate_rows_match_baseline():
    prior = CONFIG.use_v4_discovery
    try:
        CONFIG.use_v4_discovery = False
        assert _engine().execute_text(_QUERY)[-1].fetchall() == [
            (100, Decimal("5.00")),
            (101, Decimal("9.00")),
        ]
        CONFIG.use_v4_discovery = True
        assert _engine().execute_text(_QUERY)[-1].fetchall() == [
            (100, Decimal("5.00")),
            (101, Decimal("9.00")),
        ]
    finally:
        CONFIG.use_v4_discovery = prior


def test_fd_dimension_on_row_grain_does_not_force_input_normalization():
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        sql = _engine().generate_sql(_QUERY)[-1]
    finally:
        CONFIG.use_v4_discovery = prior

    assert '"line_id"' not in sql.lower()
    assert sql.lower().count("group by") == 1
    assert "sum(CASE" in sql


def test_scoped_join_alias_dimension_does_not_expand_aggregate_input_grain():
    info = _scoped_join_build_info()

    aggregate_input_grains = {
        primary: attrs.aggregate_input_grain
        for attrs in info.group_attrs.values()
        for primary in attrs.primary_members
        if attrs.aggregate_input_grain
    }

    assert aggregate_input_grains["local.cat_sales"] == frozenset(
        {"local.catalog_order", "local.catalog_item"}
    )
    assert aggregate_input_grains["local.web_sales_out"] == frozenset(
        {"local.web_order", "local.web_item"}
    )
