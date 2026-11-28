"""Outer joins the planner narrows itself that only the optimizer's join
upgrade narrowed before (handoff 2.1 step 4): read off the planned
`QueryDatasource`, before any optimizer phase runs."""

from decimal import Decimal

from tests.core.processing.test_keyed_class_beside_trees import MODEL as TREES_MODEL
from tests.core.processing.test_keyed_class_beside_trees import _statement
from tests.core.processing.test_v4_root_partition import _MODEL as ROOT_MODEL
from tests.core.processing.test_v4_root_partition import _REV, _REV_KEYS
from tests.discovery.test_oracle_sweep_laws import BASE as SWEEP_MODEL
from tests.engine.test_duckdb_partial_key_assembly import _FORKED as FORKED_MODEL
from tests.engine.test_grain_pin_unnest_merge_filter import MERGED_SPECIES
from tests.engine.test_grain_pin_unnest_merge_filter import MODEL as PIN_MODEL
from tests.engine.test_unmodelled_regions import (
    TWO_FACT_JOIN_DERIVED,
    TWO_FACT_JOIN_MATERIALIZED,
)
from tests.optimization.test_multihop_not_null_preserved import NULLABLE_FK_MODEL
from tests.optimization.test_null_rejection_laws import SPLIT
from trilogy import Dialects, Environment
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join, QueryDatasource
from trilogy.core.query_processor import get_query_datasources


def _join_types(ds: QueryDatasource) -> list[tuple[str, JoinType]]:
    out = [
        (join.right.identifier, join.join_type)
        for join in ds.joins
        if isinstance(join, Join)
    ]
    for source in ds.datasources:
        if isinstance(source, QueryDatasource):
            out += _join_types(source)
    return out


def _env(model: str) -> Environment:
    env = Environment()
    env.parse(model)
    return env


def _joins(model: str, query: str) -> list[tuple[str, JoinType]]:
    env = _env(model)
    _, statements = env.parse(query)
    return _join_types(get_query_datasources(env, statements[-1]))


def _planned(model: str, query: str) -> set[JoinType]:
    return {jt for _, jt in _joins(model, query)}


def test_a_later_inner_narrows_a_full_to_right_after_the_merge_orders_sources():
    planned = _planned(
        NULLABLE_FK_MODEL,
        "select state, count(sale_id) as n where address_sk is not null;",
    )
    assert JoinType.FULL not in planned
    assert JoinType.RIGHT_OUTER in planned
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(NULLABLE_FK_MODEL))
        .execute_query(
            "select state, count(sale_id) as n where address_sk is not null"
            " order by state;"
        )
        .fetchall()
    )
    assert rows == [("CA", 1), ("NY", 1)]


def test_a_where_on_a_partial_column_one_side_alone_holds_narrows_it():
    assert _planned(SPLIT, "where ret_plus < 2 select order_id, item_id;") == {
        JoinType.INNER
    }


def test_a_flag_proof_reaches_through_the_merge_passing_the_flag():
    for model in (TWO_FACT_JOIN_DERIVED, TWO_FACT_JOIN_MATERIALIZED):
        planned = _joins(
            model, "select reason_class, count(ticket) as n where is_returned;"
        )
        sales_returns = [
            jt for right, jt in planned if right.startswith("returns_at_local_item_sk")
        ]
        assert sales_returns and set(sales_returns) == {JoinType.INNER}, planned


def test_a_where_read_off_one_of_a_relations_sibling_nodes_narrows_its_parent():
    query = _REV + " where r.rev > 4 and state = 'ca'" + _REV_KEYS
    users = [jt for right, jt in _joins(ROOT_MODEL, query) if right.startswith("users")]
    assert users and JoinType.FULL not in users, users
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(ROOT_MODEL))
        .execute_query(query)
        .fetchall()
    )
    assert [tuple(r) for r in rows] == [
        (1, 1, "ca", "acme", Decimal("5.0")),
        (1, 2, "ca", "zed", Decimal("7.0")),
    ]


def test_a_merged_key_is_not_null_proves_each_side_complete_on_it():
    planned = _joins(TREES_MODEL, _statement(["predicted"], "having"))
    trees = [jt for right, jt in planned if right.startswith("(enrichment")]
    assert trees and JoinType.FULL not in trees, planned


def test_a_pinned_value_proves_its_anchor():
    planned = _planned(
        PIN_MODEL + MERGED_SPECIES,
        "select upper(species) as s, tree_id where bucket = 'Same' order by s;",
    )
    assert JoinType.FULL not in planned, planned


def test_twin_aggregates_of_one_stream_through_an_equating_join():
    planned = _planned(
        SWEEP_MODEL,
        "select city, tree_form, sum(max(height) by city, species) as v0, "
        "sum(height ? (city = 'A' and city in ('B'))) as v1, "
        "min(height ? (sum(height) by city - max(height) by tree_form < 0 "
        "and sum(height) by city = count_distinct(tag) by species)) as v2 "
        "where not (height is not null) having tree_form = 'linden';",
    )
    assert JoinType.FULL not in planned, planned


def test_twin_aggregates_of_a_key_their_shared_stream_computes():
    query = (
        "select city, species ? max(height) by city > 5 as v0, "
        "sum(height) by v0 as v2, count(tree_id) as v3"
    )
    assert _planned(SWEEP_MODEL, query + ";") == {JoinType.INNER}
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by city, v0;")
        .fetchall()
    )
    assert rows == [("A", None, 8, 4), ("B", "Acer", 7, 1), ("B", "Tilia", 2, 1)]


def test_a_join_onto_lefts_an_inner_equated_reads_its_lead_left():
    query = (
        "select city, tag, species ? max(height) by city > 5 as v0, "
        "sum(height) by city, v0 as a, sum(height) by v0, tag as b"
    )
    assert _planned(SWEEP_MODEL, query + ";") == {JoinType.INNER}
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by city, tag;")
        .fetchall()
    )
    assert rows == [
        ("A", "x", None, 8, 8),
        ("A", "y", None, 8, None),
        ("B", "x", "Tilia", 2, 2),
        ("B", "y", "Acer", 7, 7),
    ]


def test_a_split_key_gate_pairs_inner_with_the_totals_it_gates():
    query = (
        "select city, species, count(tree_id) by species as v0, "
        "count(tree_id) by city as v1 where max(height) by city, species < 7"
    )
    assert _planned(SWEEP_MODEL, query + ";") == {JoinType.INNER}
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by city, species;")
        .fetchall()
    )
    assert rows == [("A", "Acer", 2, 3), ("A", "Quercus", 1, 3), ("B", "Tilia", 1, 1)]


def test_a_split_key_gate_an_inner_equated_with_the_rows_witnesses_the_key():
    query = (
        "select city, tree_form, count(tree_id) by tree_form as v0, "
        "count(tree_id) by city as v1 where min(height) by city, species != 7"
    )
    city_totals = [
        jt
        for ident, jt in _joins(SWEEP_MODEL, query + ";")
        if ident.endswith("grouped_by_local.city_at_local_city")
    ]
    assert city_totals == [JoinType.INNER]
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by city, tree_form;")
        .fetchall()
    )
    assert rows == [("A", "broadleaf", 3, 3), ("B", "linden", 1, 1)]


def test_twins_of_one_stream_each_beside_the_total_over_it():
    query = (
        "select species, max(height) by tree_form as a, count(tree_id) by tree_form as b "
        "where count_distinct(tag) by * > 1"
    )
    assert _planned(SWEEP_MODEL, query + ";") == {JoinType.INNER}
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by species;")
        .fetchall()
    )
    assert rows == [
        ("Acer", 7, 4),
        ("Quercus", 7, 4),
        ("Tilia", 2, 1),
        ("Ulmus", None, 1),
    ]


def test_twins_of_one_stream_beside_the_set_its_filter_reads():
    query = (
        "select city, tree_form, count(tree_id) as v0, max(height) as v1, "
        "max(min(height) by city, species) / (count(tree_id) - 1) as v2 "
        "where not (city in (city ? min(height) by city != 3))"
    )
    assert _planned(SWEEP_MODEL, query + ";") == {JoinType.INNER}
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(SWEEP_MODEL))
        .execute_query(query + " order by tree_form nulls last;")
        .fetchall()
    )
    assert rows == [("A", "broadleaf", 3, 5, 2.5), ("A", None, 1, None, None)]


def test_a_side_covered_on_the_rights_grain_pairs_inner_past_a_key_it_prunes():
    """Items pair orders on `(order_id, user_id)`; `user_id` is `~` on both
    and covers neither way, but the ON keeps only the orders grain."""
    query = "select order_id, product_id, total_pair_cost"
    orders = [
        jt
        for right, jt in _joins(FORKED_MODEL, query + ";")
        if right.startswith("orders")
    ]
    assert orders == [JoinType.INNER]
    rows = (
        Dialects.DUCK_DB.default_executor(environment=_env(FORKED_MODEL))
        .execute_query(query + " order by order_id nulls last, product_id;")
        .fetchall()
    )
    assert rows == [
        (100, 10, 100),
        (100, 20, 150),
        (101, 10, 120),
        (102, 20, 210),
        (None, 30, None),
    ]
