import pytest

from trilogy import Dialects
from trilogy.core.exceptions import InvalidComparison, ModelValidationError
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.models.core import EnumType
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.validation.environment import validate_environment

PREQL = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 10 as sales
''';

datasource ds_b (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 20 as sales
''';
"""


def test_enum_type_parsed():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    concept = executor.environment.concepts["local.category"]
    assert isinstance(concept.datatype, EnumType)
    assert set(concept.datatype.values) == {"A", "B"}


def test_enum_comparison_invalid():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    with pytest.raises(Exception):
        executor.execute_text("select sales where category = 'C';")


NULLABLE_ENUM_PREQL = """
key warehouse_sk int;
property warehouse_sk.sq_ft enum<bigint>[138504, 294242, 621234, 977787]?;

datasource warehouse (
    warehouse_sk: warehouse_sk,
    sq_ft: sq_ft,
)
grain (warehouse_sk)
query '''
select 1 as warehouse_sk, 138504 as sq_ft
''';
"""

NON_NULLABLE_ENUM_PREQL = """
key warehouse_sk int;
property warehouse_sk.sq_ft enum<bigint>[138504, 294242, 621234, 977787];

datasource warehouse (
    warehouse_sk: warehouse_sk,
    sq_ft: sq_ft,
)
grain (warehouse_sk)
query '''
select 1 as warehouse_sk, 138504 as sq_ft
''';
"""


@pytest.mark.parametrize(
    "predicate",
    [
        "sq_ft > 977787",  # nothing exceeds the max
        "sq_ft < 138504",  # nothing below the min
        "sq_ft = 0",  # not a domain member
        "sq_ft between 100 and 500",  # range excludes the whole domain
        "sq_ft in (0, 1)",  # no member listed
    ],
)
def test_enum_unsatisfiable_comparison(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(NULLABLE_ENUM_PREQL)
    with pytest.raises(InvalidComparison) as exc:
        executor.execute_text(f"select warehouse_sk where {predicate};")
    message = str(exc.value)
    assert "can never match" in message and "always false" in message
    assert predicate in message  # the rendered comparison, incl. operator


@pytest.mark.parametrize(
    "predicate",
    [
        "sq_ft > 0",  # every member exceeds 0
        "sq_ft >= 138504",  # every member >= min
        "sq_ft != 0",  # every member differs from a non-member
        "sq_ft between 0 and 9999999",  # range covers the whole domain
        "sq_ft not in (0, 1)",  # excludes nothing in the domain
    ],
)
def test_nullable_enum_match_all_suggests_is_not_null(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(NULLABLE_ENUM_PREQL)
    with pytest.raises(InvalidComparison) as exc:
        executor.execute_text(f"select warehouse_sk where {predicate};")
    message = str(exc.value)
    assert "only excludes nulls" in message
    assert "sq_ft is not null" in message
    assert predicate in message  # the rendered comparison, incl. operator


@pytest.mark.parametrize(
    "predicate",
    [
        "sq_ft > 0",  # every member exceeds 0
        "sq_ft != 0",  # every member differs from a non-member
        "sq_ft between 0 and 9999999",  # range covers the whole domain
    ],
)
def test_non_nullable_enum_match_all_is_tautology(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(NON_NULLABLE_ENUM_PREQL)
    with pytest.raises(InvalidComparison) as exc:
        executor.execute_text(f"select warehouse_sk where {predicate};")
    message = str(exc.value)
    assert "matches every value" in message and "always true" in message
    assert predicate in message  # the rendered comparison, incl. operator


@pytest.mark.parametrize(
    "predicate",
    [
        "sq_ft = 138504",  # a real member
        "sq_ft > 138504",  # some members exceed it, some don't
        "sq_ft between 100 and 300000",  # only the two smallest members
        "sq_ft in (138504, 999)",  # one member listed
        "sq_ft not in (138504, 999)",  # excludes one member
    ],
)
def test_enum_discriminating_comparison_allowed(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(NULLABLE_ENUM_PREQL)
    executor.generate_sql(f"select warehouse_sk where {predicate};")


STRING_ENUM_PREQL = """
key category enum<string>['RED', 'BLUE', 'GREEN'];
property category.qty int;

datasource d (
    category: category,
    qty: qty,
)
grain (category)
query '''
select 'RED' as category, 1 as qty
''';
"""


@pytest.mark.parametrize(
    "predicate",
    [
        "category like '%'",  # matches every member
        "category like 'Z%'",  # matches no member
        "category not like '%'",  # excludes every member
        "category ilike 'z%'",  # case-insensitive, still no member
        "like(category, 'Q%')",  # function form, no member
    ],
)
def test_string_enum_like_constant_raises(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(STRING_ENUM_PREQL)
    with pytest.raises(InvalidComparison):
        executor.generate_sql(f"select category where {predicate};")


@pytest.mark.parametrize(
    "predicate",
    [
        "category like 'R%'",  # only RED
        "category like '____'",  # only BLUE (4 chars)
        "category ilike 'red'",  # only RED, case-insensitive
        "category not like 'R%'",  # excludes only RED
    ],
)
def test_string_enum_like_discriminating_allowed(predicate):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(STRING_ENUM_PREQL)
    executor.generate_sql(f"select category where {predicate};")


def test_enum_union_injection():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select sum(sales) as total_sales;")[-1].fetchall()
    assert len(results) == 1
    assert results[0].total_sales == 30


def test_enum_union_by_category():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select category, sales order by category asc;")[
        -1
    ].fetchall()
    assert len(results) == 2
    assert results[0].category == "A"
    assert results[0].sales == 10
    assert results[1].category == "B"
    assert results[1].sales == 20


# 4 sources: 2 script queries + 2 materialized tables, each pair covering both enum values.
# The materialized pair should be preferred; the union must be exactly 2 sources.
PREQL_OVERLAPPING = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a_query (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 10 as sales
''';

datasource ds_b_query (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 20 as sales
''';

datasource ds_a_table (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
address category_a;

datasource ds_b_table (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
address category_b;
"""


def test_overlapping_sources_picks_pair():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE category_a AS SELECT 'A' as category, 10 as sales"
    )
    executor.execute_raw_sql(
        "CREATE TABLE category_b AS SELECT 'B' as category, 20 as sales"
    )
    executor.execute_text(PREQL_OVERLAPPING)

    env = executor.environment.materialize_for_select()
    unions = get_union_sources(
        list(env.datasources.values()), [env.concepts["category"]]
    )

    assert len(unions) == 1, f"Expected 1 union group, got {len(unions)}"
    assert len(unions[0]) == 2, f"Expected pair of 2, got {len(unions[0])}"
    ds_names = {ds.name for ds in unions[0]}
    assert ds_names == {"ds_a_table", "ds_b_table"}


def test_overlapping_sources_no_duplicate_results():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE category_a AS SELECT 'A' as category, 10 as sales"
    )
    executor.execute_raw_sql(
        "CREATE TABLE category_b AS SELECT 'B' as category, 20 as sales"
    )
    executor.execute_text(PREQL_OVERLAPPING)

    results = executor.execute_text("select sum(sales) as total_sales;")[-1].fetchall()
    assert len(results) == 1
    # Would be 60 if all 4 sources were incorrectly unioned
    assert results[0].total_sales == 30


# Two sources covering both enum values but with no overlapping data fields.
# They share only the partition key, so the union should only expose that key.
# Sources with zero concept overlap should not be combinable at all.
PREQL_NO_OVERLAP = """
key category enum<string>['A', 'B'];
property <category>.field_a int;
property <category>.field_b int;

datasource ds_a_only (
    ~category,
    field_a
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 1 as field_a
''';

datasource ds_b_only (
    ~category,
    field_b
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 2 as field_b
''';
"""


PREQL_CAST = """
key sun_exposure string;
auto sun_exposure_label <- CASE sun_exposure
  when 'full_sun' then 'Full sun'
  when 'partial_shade' then 'Partial shade'
  when 'shade' then 'Shade'
  end::enum<string>['Full sun', 'Partial shade', 'Shade'];

datasource plants (
  sun_exposure: sun_exposure
)
grain (sun_exposure)
query '''
select 'full_sun' as sun_exposure
''';
"""


def test_enum_cast():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_CAST)
    concept = executor.environment.concepts["local.sun_exposure_label"]
    assert isinstance(concept.datatype, EnumType)
    assert set(concept.datatype.values) == {"Full sun", "Partial shade", "Shade"}
    results = executor.execute_text("select sun_exposure_label;")[-1].fetchall()
    assert len(results) == 1
    assert results[0].sun_exposure_label == "Full sun"


PREQL_ENUM_VALID = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource valid_ds (
    ~category,
    sales
)
grain (category)
query '''
select 'A' as category, 10 as sales
UNION ALL
select 'B' as category, 20 as sales
''';
"""

PREQL_ENUM_INVALID = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource invalid_ds (
    ~category,
    sales
)
grain (category)
query '''
select 'C' as category, 99 as sales
''';
"""


def test_enum_validate_valid_values():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_ENUM_VALID)
    validate_environment(executor.environment, exec=executor)


def test_enum_validate_invalid_values():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_ENUM_INVALID)
    with pytest.raises(ModelValidationError):
        validate_environment(executor.environment, exec=executor)


def test_no_shared_data_fields_not_combined():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_NO_OVERLAP)

    env = executor.environment.materialize_for_select()
    unions = get_union_sources(
        list(env.datasources.values()), [env.concepts["category"]]
    )

    assert (
        len(unions) == 0
    ), f"Sources with no shared data fields should not be combined, got {unions}"


# Two derived aggregates pull different *subsets* of columns from the same enum
# partitioned arms (ds_a, ds_b, ds_c). Without union-CTE merging the planner
# emits two near-identical UNION CTEs; with it, both consumers share one.
PREQL_SHARED_UNION_ARMS = """
key category enum<string>['A', 'B', 'C'];
key item_id int;
key date_id int;
property <category, item_id>.x int;
property <category, item_id>.y int;
property <category, item_id>.flag int;
property date_id.year int;

datasource date_dim (
    date_id,
    year,
)
grain (date_id)
query '''
select 1 as date_id, 2024 as year
union all
select 2 as date_id, 2025 as year
''';

datasource ds_a (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'A'
query '''
select 'A' as category, 1 as item_id, 1 as date_id, 1 as x, 10 as y, 1 as flag
''';

datasource ds_b (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'B'
query '''
select 'B' as category, 1 as item_id, 1 as date_id, 2 as x, 20 as y, 1 as flag
''';

datasource ds_c (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'C'
query '''
select 'C' as category, 1 as item_id, 2 as date_id, 3 as x, 30 as y, 0 as flag
''';
"""


SHARED_UNION_ARMS_QUERY = """
auto x_in_year <- sum(x ? year = 2024 and flag = 1) by item_id;
auto y_overall <- sum(y ? flag = 1) by item_id;

with x_summary as
SELECT
    max(x_in_year) as max_x,
;

with y_summary as
SELECT
    max(y_overall) as max_y,
;

SELECT
    x_summary.max_x,
    y_summary.max_y,
;
"""


def test_shared_union_arms_collapse_to_single_union():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_SHARED_UNION_ARMS)
    sql = executor.generate_sql(SHARED_UNION_ARMS_QUERY)[-1]
    # 3 arms => 2 UNION ALL operators per union; one merged union => 2 total
    assert sql.count("UNION ALL") == 2, sql
    # each arm's underlying source select runs exactly once
    assert sql.count('as "ds_a"') == 1, sql
    assert sql.count('as "ds_b"') == 1, sql
    assert sql.count('as "ds_c"') == 1, sql


# An enum-partitioned fact where the FK that links a measure to its dimension is
# carried directly on the fact for some channels (A, B) but, for channel C, lives
# on a SIBLING table (`sale_c`) keyed by the same (oid, chan). Satisfying the C arm
# of the fact union therefore requires MERGING `ret_c` (the measure) with `sale_c`
# (the FK): its requested fields span two sources. A union projects only the
# columns common to every branch, so `dim_id` (absent from `ret_c` alone) gets
# dropped and the measure loses correlation to its dimension. Mirrors TPC-DS q05,
# where a web return's return-site comes from web_sales, not web_returns.
PREQL_UNION_ARM_SPANS_SOURCES = """
key oid int;
key chan enum<string>['A', 'B', 'C'];
key dim_id int;
property dim_id.dim_text string;
property <oid, chan>.amt float;
key k int;
property k.v float;

partial datasource ret_a (oid:oid, raw(''' 'A' '''):chan, aid:?dim_id, amt:amt)
grain (oid, chan) complete where chan = 'A'
query ''' select 1 as oid, 100 as aid, 11.0 as amt union all select 2 as oid, 200 as aid, 20.0 as amt ''';

partial datasource ret_b (oid:oid, raw(''' 'B' '''):chan, bid:?dim_id, amt:amt)
grain (oid, chan) complete where chan = 'B'
query ''' select 3 as oid, 200 as bid, 30.0 as amt ''';

partial datasource ret_c (oid:oid, raw(''' 'C' '''):chan, amt:amt)
grain (oid, chan) complete where chan = 'C'
query ''' select 4 as oid, 40.0 as amt union all select 5 as oid, 55.0 as amt ''';

partial datasource sale_c (oid:oid, raw(''' 'C' '''):chan, cid:?dim_id)
grain (oid, chan) complete where chan = 'C'
query ''' select 4 as oid, 100 as cid union all select 5 as oid, 300 as cid ''';

partial datasource dim_a (raw(''' 'A' '''):chan, did:dim_id, dtext:dim_text)
grain (dim_id, chan) complete where chan = 'A'
query ''' select 100 as did, 'p100' as dtext union all select 200 as did, 'p200' as dtext ''';

partial datasource dim_b (raw(''' 'B' '''):chan, did:dim_id, dtext:dim_text)
grain (dim_id, chan) complete where chan = 'B'
query ''' select 100 as did, 'p100' as dtext union all select 200 as did, 'p200' as dtext ''';

partial datasource dim_c (raw(''' 'C' '''):chan, did:dim_id, dtext:dim_text)
grain (dim_id, chan) complete where chan = 'C'
query ''' select 100 as did, 'p100' as dtext union all select 300 as did, 'p300' as dtext ''';

datasource other (k:k, v:v) grain (k) query ''' select 1 as k, 7.0 as v ''';
"""

# p100: A/oid1 (11) + C/oid4 (40) = 51; p200: A/oid2 (20) + B/oid3 (30) = 50;
# p300: C/oid5 (55). C's measure must correlate to its FK via sale_c.
_EXPECTED_RETURNS = [("ep100", 51.0), ("ep200", 50.0), ("ep300", 55.0)]


def test_enum_union_arm_spanning_multiple_sources_row_grain():
    # The core defect, with no union/aggregate to obscure it: a plain row-grain
    # select of (entity-via-partial-FK, measure). The measure union drops dim_id
    # (ret_c lacks it) and joins the dimension on the enum key `chan` alone, so a
    # measure pairs with EVERY entity sharing its channel — inventing (ent, amt)
    # pairs that no row carries. The five real pairs (one per oid) are the only
    # correct rows.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_UNION_ARM_SPANS_SOURCES)
    results = executor.execute_text("""
where dim_id is not null
select concat('e', dim_text) as ent, amt as m order by ent, m;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [
        ("ep100", 11.0),
        ("ep100", 40.0),
        ("ep200", 20.0),
        ("ep200", 30.0),
        ("ep300", 55.0),
    ]


def test_enum_union_arm_spanning_multiple_sources_aggregated():
    # Control: with an explicit aggregate the measure is sourced at dim_id grain,
    # which pulls in the sale_c FK bridge, so this already resolves correctly.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_UNION_ARM_SPANS_SOURCES)
    results = executor.execute_text("""
where dim_id is not null
select concat('e', dim_text) as ent, sum(amt) as total order by ent;
""")[0].fetchall()
    assert [tuple(r) for r in results] == _EXPECTED_RETURNS


# Two rival enum unions over the same partition key: a "sales" family binding the
# grain keys complete, and a "returns" family binding them column-level partial
# (~order_id / ~item_id — returns only cover returned lines). The union heals the
# table-level partial stamp but must NOT heal explicit ~ columns: the returns
# union stays partial on the keys, so the sales union strictly wins for key-only
# requests. Before that propagation, both unions tied on partial_count and the
# winner was decided by lexical datasource-name order ("ret_*" < "sale_*") —
# unresolvable errors for plain keys, silently wrong tables for aggregates.
# Mirrors TPC-DS q14 over the consolidated all_sales model. `site_id` is shared
# by exactly one sales and one returns source so a mixed-family combo would
# survive the maximal-overlap filter — get_union_sources must reject it.
PREQL_PARTIAL_KEY_FAMILIES = """
key chan enum<string>['A', 'B'];
key order_id int;
key item_id int;

properties <order_id, chan, item_id> (
    quantity int?,
    return_amount float?,
    site_id int?,
);

partial datasource {ret_a} (
    raw(''' 'A' '''): chan,
    oid: ~order_id,
    iid: ~item_id,
    ramt: return_amount,
)
grain (order_id, chan, item_id)
complete where chan = 'A'
query '''
select 1 as oid, 10 as iid, 5.0 as ramt
''';

partial datasource {ret_b} (
    raw(''' 'B' '''): chan,
    oid: ~order_id,
    iid: ~item_id,
    ramt: return_amount,
    rsite: ?site_id,
)
grain (order_id, chan, item_id)
complete where chan = 'B'
query '''
select 2 as oid, 20 as iid, 6.0 as ramt, 200 as rsite
''';

partial datasource {sale_a} (
    raw(''' 'A' '''): chan,
    oid: order_id,
    iid: item_id,
    qty: quantity,
    ssite: ?site_id,
)
grain (order_id, chan, item_id)
complete where chan = 'A'
query '''
select 1 as oid, 10 as iid, 1 as qty, 100 as ssite
union all
select 3 as oid, 30 as iid, 2 as qty, 300 as ssite
''';

partial datasource {sale_b} (
    raw(''' 'B' '''): chan,
    oid: order_id,
    iid: item_id,
    qty: quantity,
)
grain (order_id, chan, item_id)
complete where chan = 'B'
query '''
select 2 as oid, 20 as iid, 3 as qty
union all
select 4 as oid, 40 as iid, 4 as qty
''';
"""

# returns sort before sales / after sales: the outcome must be identical.
_NAMES_RETURNS_FIRST = {
    "ret_a": "ret_a",
    "ret_b": "ret_b",
    "sale_a": "sale_a",
    "sale_b": "sale_b",
}
_NAMES_RETURNS_LAST = {
    "ret_a": "zz_ret_a",
    "ret_b": "zz_ret_b",
    "sale_a": "aa_sale_a",
    "sale_b": "aa_sale_b",
}

_ALL_ROWS = [("A", 1), ("B", 2), ("A", 3), ("B", 4)]


def _partial_key_executor(names: dict[str, str]):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_PARTIAL_KEY_FAMILIES.format(**names))
    return executor


@pytest.mark.parametrize("names", [_NAMES_RETURNS_FIRST, _NAMES_RETURNS_LAST])
@pytest.mark.parametrize(
    "query,expected",
    [
        ("select chan, order_id order by order_id asc;", _ALL_ROWS),
        (
            "select chan, item_id, order_id order by order_id asc;",
            [("A", 10, 1), ("B", 20, 2), ("A", 30, 3), ("B", 40, 4)],
        ),
        (
            "select chan, order_id, quantity order by order_id asc;",
            [("A", 1, 1), ("B", 2, 3), ("A", 3, 2), ("B", 4, 4)],
        ),
        ("select count(order_id) as c;", [(4,)]),
        ("select count(item_id) as c;", [(4,)]),
        ("select sum(return_amount) as total;", [(11.0,)]),
        (
            "select order_id, return_amount order by order_id asc;",
            [(1, 5.0), (2, 6.0), (3, None), (4, None)],
        ),
        (
            "where return_amount is not null select order_id, return_amount order by order_id asc;",
            [(1, 5.0), (2, 6.0)],
        ),
    ],
)
def test_partial_key_union_matrix(names, query, expected):
    executor = _partial_key_executor(names)
    results = executor.execute_text(query)[-1].fetchall()
    assert [tuple(r) for r in results] == expected, query


@pytest.mark.parametrize("names", [_NAMES_RETURNS_FIRST, _NAMES_RETURNS_LAST])
def test_partial_key_union_aggregate_reads_complete_family(names):
    # count over a grain key must come from the family binding it complete
    # (sales), never the ~-partial returns family.
    executor = _partial_key_executor(names)
    sql = executor.generate_sql("select count(order_id) as c;")[-1]
    ret_a, ret_b = names["ret_a"], names["ret_b"]
    assert ret_a not in sql and ret_b not in sql, sql


def test_union_partial_concepts_propagate_explicit_partials():
    executor = _partial_key_executor(_NAMES_RETURNS_FIRST)
    env = executor.environment.materialize_for_select()
    ds = {d.name: d for d in env.datasources.values()}
    returns_union = BuildUnionDatasource(children=[ds["ret_a"], ds["ret_b"]])
    assert {c.address for c in returns_union.partial_concepts} == {
        "local.order_id",
        "local.item_id",
    }
    sales_union = BuildUnionDatasource(children=[ds["sale_a"], ds["sale_b"]])
    assert sales_union.partial_concepts == []


def test_union_partial_concepts_discriminator_healed():
    # The enum discriminator itself (~category, partitioned by complete-where)
    # IS healed by the union — only non-discriminator ~ columns survive.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    env = executor.environment.materialize_for_select()
    ds = {d.name: d for d in env.datasources.values()}
    union = BuildUnionDatasource(children=[ds["ds_a"], ds["ds_b"]])
    assert union.partial_concepts == []


def test_union_sources_mixed_family_stays_partial():
    # sale_a and ret_b share site_id, so the mixed combo's overlap signature is
    # not a subset of either pure family's and it survives as a candidate — it
    # is the only union providing site_id across both channels (the q05 shape:
    # a web return's return-site lives on web_sales). What keeps it safe is
    # partial propagation: it stays partial on the ~ keys its returns child
    # binds, so it can never outrank the pure sales union for key requests.
    executor = _partial_key_executor(_NAMES_RETURNS_FIRST)
    env = executor.environment.materialize_for_select()
    unions = get_union_sources(
        list(env.datasources.values()),
        [env.concepts["chan"], env.concepts["order_id"], env.concepts["site_id"]],
    )
    families = {frozenset(ds.name for ds in group): group for group in unions}
    assert frozenset({"sale_a", "sale_b"}) in families
    assert frozenset({"ret_a", "ret_b"}) in families
    mixed = families.get(frozenset({"sale_a", "ret_b"}))
    assert mixed is not None
    mixed_union = BuildUnionDatasource(children=mixed)
    assert {c.address for c in mixed_union.partial_concepts} == {
        "local.order_id",
        "local.item_id",
    }


def test_enum_union_arm_spanning_multiple_sources_in_tvf():
    # The same split arm inside a multi-arm union(...) TVF: the arm is resolved at
    # row grain and does NOT carry the grain key (oid) in its output, so the
    # planner has no grain-key bridge to pull `sale_c` in for the C branch. The
    # measure union can then only join the dimension on the low-cardinality enum
    # key (chan), and C's measure fans out / broadcasts (ep300 picks up all of C,
    # ep100/ep200 inflate). The per-enum source search must instead recognize a
    # mergeable set (ret_c |x| sale_c) so every branch provides dim_id.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_UNION_ARM_SPANS_SOURCES)
    results = executor.execute_text("""
with combined as union(
  (select 'zzz' as ent, v as m where k = 1),
  (where dim_id is not null select concat('e', dim_text) as ent, amt as m)
) -> (entity, meas);
select combined.entity, sum(combined.meas) as total order by combined.entity;
""")[0].fetchall()
    assert [tuple(r) for r in results] == _EXPECTED_RETURNS + [("zzz", 7.0)]
