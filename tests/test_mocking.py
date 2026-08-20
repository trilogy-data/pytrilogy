from decimal import Decimal

import pytest
from click.exceptions import Exit

from trilogy import Dialects
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    MapType,
    NumericType,
    StructComponent,
    StructType,
    TraitDataType,
)
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect.mock import ARRAY_MOCK_SIZE, mock_datatype
from trilogy.scripts.common import validate_environment as cli_validate_environment


def test_mock_datatype_enum_int_random():
    enum = EnumType(type=DataType.INTEGER, values=[0, 1, 2, 3])
    rows = mock_datatype(enum, DataType.INTEGER, scale_factor=50, is_key=False)
    assert len(rows) == 50
    assert set(rows).issubset({0, 1, 2, 3})


def test_mock_datatype_enum_string_random():
    enum = EnumType(type=DataType.STRING, values=["A", "B", "C"])
    rows = mock_datatype(enum, DataType.STRING, scale_factor=20, is_key=False)
    assert len(rows) == 20
    assert set(rows).issubset({"A", "B", "C"})


def test_mock_datatype_enum_key_unique_capped():
    """A key never repeats: a domain smaller than scale_factor caps the row
    count rather than cycling into grain violations."""
    enum = EnumType(type=DataType.INTEGER, values=[0, 1, 2])
    rows = mock_datatype(enum, DataType.INTEGER, scale_factor=7, is_key=True)
    assert rows == [0, 1, 2]


def test_mock_datatype_trait_over_enum_stays_in_domain():
    enum = EnumType(type=DataType.STRING, values=["a@example.com", "b@example.com"])
    traited = TraitDataType(type=enum, traits=["email_address"])
    rows = mock_datatype(traited, traited, scale_factor=20)
    assert len(rows) == 20
    assert set(rows).issubset(set(enum.values))


def test_mock_datatype_bool_key_unique_capped():
    rows = mock_datatype(DataType.BOOL, DataType.BOOL, scale_factor=10, is_key=True)
    assert rows == [False, True]


def test_mock_datatype_enum_empty_raises():
    enum = EnumType(type=DataType.INTEGER, values=[])
    with pytest.raises(ValueError):
        mock_datatype(enum, DataType.INTEGER, scale_factor=5)


def test_mock_datatype_numeric_type_returns_decimals():
    numeric = NumericType(15, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=20)
    assert len(rows) == 20
    assert all(isinstance(r, Decimal) for r in rows)
    assert all(r == r.quantize(Decimal("0.01")) for r in rows)


def test_mock_datatype_numeric_type_key_unique():
    numeric = NumericType(15, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=50, is_key=True)
    assert len(set(rows)) == 50


def test_mock_datatype_numeric_trait_unwraps():
    traited = TraitDataType(type=NumericType(15, 2), traits=["usd"])
    rows = mock_datatype(traited, traited, scale_factor=10)
    assert all(isinstance(r, Decimal) for r in rows)


def test_mock_datatype_numeric_respects_precision_and_scale():
    numeric = NumericType(4, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=100)
    assert all(abs(r) < 100 for r in rows)
    assert all(r == r.quantize(Decimal("0.01")) for r in rows)


def test_mock_datatype_bare_numeric_still_supported():
    rows = mock_datatype(DataType.NUMERIC, DataType.NUMERIC, scale_factor=10)
    assert len(rows) == 10
    assert all(isinstance(r, Decimal) for r in rows)


def test_mock_datatype_bigint_and_number():
    assert mock_datatype(DataType.BIGINT, DataType.BIGINT, 5, is_key=True) == [
        1,
        2,
        3,
        4,
        5,
    ]
    rows = mock_datatype(DataType.NUMBER, DataType.NUMBER, scale_factor=5)
    assert all(isinstance(r, float) for r in rows)


def test_mock_datatype_bytes():
    rows = mock_datatype(DataType.BYTES, DataType.BYTES, scale_factor=5)
    assert all(isinstance(r, bytes) for r in rows)
    keys = mock_datatype(DataType.BYTES, DataType.BYTES, scale_factor=5, is_key=True)
    assert len(set(keys)) == 5


def test_mock_datatype_array_shape_matches_declared_element_type():
    arr = ArrayType(type=DataType.STRING)
    rows = mock_datatype(arr, arr, scale_factor=10)
    assert all(isinstance(r, list) and len(r) == ARRAY_MOCK_SIZE for r in rows)
    assert all(isinstance(v, str) for r in rows for v in r)


def test_mock_datatype_array_key_unique_and_homogeneous():
    arr = ArrayType(type=DataType.STRING)
    rows = mock_datatype(arr, arr, scale_factor=10, is_key=True)
    assert len({tuple(r) for r in rows}) == 10
    assert all(isinstance(v, str) for r in rows for v in r)


def test_mock_datatype_array_of_precision_numeric():
    arr = ArrayType(type=NumericType(15, 2))
    rows = mock_datatype(arr, arr, scale_factor=5)
    assert all(isinstance(v, Decimal) for r in rows for v in r)


def test_mock_datatype_map():
    mp = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)
    rows = mock_datatype(mp, mp, scale_factor=10)
    assert all(isinstance(r, dict) for r in rows)
    assert all(
        isinstance(k, str) and isinstance(v, int) for r in rows for k, v in r.items()
    )
    keyed = mock_datatype(mp, mp, scale_factor=10, is_key=True)
    assert len({tuple(sorted(r.items())) for r in keyed}) == 10


def test_mock_datatype_struct():
    struct = StructType(
        fields=[
            StructComponent(name="a", type=DataType.INTEGER),
            StructComponent(name="b", type=DataType.STRING),
        ],
        fields_map={"a": DataType.INTEGER, "b": DataType.STRING},
    )
    rows = mock_datatype(struct, struct, scale_factor=10, is_key=True)
    assert len({r["a"] for r in rows}) == 10
    assert all(isinstance(r["b"], str) for r in rows)


def test_mock_datatype_unsupported_remains_loud():
    with pytest.raises(NotImplementedError):
        mock_datatype(DataType.GEOGRAPHY, DataType.GEOGRAPHY, 5)


def test_mock_validate_passes_with_precision_numeric_column():
    """q89 regression: unit-mode validation must survive a model whose
    datasource has a numeric(p,s) column, referenced by nothing."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        type usd numeric;

        key id int;
        property id.ext_sales_price numeric(15,2)::usd;
        property id.quantity int;

        datasource store_sales (
            id: id,
            ext_sales_price: ext_sales_price,
            quantity: quantity,
        )
        grain (id)
        address store_sales_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_mock_validate_passes_with_composite_columns():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.big bigint;
        property id.blob bytes;
        property id.tags array<string>;
        property id.bare_list list;
        property id.counts map<string, int>;
        property id.nested struct<a: int, b: string>;

        datasource spicy (
            id: id,
            big: big,
            blob: blob,
            tags: tags,
            bare_list: bare_list,
            counts: counts,
            nested: nested,
        )
        grain (id)
        address spicy_tbl;

        mock datasource spicy;
    """)

    validate_environment(executor.environment, exec=executor)
    assert executor.execute_raw_sql(
        "select map_keys(counts) from spicy_tbl limit 1"
    ).fetchall()
    tags_type = {
        row[0]: row[1]
        for row in executor.execute_raw_sql("describe spicy_tbl").fetchall()
    }
    assert tags_type["tags"] == "VARCHAR[]"
    assert tags_type["counts"] == "MAP(VARCHAR, BIGINT)"


def test_mock_castable_values_for_datasource_cast_bindings():
    """A string column feeding a datasource-declared cast (`concept::type:
    other`) must mock as strings that survive the cast, or validation of the
    derived concept aborts the whole connection's transaction."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key sk int;
        property sk._date_string string;
        property sk.date_val date;

        datasource dates (
            D_DATE_SK: sk,
            D_DATE: _date_string,
            _date_string::date: date_val,
        )
        grain (sk)
        address date_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_mock_unsupported_column_error_names_concept():
    executor = Dialects.DUCK_DB.default_executor()
    with pytest.raises(NotImplementedError, match="local.geo"):
        executor.execute_text("""
            key id int;
            property id.geo geography;
            datasource t (id: id, geo: geo) grain (id) address t_tbl;
            mock datasource t;
        """)


def test_mock_validate_passes_with_enum_datasource():
    """Mocked enum-typed columns must produce values from the enum's allowed
    set so datasource type-binding validation succeeds."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.category enum<int>[0, 1, 2];
        property id.color enum<string>['RED', 'BLUE', 'GREEN'];

        datasource thing (
            id: id,
            category: category,
            color: color,
        )
        grain (id)
        address my_thing;

        mock datasource thing;
    """)

    validate_environment(executor.environment, exec=executor)


def test_mock_validate_passes_with_non_key_grain_component():
    """A datasource grained on a non-KEY concept (e.g. a property date) must
    still satisfy grain validation after mocking — grain components have to
    be unique-per-row even when their purpose isn't KEY."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.flight_date date;
        auto flight_count <- count(id);

        datasource flight (
            id: id,
            flight_date: flight_date,
        )
        grain (id)
        address flight_tbl;

        datasource flight_count_by_date (
            flight_date: flight_date,
            flight_count: flight_count,
        )
        grain (flight_date)
        address flight_count_by_date_tbl;

        mock datasource flight, flight_count_by_date;
    """)

    validate_environment(executor.environment, exec=executor)


def test_mock_validate_passes_with_enum_key_property():
    """An enum used as a property on a KEY concept should mock + validate
    cleanly through the unit (mock) path."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.status enum<string>['active', 'inactive', 'pending'];

        datasource records (
            id: id,
            status: status,
        )
        grain (id)
        address records_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_mock_validate_passes_with_bool_grain_component():
    """A datasource grained on a boolean must mock as at most one row per
    truth value, or grain validation can never pass."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.is_active bool;
        auto record_count <- count(id);

        datasource records (
            id: id,
            is_active: is_active,
        )
        grain (id)
        address records_tbl;

        datasource counts_by_active (
            is_active: is_active,
            record_count: record_count,
        )
        grain (is_active)
        address counts_by_active_tbl;

        mock datasource records, counts_by_active;
    """)

    validate_environment(executor.environment, exec=executor)


def test_mock_composite_grain_fills_combination_space():
    """A composite grain must not cap the table at its smallest component:
    (id, is_active) mocks the full scale_factor with the tuple unique, every
    id present, and both truth values present."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        key is_active bool;
        property <id, is_active>.amount float;

        datasource id_by_active (
            id: id,
            is_active: is_active,
            amount: amount,
        )
        grain (id, is_active)
        address id_by_active_tbl;

        mock datasource id_by_active;
    """)

    validate_environment(executor.environment, exec=executor)

    total, combos, ids, actives = executor.execute_raw_sql("""select count(*),
                  (select count(*) from (select distinct id, is_active from id_by_active_tbl)),
                  count(distinct id),
                  count(distinct is_active)
           from id_by_active_tbl""").fetchall()[0]
    assert total == 100
    assert combos == 100
    assert ids == 100
    assert actives == 2


def test_mock_validate_passes_with_enum_key_grain():
    """The ingest round-trip shape (users_with_pk): a key with a finite enum
    domain caps the mock table's rows so grain holds, and an enum property
    under a trait stays inside its declared domain."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        import std.net;

        key user_id enum<int>[1, 2, 3];
        property user_id.email enum<string>['a@example.com', 'b@example.com', 'c@example.com']::email_address;

        datasource users (
            user_id: user_id,
            email: email,
        )
        grain (user_id)
        address users_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)

    rows = executor.execute_raw_sql(
        "select count(*), count(distinct user_id) from users_tbl"
    ).fetchall()
    assert rows[0][0] == 3
    assert rows[0][1] == 3


def test_mock_partial_binding_leaves_extension_rows():
    """A `~` binding must cover a strict prefix of its key's domain: without
    unmatched members LEFT and INNER return the same rows and no unit-tier test
    can see a join-type regression on the bridge."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key user_id int;
        property user_id.name string;
        key line_id int;
        property line_id.amount float;

        datasource users (
            id: user_id,
            name: name,
        )
        grain (user_id)
        address users_tbl;

        datasource lines (
            id: line_id,
            user_id: ~user_id,
            amount: amount,
        )
        grain (line_id)
        address lines_tbl;

        mock datasources users, lines;
    """)

    never_ordered, matched = executor.execute_raw_sql(
        "select (select count(*) from users_tbl u "
        "  anti join lines_tbl l on l.user_id = u.id), "
        "       (select count(distinct user_id) from lines_tbl)"
    ).fetchall()[0]
    assert never_ordered > 0
    assert matched > 0


def test_mock_complete_binding_covers_every_key():
    """The same model without the `~` must cover the whole key domain, or the
    partial prefix would be indistinguishable noise."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key user_id int;
        key line_id int;

        datasource users (id: user_id) grain (user_id) address users_tbl;
        datasource lines (
            id: line_id,
            user_id: user_id,
        )
        grain (line_id)
        address lines_tbl;

        mock datasources users, lines;
    """)

    assert (
        executor.execute_raw_sql(
            "select count(*) from users_tbl u anti join lines_tbl l on l.user_id = u.id"
        ).fetchall()[0][0]
        == 0
    )


def test_mock_redundant_foreign_key_agrees_across_tables():
    """A column functionally determined by a key the same table binds is looked
    up, not cycled: a redundant FK that disagrees manufactures non-matches on
    every join built from both keys. Row counts here deliberately diverge (the
    order key's domain caps its table at three rows), which is exactly where
    index-cycling desynchronizes."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key order_id enum<int>[1, 2, 3];
        property order_id.user_id int;
        key line_id int;
        property line_id.amount float;

        datasource orders (
            order_id: order_id,
            user_id: user_id,
        )
        grain (order_id)
        address orders_tbl;

        datasource lines (
            id: line_id,
            order_id: order_id,
            user_id: user_id,
            amount: amount,
        )
        grain (line_id)
        address lines_tbl;

        mock datasources orders, lines;
    """)

    orders, lines, mismatched = executor.execute_raw_sql(
        "select (select count(*) from orders_tbl), "
        "       (select count(*) from lines_tbl), "
        "       (select count(*) from lines_tbl l join orders_tbl o "
        "          using (order_id) where l.user_id != o.user_id)"
    ).fetchall()[0]
    assert orders == 3
    assert lines > orders
    assert mismatched == 0


_PARTIAL_CHAIN = """
    key user_id int;
    key order_id int;
    key line_id int;
    property line_id.amount float;

    datasource users (id: user_id) grain (user_id) address users_tbl;

    datasource orders (
        id: order_id,
        user_id: {user_binding},
    )
    grain (order_id)
    address orders_tbl;

    datasource lines (
        id: line_id,
        order_id: order_id,
        user_id: ~user_id,
        amount: amount,
    )
    grain (line_id)
    address lines_tbl;

    mock datasources users, orders, lines;
"""


def test_mock_partial_foreign_key_inherits_a_partial_source():
    """A `~` column fixed by a key the same table binds must be looked up, not
    cycled — even though the source binds that key `~` too. Judging the source's
    coverage against the column's own partial prefix instead of the concept's
    full domain skips the lookup, and the redundant key then disagrees on
    almost every row while still looking partial."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_PARTIAL_CHAIN.format(user_binding="~user_id"))

    mismatched, never_ordered, covered, users = executor.execute_raw_sql(
        "select (select count(*) from lines_tbl l join orders_tbl o "
        "          on l.order_id = o.id where l.user_id != o.user_id), "
        "       (select count(*) from users_tbl u "
        "          anti join orders_tbl o on o.user_id = u.id), "
        "       (select count(distinct user_id) from lines_tbl), "
        "       (select count(*) from users_tbl)"
    ).fetchall()[0]
    assert mismatched == 0
    assert never_ordered > 0
    assert 0 < covered < users


def test_mock_partial_key_is_not_widened_by_a_complete_source():
    """The mirror: when the determinant reaches every member of the domain,
    inheriting it would hand the whole domain back and erase the `~`. The
    partial binding wins — a `~` column that covers everything is the failure
    the modifier exists to describe."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_PARTIAL_CHAIN.format(user_binding="user_id"))

    covered, users = executor.execute_raw_sql(
        "select (select count(distinct user_id) from lines_tbl), "
        "       (select count(*) from users_tbl)"
    ).fetchall()[0]
    assert 0 < covered < users


def test_mock_fact_fans_out_over_its_dimension():
    """A fact must carry several rows per dimension member, and not the same
    number for each: a 1:1 mock makes a row-multiplying join look identical to
    a well-behaved one, and a uniform ratio hides skew entirely. Every member
    still appears, because a complete binding that misses values is a model
    error, not mock noise."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key user_id int;
        key line_id int;
        property line_id.amount float;

        datasource users (id: user_id) grain (user_id) address users_tbl;
        datasource lines (
            id: line_id,
            user_id: user_id,
            amount: amount,
        )
        grain (line_id)
        address lines_tbl;

        mock datasources users, lines;
    """)

    users, lines, covered, low, high = executor.execute_raw_sql(
        "select (select count(*) from users_tbl), "
        "       (select count(*) from lines_tbl), "
        "       (select count(distinct user_id) from lines_tbl), "
        "       (select min(c) from (select count(*) c from lines_tbl "
        "          group by user_id)), "
        "       (select max(c) from (select count(*) c from lines_tbl "
        "          group by user_id))"
    ).fetchall()[0]
    assert lines > users
    assert covered == users
    assert low >= 1
    assert high > low


def test_mock_rollup_is_computed_from_the_fact_it_summarizes():
    """A datasource binding derived concepts is a rollup, not a source: its
    metrics must be computed off the mocked fact, or the same question answered
    through the pre-aggregate and through the base table disagrees."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.amount float;
        property id.cat enum<string>['a', 'b', 'c'];
        auto total <- sum(amount);
        auto line_count <- count(id);

        root datasource lines (
            id: id,
            amount: amount,
            cat: cat,
        )
        grain (id)
        address lines_tbl;

        datasource cat_totals (
            cat: cat,
            total: total,
            line_count: line_count,
        )
        grain (cat)
        address cat_totals_tbl;

        mock datasources lines, cat_totals;
    """)

    rows, rollup_total, rollup_count, fact_total, fact_count = executor.execute_raw_sql(
        "select (select count(*) from cat_totals_tbl), "
        "       (select sum(total) from cat_totals_tbl), "
        "       (select sum(line_count) from cat_totals_tbl), "
        "       (select sum(amount) from lines_tbl), "
        "       (select count(*) from lines_tbl)"
    ).fetchall()[0]
    assert rows == 3
    assert rollup_count == fact_count
    assert rollup_total == pytest.approx(fact_total)


_DECLARED_MODEL = """
    key id int;
    key user_id int;
    property id.note string;
    property id.score int[0..50];
    property id.status string;

    datasource users (id: user_id) grain (user_id) address users_tbl;
    datasource events (
        id: id,
        user_id: ~?user_id,
        note: ~?note,
        score: score,
        status: status,
    )
    grain (id)
    address events_tbl
    where status = 'ok';

    mock datasources users, events;
"""


def _declared_model_row(executor):
    return dict(
        zip(
            (
                "rows",
                "null_fk",
                "null_note",
                "null_score",
                "null_id",
                "statuses",
                "status",
                "low",
                "high",
                "covered",
                "users",
            ),
            executor.execute_raw_sql(
                "select count(*), "
                "sum(case when user_id is null then 1 else 0 end), "
                "sum(case when note is null then 1 else 0 end), "
                "sum(case when score is null then 1 else 0 end), "
                "sum(case when id is null then 1 else 0 end), "
                "count(distinct status), min(status), "
                "min(score), max(score), count(distinct user_id), "
                "(select count(*) from users_tbl) from events_tbl"
            ).fetchall()[0],
        )
    )


def test_mock_nullable_column_is_sometimes_empty():
    """A column that declares it can be empty and never is makes three-valued
    logic unobservable — and leaves a value NULL indistinguishable from an
    outer-join padding NULL. Grain components stay populated (a NULL there is a
    grain violation), and nulling never costs a distinct value, which
    validate_multi_datasource_concept would read as missing data."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_DECLARED_MODEL)

    row = _declared_model_row(executor)
    assert row["null_fk"] > 0
    assert row["null_note"] > 0
    assert row["null_score"] == 0
    assert row["null_id"] == 0
    assert 0 < row["covered"] < row["users"]


def test_mock_honours_a_datasource_where_clause():
    """A datasource declared `where status = 'ok'` describes a table holding
    only those rows; mocking it with every status validates the model against
    data its own declaration says cannot exist."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_DECLARED_MODEL)

    row = _declared_model_row(executor)
    assert row["statuses"] == 1
    assert row["status"] == "ok"


def test_mock_declared_range_includes_its_endpoints():
    """Off-by-one and inclusive/exclusive bugs live on a range's edges, and a
    pool that only samples the interior never lands on one."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_DECLARED_MODEL)

    row = _declared_model_row(executor)
    assert row["low"] == 0
    assert row["high"] == 50


def test_mock_shared_address_keeps_every_bound_column():
    """Each datasource writes its whole table, so two bindings of one address
    with different column sets would leave the loser's columns missing from the
    table every query against them reads."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key a_id int;
        property a_id.label string;
        property a_id.extra string;

        datasource one (id: a_id, label: label) grain (a_id) address shared_tbl;
        datasource two (
            id: a_id,
            label: label,
            extra: extra,
        )
        grain (a_id)
        address shared_tbl;

        mock datasources two, one;
    """)

    columns = {
        row[0] for row in executor.execute_raw_sql("describe shared_tbl").fetchall()
    }
    assert columns == {"id", "label", "extra"}


def test_canonical_column_map_merges_namespaced_bindings():
    """Two namespaces reaching the same physical column must share one pool —
    otherwise the second table written to that address contradicts the first."""
    from trilogy.dialect.mock import canonical_column_map

    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key a_id int;
        property a_id.a_label string;
        key b_id int;
        property b_id.b_label string;

        datasource one (id: a_id, label: a_label) grain (a_id) address shared_tbl;
        datasource two (id: b_id, label: b_label) grain (b_id) address shared_tbl;
    """)

    canonical = canonical_column_map(executor.environment)
    assert canonical["local.a_id"] == canonical["local.b_id"]
    assert canonical["local.a_label"] == canonical["local.b_label"]
    assert canonical["local.a_id"] != canonical["local.a_label"]


def test_cli_validate_quiet_collects_target_failures():
    """The quiet validation path collects per-target failures via the
    on_target_complete callback and surfaces them as a single
    ModelValidationError summary."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.tail_num string;

        datasource flight (
            id: id,
            tail_num: tail_num,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 123 AS tail_num
        ''';
    """)

    with pytest.raises(ModelValidationError) as exc_info:
        cli_validate_environment(executor, mock=False, quiet=True)

    assert "tail_num" in str(exc_info.value)


def test_cli_validate_quiet_records_synthesis_error(monkeypatch):
    """If core validation raises ModelValidationError before any per-target
    callback fires (e.g. a synthesis error setting up grain_check concepts),
    the quiet path should still surface it as an environment-level failure."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        datasource thing (id: id) grain (id) address thing_tbl;
    """)

    def fake_validate(*args, **kwargs):
        raise ModelValidationError("synthesis failed")

    monkeypatch.setattr(
        "trilogy.core.validation.environment.validate_environment", fake_validate
    )

    with pytest.raises(ModelValidationError) as exc_info:
        cli_validate_environment(executor, mock=False, quiet=True)

    assert "synthesis failed" in str(exc_info.value)


def test_cli_validate_rich_collects_target_failures():
    """The rich (non-quiet) validation path renders failures and exits with
    click.Exit(1)."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.tail_num string;

        datasource flight (
            id: id,
            tail_num: tail_num,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 123 AS tail_num
        ''';
    """)

    with pytest.raises(Exit):
        cli_validate_environment(executor, mock=False, quiet=False)


def test_cli_validate_rich_records_synthesis_error(monkeypatch):
    """The rich path's synthesis fallback should record an environment-level
    failure when core validation raises before any per-target callback fires."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        datasource thing (id: id) grain (id) address thing_tbl;
    """)

    def fake_validate(*args, **kwargs):
        raise ModelValidationError("synthesis failed")

    monkeypatch.setattr(
        "trilogy.core.validation.environment.validate_environment", fake_validate
    )

    with pytest.raises(Exit):
        cli_validate_environment(executor, mock=False, quiet=False)


def test_cli_validate_quiet_success():
    """A clean environment under the quiet path should return without raising."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.name string;
        datasource thing (
            id: id,
            name: name,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'
        ''';
    """)

    cli_validate_environment(executor, mock=False, quiet=True)
