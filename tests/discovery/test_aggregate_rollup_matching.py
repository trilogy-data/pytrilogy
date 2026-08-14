"""Matching an agent-authored inline aggregate to a hidden summary table.

This is an *optimization*: when it stops working the queries still return the
right numbers, just off the raw fact table, so nothing fails loudly. Every test
here therefore asserts both halves — the summary table IS used, and the answer
still equals the raw-fact answer — and the negative cases assert the summary is
NOT used where the rollup would be unsound.
"""

from pytest import fixture, mark

from trilogy import Dialects, Environment
from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX
from trilogy.core.enums import FunctionType
from trilogy.core.models.build import canonical_address_for

BASE = """
key id int;
key origin_code string;
key destination_code string;
key carrier_code string;
property carrier_code.carrier_name string;
property id.flight_date date;
property id.distance int;
property id.delay int;

auto total_distance <- sum(distance);
auto flight_count <- count(id);
auto avg_distance <- avg(distance);
auto distinct_carriers <- count_distinct(carrier_code);

datasource flight (
    id:id,
    origin_code:origin_code,
    destination_code:destination_code,
    carrier_code:carrier_code,
    flight_date:flight_date,
    distance:distance,
    delay:delay,
)
grain (id)
query '''
select 1 as id, 'A' as origin_code, 'X' as destination_code, 'AA' as carrier_code, '2024-01-01'::date as flight_date, 10 as distance, 5 as delay
union all select 2, 'A', 'Y', 'AA', '2024-01-01'::date, 20, 0
union all select 3, 'B', 'X', 'UA', '2024-01-01'::date, 30, 15
union all select 4, 'A', 'X', 'UA', '2024-01-02'::date, 40, 0
''';

datasource carrier (
    carrier_code:carrier_code,
    carrier_name:carrier_name,
)
grain (carrier_code)
query '''
select 'AA' as carrier_code, 'American' as carrier_name
union all select 'UA' as carrier_code, 'United' as carrier_name
''';
"""

# Grain (origin_code, destination_code, flight_date); deliberately consistent
# with `flight` so any numeric divergence is a rollup bug, not bad fixture data.
SUMMARY = """
datasource flight_agg (
    origin_code:origin_code,
    destination_code:destination_code,
    flight_date:flight_date,
    flight_count:flight_count,
    total_distance:total_distance,
)
grain (origin_code, destination_code, flight_date)
query '''
select 'A' as origin_code, 'X' as destination_code, '2024-01-01'::date as flight_date, 1 as flight_count, 10 as total_distance
union all select 'A', 'Y', '2024-01-01'::date, 1, 20
union all select 'B', 'X', '2024-01-01'::date, 1, 30
union all select 'A', 'X', '2024-01-02'::date, 1, 40
''';
"""

# Grain (carrier_code) — cannot answer anything keyed by origin/destination.
CARRIER_SUMMARY = """
datasource flight_carrier_agg (
    carrier_code:carrier_code,
    flight_count:flight_count,
    total_distance:total_distance,
)
grain (carrier_code)
query '''
select 'AA' as carrier_code, 2 as flight_count, 30 as total_distance
union all select 'UA' as carrier_code, 2 as flight_count, 70 as total_distance
''';
"""


def _executor(text: str):
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(text)
    return exec


@fixture
def raw():
    return _executor(BASE)


@fixture
def summarized():
    return _executor(BASE + SUMMARY)


def _rows(exec, query: str):
    return sorted(str(tuple(row)) for row in exec.execute_text(query)[-1].fetchall())


def assert_served_by_summary(raw, summarized, query: str):
    """The summary answers the query, the raw fact is not touched, and the
    numbers are unchanged."""
    sql = summarized.generate_sql(query)[-1]
    assert "flight_agg" in sql, sql
    assert '"flight"' not in sql, sql
    assert _rows(raw, query) == _rows(summarized, query)


def assert_not_served_by_summary(raw, summarized, query: str):
    sql = summarized.generate_sql(query)[-1]
    assert "flight_agg" not in sql, sql
    assert _rows(raw, query) == _rows(summarized, query)


# --------------------------------------------------------------------------
# The matrix that regressed: named vs inline alias, exact vs coarser grain,
# local vs imported namespace. Only the same-namespace/inline/exact cell used
# to work.
# --------------------------------------------------------------------------

EXACT = "origin_code, destination_code, flight_date"

MATRIX = {
    "named_exact": f"select {EXACT}, total_distance;",
    "named_coarser": "select origin_code, total_distance;",
    "inline_exact": f"select {EXACT}, sum(distance) as td;",
    "inline_coarser": "select origin_code, sum(distance) as td;",
    "inline_coarser_count": "select origin_code, count(id) as fc;",
    "inline_single_key": "select flight_date, sum(distance) as td;",
    "inline_two_keys": "select origin_code, flight_date, sum(distance) as td;",
    "named_grand_total": "select total_distance;",
    "inline_grand_total": "select sum(distance) as td;",
}


@mark.parametrize("case", sorted(MATRIX))
def test_summary_serves_local_namespace(raw, summarized, case):
    assert_served_by_summary(raw, summarized, MATRIX[case])


@mark.parametrize("case", sorted(MATRIX))
def test_summary_serves_imported_namespace(tmp_path, case):
    """The dominant eval failure: the model is imported under an alias, so the
    query's inline aggregate is authored in `local` while the summary column
    lives in `f`. Their canonicals differ only by that prefix."""
    (tmp_path / "m.preql").write_text(BASE + SUMMARY)
    (tmp_path / "raw.preql").write_text(BASE)

    def build(module: str):
        env = Environment(working_path=tmp_path)
        exec = Dialects.DUCK_DB.default_executor(environment=env)
        exec.parse_text(f"import {module} as f;")
        return exec

    query = MATRIX[case].replace("origin_code", "f.origin_code")
    query = query.replace("destination_code", "f.destination_code")
    query = query.replace("flight_date", "f.flight_date")
    query = query.replace("total_distance", "f.total_distance")
    query = query.replace("sum(distance)", "sum(f.distance)")
    query = query.replace("count(id)", "count(f.id)")
    assert_served_by_summary(build("raw"), build("m"), query)


def test_summary_rollup_renders_sum_and_group_by(summarized):
    sql = summarized.generate_sql("select origin_code, sum(distance) as td;")[-1]
    assert 'sum("flight_agg"."total_distance")' in sql, sql
    assert "GROUP BY" in sql, sql


def test_summary_serves_group_level_filter(raw, summarized):
    """A filter on a summary grain key is applied to the summary itself."""
    assert_served_by_summary(
        raw,
        summarized,
        "select origin_code, sum(distance) as td where flight_date > '2024-01-01'::date;",
    )


def test_summary_serves_any_subset_of_its_grain(raw, summarized):
    assert_served_by_summary(
        raw,
        summarized,
        "select destination_code, sum(distance) as td;",
    )


# --------------------------------------------------------------------------
# Negative cases — the rollup must not fire where it would be unsound.
# --------------------------------------------------------------------------


def test_non_additive_aggregate_not_rolled_up(raw, summarized):
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, avg(distance) as ad;"
    )
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, count_distinct(carrier_code) as dc;"
    )
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, max(distance) as md;"
    )


def test_unbound_measure_not_rolled_up(raw, summarized):
    """`delay` is not in the summary, so `sum(delay)` shares no signature with
    any of its columns."""
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, sum(delay) as td;"
    )


def test_row_level_filter_not_rolled_up(raw, summarized):
    """`distance` varies inside a summary row, so the summary cannot express a
    row-level predicate on it."""
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, sum(distance) as td where distance > 15;"
    )


def test_summary_cannot_supply_target_grain():
    """A carrier-grain summary must not answer a per-origin question: the rollup
    check has to require the *target* grain, not just a safe dropped grain
    (`carrier_code` is functionally determined by `id`, which used to be enough).
    """
    raw = _executor(BASE)
    summarized = _executor(BASE + CARRIER_SUMMARY)
    sql = summarized.generate_sql("select origin_code, sum(distance) as td;")[-1]
    assert "flight_carrier_agg" not in sql, sql
    assert _rows(raw, "select origin_code, sum(distance) as td;") == _rows(
        summarized, "select origin_code, sum(distance) as td;"
    )


def test_summary_serves_property_of_its_own_grain():
    """The complement of the above: `carrier_name` is a property of the carrier
    summary's grain key, so that summary IS usable."""
    raw = _executor(BASE)
    summarized = _executor(BASE + CARRIER_SUMMARY)
    query = "select carrier_name, sum(distance) as td;"
    sql = summarized.generate_sql(query)[-1]
    assert "flight_carrier_agg" in sql, sql
    assert _rows(raw, query) == _rows(summarized, query)


def test_finer_request_not_served_by_coarser_summary(raw, summarized):
    """The summary is already grouped, so it cannot be split by a key it does
    not carry."""
    assert_not_served_by_summary(
        raw,
        summarized,
        f"select {EXACT}, carrier_code, sum(distance) as td;",
    )


def test_aggregate_inside_an_expression_is_a_known_gap(raw, summarized):
    """Only a bare aggregate is matched: `sum(x) + 0` demands the enclosing
    BASIC, and the summary-table search never looks inside its lineage. The
    answer is still right, just computed off the raw fact. Pinned so the day
    that changes is a deliberate one (it would also unlock `sum(a) - sum(b)`)."""
    assert_not_served_by_summary(
        raw, summarized, "select origin_code, sum(distance) + 0 as td;"
    )


# --------------------------------------------------------------------------
# The primitives the matching is built on.
# --------------------------------------------------------------------------


def test_canonical_address_strips_namespace_for_virtual_names():
    """A `_virt_*` name is a lineage hash over fully-qualified args, so it is
    already globally unique; the authoring namespace must not re-partition it.
    Ordinary names keep their namespace."""
    virt = f"{VIRTUAL_CONCEPT_PREFIX}_agg_sum_123"
    assert canonical_address_for("ss", virt) == canonical_address_for("local", virt)
    assert canonical_address_for("ss", virt) == f"{DEFAULT_NAMESPACE}.{virt}"
    assert canonical_address_for("ss", "distance") == "ss.distance"
    assert canonical_address_for("ss", "distance") != canonical_address_for(
        "local", "distance"
    )


def _build_env(text: str):
    exec = _executor(text)
    return exec.environment.materialize_for_select()


def test_additive_aggregate_signature_matches_across_spellings():
    env = _build_env(BASE + SUMMARY)
    bound = env.concepts["local.total_distance"]
    signature = bound.additive_aggregate_signature
    assert signature is not None
    assert signature[0] == FunctionType.SUM
    assert env.concepts["local.flight_count"].additive_aggregate_signature != signature
    # Non-additive aggregates have no rollup identity at all.
    assert env.concepts["local.avg_distance"].additive_aggregate_signature is None
    assert env.concepts["local.distinct_carriers"].additive_aggregate_signature is None
    # A plain column is not an aggregate.
    assert env.concepts["local.distance"].additive_aggregate_signature is None


def test_rollup_column_for_matches_a_coarser_aggregate():
    env = _build_env(
        BASE + SUMMARY + "auto origin_distance <- sum(distance) by origin_code;"
    )
    summary = env.datasources["flight_agg"]
    coarse = env.concepts["local.origin_distance"]
    column = summary.rollup_column_for(coarse)
    assert column is not None
    assert column.alias == "total_distance"


def test_rollup_column_for_ignores_a_table_at_the_aggregate_grain():
    """The column's own concept is pinned to the summary's grain — rolling it up
    to itself would double count, so it must not match."""
    env = _build_env(BASE + SUMMARY)
    summary = env.datasources["flight_agg"]
    bound = next(
        c.concept
        for c in summary.columns
        if c.concept.address == "local.total_distance"
    )
    assert summary.rollup_column_for(bound) is None


def test_rollup_column_for_survives_regraining():
    """`QueryDatasource.get_alias` re-grains the concept onto the source it is
    asking about. The finer-table check therefore reads the aggregate's own `by`
    grain — using `concept.grain` would make every request look exact and the
    rollup column would be silently lost."""
    env = _build_env(
        BASE + SUMMARY + "auto origin_distance <- sum(distance) by origin_code;"
    )
    summary = env.datasources["flight_agg"]
    regrained = env.concepts["local.origin_distance"].with_grain(summary.grain)
    assert summary.rollup_column_for(regrained) is not None


def test_rollup_column_for_rejects_non_additive_aggregates():
    env = _build_env(
        BASE + SUMMARY + "auto origin_avg <- avg(distance) by origin_code;"
    )
    summary = env.datasources["flight_agg"]
    assert summary.rollup_column_for(env.concepts["local.origin_avg"]) is None
