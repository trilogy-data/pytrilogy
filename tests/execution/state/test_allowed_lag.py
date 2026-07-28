from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import DatePart
from trilogy.core.models.datasource import Datasource, FreshnessLag
from trilogy.execution.state import BaseStateStore
from trilogy.execution.state.watermarks import _watermark_distance, within_allowed_lag
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.model import HydrationError

MINUTES_10 = FreshnessLag(value=10, unit=DatePart.MINUTE)
BARE_500 = FreshnessLag(value=500)


class TestWatermarkDistance:
    def test_datetime(self) -> None:
        assert _watermark_distance(
            datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 5)
        ) == timedelta(minutes=5)

    def test_date(self) -> None:
        assert _watermark_distance(date(2024, 1, 1), date(2024, 1, 3)) == timedelta(
            days=2
        )

    def test_numeric(self) -> None:
        assert _watermark_distance(10, 60) == 50

    @pytest.mark.parametrize(
        "current,expected",
        [
            ("abc", "abd"),
            (datetime(2024, 1, 1, 12, 0), date(2024, 1, 2)),
            (True, False),
            (1, "2"),
        ],
    )
    def test_not_measurable(self, current, expected) -> None:
        assert _watermark_distance(current, expected) is None

    def test_mixed_awareness_raises(self) -> None:
        from datetime import timezone

        with pytest.raises(TypeError):
            _watermark_distance(
                datetime(2024, 1, 1, 12, 0),
                datetime(2024, 1, 1, 12, 5, tzinfo=timezone.utc),
            )


class TestWithinAllowedLag:
    def test_temporal_inside(self) -> None:
        assert within_allowed_lag(
            datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 5), MINUTES_10, "ts"
        )

    def test_temporal_at_boundary(self) -> None:
        assert within_allowed_lag(
            datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 10), MINUTES_10, "ts"
        )

    def test_temporal_outside(self) -> None:
        assert not within_allowed_lag(
            datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 11), MINUTES_10, "ts"
        )

    def test_numeric(self) -> None:
        assert within_allowed_lag(1000, 1400, BARE_500, "seq")
        assert not within_allowed_lag(1000, 1600, BARE_500, "seq")

    def test_missing_value_is_never_lag(self) -> None:
        assert not within_allowed_lag(None, datetime(2024, 1, 1), MINUTES_10, "ts")

    def test_unmeasurable_raises(self) -> None:
        with pytest.raises(TypeError, match="not"):
            within_allowed_lag("abc", "abd", BARE_500, "hash")

    def test_unit_on_numeric_watermark_raises(self) -> None:
        with pytest.raises(TypeError, match="does not fit"):
            within_allowed_lag(1000, 1400, MINUTES_10, "seq")

    def test_bare_number_on_temporal_watermark_raises(self) -> None:
        with pytest.raises(TypeError, match="does not fit"):
            within_allowed_lag(
                datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 12, 5), BARE_500, "ts"
            )


@pytest.fixture
def engine() -> Executor:
    """Own executor per test — staleness scans span the whole environment, so a
    shared one would see other tests' datasources."""
    return Executor(
        dialect=Dialects.DUCK_DB,
        engine=create_engine("duckdb:///:memory:", future=True),
        environment=Environment(),
    )


SYNTAX_HEADER = """
key order_id int;
property order_id.updated_at datetime;
property order_id.seq int;
"""


def _parse_datasource(trailer: str) -> Datasource:
    env = Environment()
    env.parse(
        SYNTAX_HEADER
        + "datasource orders (id: order_id, ts: updated_at, s: seq,)"
        + " grain (order_id) address orders "
        + trailer
    )
    return env.datasources["orders"]


class TestLagSyntax:
    @pytest.mark.parametrize(
        "trailer,expected",
        [
            (
                "freshness by updated_at within 5 minutes;",
                FreshnessLag(value=5, unit=DatePart.MINUTE),
            ),
            (
                "freshness by updated_at within 1 hour;",
                FreshnessLag(value=1, unit=DatePart.HOUR),
            ),
            (
                "incremental by updated_at within 2 days;",
                FreshnessLag(value=2, unit=DatePart.DAY),
            ),
            ("incremental by seq within 500;", FreshnessLag(value=500)),
            ("freshness by updated_at;", None),
        ],
    )
    def test_parses(self, trailer: str, expected: FreshnessLag | None) -> None:
        assert _parse_datasource(trailer).allowed_lag == expected

    @pytest.mark.parametrize(
        "trailer",
        [
            "freshness by updated_at within 5 minutes;",
            "incremental by seq within 500;",
        ],
    )
    def test_round_trips(self, trailer: str) -> None:
        rendered = Renderer().to_string(_parse_datasource(trailer))
        env = Environment()
        env.parse(SYNTAX_HEADER + rendered)
        assert (
            env.datasources["orders"].allowed_lag
            == _parse_datasource(trailer).allowed_lag
        )

    @pytest.mark.parametrize(
        "trailer,message",
        [
            ("freshness by updated_at within 5;", "needs a numeric watermark"),
            ("incremental by seq within 5 minutes;", "needs a temporal watermark"),
            ("freshness by updated_at within 2 months;", "not a fixed-length unit"),
            ("within 5 minutes;", "needs a watermark to measure against"),
        ],
    )
    def test_rejects(self, trailer: str, message: str) -> None:
        with pytest.raises(HydrationError, match=message):
            _parse_datasource(trailer)

    def test_rejects_probe_lag(self) -> None:
        with pytest.raises(HydrationError, match="probe-based freshness"):
            _parse_datasource("freshness by `probe.py` within 5 minutes;")

    def test_rejects_root_lag(self) -> None:
        env = Environment()
        with pytest.raises(HydrationError, match="not supported on root"):
            env.parse(
                SYNTAX_HEADER
                + "root datasource stream (id: order_id, ts: updated_at,)"
                + " grain (order_id) address stream within 30 seconds;"
            )


def _lag_model(consumer_lag: str = "") -> str:
    return f"""
        key event_id int;
        property event_id.event_ts datetime;

        root datasource source_events (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        query '''
        SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
        UNION ALL
        SELECT 2 as event_id, TIMESTAMP '2024-01-10 12:05:00' as event_ts
        ''';

        datasource target_events (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        address target_events_table
        incremental by event_ts{consumer_lag};

        CREATE IF NOT EXISTS DATASOURCE target_events;

        RAW_SQL('''
        INSERT INTO target_events_table
        SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
        ''');
        """


def _stale(engine: Executor):
    return BaseStateStore().get_stale_assets(engine.environment, engine)


def test_consumer_lag_suppresses_staleness(engine: Executor):
    engine.execute_text(_lag_model(consumer_lag=" within 10 minutes"))
    assert _stale(engine) == []


def test_consumer_lag_too_small_still_stale(engine: Executor):
    engine.execute_text(_lag_model(consumer_lag=" within 1 minute"))
    stale = _stale(engine)

    assert len(stale) == 1
    assert stale[0].datasource_id == "target_events"
    assert "exceeds allowed lag 1 minute" in stale[0].reason


def test_no_lag_is_stale(engine: Executor):
    engine.execute_text(_lag_model())
    stale = _stale(engine)

    assert len(stale) == 1
    assert "exceeds allowed lag" not in stale[0].reason


def test_numeric_lag_on_incremental_key(engine: Executor):
    engine.execute_text("""
        key batch_id int;
        property batch_id.seq int;

        root datasource source_batches (
            batch_id: batch_id,
            seq: seq
        )
        grain (batch_id)
        query '''
        SELECT 1 as batch_id, 400 as seq
        ''';

        datasource target_batches (
            batch_id: batch_id,
            seq: seq
        )
        grain (batch_id)
        address target_batches_table
        incremental by seq within 500;

        CREATE IF NOT EXISTS DATASOURCE target_batches;

        RAW_SQL('''
        INSERT INTO target_batches_table
        SELECT 1 as batch_id, 100 as seq
        ''');
        """)
    assert _stale(engine) == []


def test_empty_target_is_stale_despite_lag(engine: Executor):
    engine.execute_text("""
        key event_id int;
        property event_id.event_ts datetime;

        root datasource lag_source (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        query '''
        SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
        ''';

        datasource lag_target (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        address lag_target_table
        incremental by event_ts within 1 day;

        CREATE IF NOT EXISTS DATASOURCE lag_target;
        """)
    stale = _stale(engine)

    assert len(stale) == 1
    assert stale[0].datasource_id == "lag_target"
