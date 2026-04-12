from trilogy.core.enums import Purpose
from trilogy.core.environment_helpers import enrich_environment
from trilogy.core.models.author import Concept, Grain, Metadata
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment


def _make_env(*concepts: Concept) -> Environment:
    env = Environment()
    for c in concepts:
        env.add_concept(c)
    return env


def _concept(name: str, datatype: DataType | str, purpose: Purpose) -> Concept:
    return Concept(
        name=name,
        datatype=datatype,
        purpose=purpose,
        grain=Grain(),
        namespace="default",
        keys=set(),
        metadata=Metadata(),
    )


def test_enrich_environment_key_concept():
    key = _concept("user_id", DataType.INTEGER, Purpose.KEY)
    env = _make_env(key)

    enrich_environment(env)

    assert "default.user_id.count" in env.concepts


def test_enrich_environment_date_concept():
    d = _concept("event_date", DataType.DATE, Purpose.PROPERTY)
    env = _make_env(d)

    enrich_environment(env)

    for suffix in (
        "month",
        "year",
        "quarter",
        "day",
        "day_of_week",
        "month_start",
        "year_start",
    ):
        assert f"default.event_date.{suffix}" in env.concepts


def test_enrich_environment_datetime_concept():
    dt = _concept("created_at", DataType.DATETIME, Purpose.PROPERTY)
    env = _make_env(dt)

    enrich_environment(env)

    for suffix in ("month", "year", "date", "hour", "minute", "second"):
        assert f"default.created_at.{suffix}" in env.concepts


def test_enrich_environment_timestamp_concept():
    ts = _concept("updated_at", DataType.TIMESTAMP, Purpose.PROPERTY)
    env = _make_env(ts)

    enrich_environment(env)

    for suffix in ("month", "year", "date", "hour", "minute", "second"):
        assert f"default.updated_at.{suffix}" in env.concepts


def test_enrich_environment_skips_non_date_non_key():
    c = _concept("name", DataType.STRING, Purpose.PROPERTY)
    env = _make_env(c)

    enrich_environment(env)

    # No sub-concepts should be derived from a plain string property
    assert "default.name.count" not in env.concepts
    assert "default.name.month" not in env.concepts
