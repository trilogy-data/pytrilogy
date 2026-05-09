"""Unit tests for ClickhouseConfig.connection_string URL parsing.

No DB connection required — these run on every platform.
"""

import sys

import pytest

from trilogy.dialect.config import ClickhouseConfig

# chdb has no Windows wheel; skip engine-construction tests there.
skip_on_windows = pytest.mark.skipif(
    sys.platform == "win32", reason="chdb has no Windows wheel"
)


def test_chdb_default():
    cs = ClickhouseConfig(mode="chdb").connection_string()
    assert cs == "chdb:///:memory:"


def test_chdb_with_path():
    cs = ClickhouseConfig(mode="chdb", chdb_path="/tmp/x.db").connection_string()
    assert cs == "chdb:////tmp/x.db"


def test_invalid_mode_rejected():
    with pytest.raises(ValueError, match="must be 'chdb' or 'server'"):
        ClickhouseConfig(mode="bogus")


def test_server_bare_host_native():
    cs = ClickhouseConfig(
        mode="server", host="example.com", username="u", password="p"
    ).connection_string()
    # bare host + no secure → native, port 9000, no query
    assert cs == "clickhouse+native://u:p@example.com:9000/default"


def test_server_bare_host_native_secure_default_port():
    cs = ClickhouseConfig(
        mode="server", host="example.com", secure=True
    ).connection_string()
    assert cs == "clickhouse+native://default@example.com:9440/default?secure=true"


def test_server_https_url_picks_http_driver():
    cs = ClickhouseConfig(
        mode="server",
        host="https://foo.clickhouse.cloud:8443",
        username="u",
        password="p",
        database="db",
    ).connection_string()
    assert cs.startswith("clickhouse+http://")
    assert "foo.clickhouse.cloud:8443" in cs
    assert "/db" in cs
    assert "protocol=https" in cs


def test_server_http_url_no_tls():
    cs = ClickhouseConfig(
        mode="server", host="http://localhost:8123"
    ).connection_string()
    assert cs.startswith("clickhouse+http://")
    assert ":8123" in cs
    assert "protocol=https" not in cs


def test_port_9440_overrides_https_scheme():
    """User pasted https://...:9440 — port wins, switch to native+TLS."""
    cs = ClickhouseConfig(
        mode="server",
        host="https://foo.clickhouse.cloud:9440",
        password="pw",
    ).connection_string()
    assert cs.startswith("clickhouse+native://")
    assert ":9440" in cs
    assert "secure=true" in cs


def test_port_8443_forces_http_driver():
    """Bare host with explicit port 8443 should pick HTTP driver."""
    cs = ClickhouseConfig(
        mode="server",
        host="example.com",
        port=8443,
        secure=True,
    ).connection_string()
    assert cs.startswith("clickhouse+http://")


def test_explicit_port_wins_over_url_port():
    cs = ClickhouseConfig(
        mode="server",
        host="https://example.com:8443",
        port=9000,
    ).connection_string()
    assert ":9000" in cs
    assert ":8443" not in cs


def test_password_url_encoded():
    cs = ClickhouseConfig(
        mode="server", host="example.com", username="u", password="a/b@c"
    ).connection_string()
    # / and @ in passwords would otherwise corrupt URL parsing
    assert "a%2Fb%40c" in cs


def test_no_password_no_colon():
    cs = ClickhouseConfig(
        mode="server", host="example.com", username="u"
    ).connection_string()
    assert "u@example.com" in cs
    assert "u:@example.com" not in cs


def test_database_default_when_unset():
    cs = ClickhouseConfig(mode="server", host="example.com").connection_string()
    assert "/default" in cs


@skip_on_windows
def test_default_engine_chdb_mode():
    """Dialects.CLICKHOUSE.default_engine wires chdb mode to ChdbEngine."""
    from trilogy import Dialects
    from trilogy.dialect.clickhouse_chdb import ChdbEngine

    engine = Dialects.CLICKHOUSE.default_engine(conf=ClickhouseConfig(mode="chdb"))
    assert isinstance(engine, ChdbEngine)


@skip_on_windows
def test_default_engine_server_mode_constructs_sqlalchemy():
    """Server mode produces a SQLAlchemy engine without connecting."""
    from trilogy import Dialects

    engine = Dialects.CLICKHOUSE.default_engine(
        conf=ClickhouseConfig(
            mode="server",
            host="not-a-real-host.invalid",
            password="x",
        )
    )
    assert engine.dialect.name == "clickhouse"


def test_default_engine_rejects_wrong_config_type():
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    with pytest.raises(TypeError, match="ClickhouseConfig"):
        Dialects.CLICKHOUSE.default_engine(conf=DuckDBConfig())  # type: ignore[arg-type]


def test_clickhouse_dialect_alias():
    """The 'chdb' string maps to the same Dialects entry as 'clickhouse'."""
    from trilogy import Dialects

    assert Dialects("chdb") is Dialects.CLICKHOUSE
    assert Dialects("clickhouse") is Dialects.CLICKHOUSE


# Direct unit tests for the FUNCTION_MAP helper functions. These cover the
# error/edge branches that don't fire from the parametrized smoke tests.


def test_ch_log_base_10():
    from trilogy.dialect.clickhouse import _ch_log

    assert _ch_log(["x", "10"]) == "log10(x)"


def test_ch_log_base_2():
    from trilogy.dialect.clickhouse import _ch_log

    assert _ch_log(["x", "2"]) == "log2(x)"


def test_ch_log_arbitrary_base():
    from trilogy.dialect.clickhouse import _ch_log

    assert _ch_log(["x", "3"]) == "(ln(x) / ln(3))"


def test_ch_hash_supported_types():
    from trilogy.dialect.clickhouse import _ch_hash

    assert _ch_hash("col", "'md5'") == "lower(hex(MD5(col)))"
    assert _ch_hash("col", "'sha1'") == "lower(hex(SHA1(col)))"
    assert _ch_hash("col", "'sha256'") == "lower(hex(SHA256(col)))"
    assert _ch_hash("col", "'sha512'") == "lower(hex(SHA512(col)))"


def test_ch_hash_rejects_unknown():
    from trilogy.dialect.clickhouse import _ch_hash

    with pytest.raises(ValueError, match="Unsupported hash"):
        _ch_hash("col", "'crc32'")


def test_ch_struct_without_types():
    """Defensive: if types isn't passed, fall back to Nullable(Nothing)."""
    from trilogy.dialect.clickhouse import _ch_struct

    out = _ch_struct(["1", "'a'"], [])
    assert out.startswith("cast(tuple(1)")
    assert "Nullable(Nothing)" in out


def test_chdb_coerce_dates():
    """ChdbConnection coerces Date/DateTime JSON strings back to Python objects.

    The HTTP-style JSON output of chdb returns dates as ISO strings; the native
    server driver returns date/datetime objects. We bridge the two so callers
    don't see backend-specific shapes.
    """
    from datetime import date, datetime

    from trilogy.dialect.clickhouse_chdb import (
        _coerce_value,
        _parse_datetime,
        _strip_modifiers,
    )

    assert _strip_modifiers("Date") == "Date"
    assert _strip_modifiers("Nullable(Date)") == "Date"
    assert _strip_modifiers("Nullable(DateTime64(3))") == "DateTime64"
    assert _strip_modifiers("LowCardinality(Nullable(String))") == "String"
    assert _strip_modifiers("Array(Int64)") == "Array"

    assert _coerce_value("Date", "2024-03-15") == date(2024, 3, 15)
    assert _coerce_value("Nullable(Date)", "2024-03-15") == date(2024, 3, 15)
    assert _coerce_value("Date", None) is None
    assert _coerce_value("DateTime64(3)", "2024-03-15 10:20:30.000") == datetime(
        2024, 3, 15, 10, 20, 30
    )
    assert _coerce_value("String", "hello") == "hello"
    assert _coerce_value("Int64", 42) == 42

    # _parse_datetime handles the space separator chdb uses
    assert _parse_datetime("2024-03-15 10:20:30") == datetime(2024, 3, 15, 10, 20, 30)


def test_render_map_literal_emits_map_call():
    """CH render_map_literal uses map(k, v, ...) instead of MAP {k:v}.

    Avoids the SQLAlchemy text() bug where `:value` after a colon is read as a
    bound parameter.
    """
    from trilogy.core.models.core import DataType, MapWrapper
    from trilogy.dialect.clickhouse import ClickhouseDialect

    dialect = ClickhouseDialect()
    mw = MapWrapper(
        {"a": 1, "b": 2},
        key_type=DataType.STRING,
        value_type=DataType.INTEGER,
    )
    out = dialect.render_map_literal(mw)
    assert out.startswith("map(")
    assert "'a'" in out and "'b'" in out
    assert "1" in out and "2" in out
    # Confirm no `:` between key and value (this is the actual bug guard)
    assert "'a':1" not in out
