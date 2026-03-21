"""Tests for serve_helpers/state_computation.py."""

import textwrap
from pathlib import Path

import pytest

from trilogy.scripts.serve_helpers.state_computation import compute_state_sync

# A minimal DuckDB-compatible trilogy file with one root datasource.
SIMPLE_PREQL = textwrap.dedent(
    """\
    key id int;

    root datasource raw (
        id
    )
    grain (id)
    query '''select 1 as id''';
"""
)

# Two datasources: root + a derived table that won't exist → stale/unknown.
INCREMENTAL_PREQL = textwrap.dedent(
    """\
    key id int;
    property id.version int;

    root datasource raw (
        id,
        version
    )
    grain (id)
    query '''select 1 as id, 1 as version''';

    datasource derived (
        id,
        version
    )
    grain (id)
    address derived_missing_table
    incremental by version;
"""
)


# ── no-dialect error ──────────────────────────────────────────────────────────


def test_compute_state_raises_without_dialect(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    with pytest.raises(ValueError, match="No dialect"):
        compute_state_sync(preql, "generic", None, tmp_path)


# ── happy path via engine param ───────────────────────────────────────────────


def test_compute_state_with_duckdb_engine(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    response = compute_state_sync(preql, "duck_db", None, tmp_path)

    assert response.target == "test.preql"
    assert response.summary.total == 1
    assert response.summary.root == 1
    assert response.assets[0].id == "raw"
    assert response.assets[0].is_root is True


# ── happy path via config file ────────────────────────────────────────────────


def test_compute_state_with_config_dialect(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    toml = tmp_path / "trilogy.toml"
    toml.write_text('[engine]\ndialect = "duckdb"\n')

    response = compute_state_sync(preql, "generic", toml, tmp_path)
    assert response.target == "test.preql"
    assert response.summary.total >= 1


# ── asset status classifications ──────────────────────────────────────────────


def test_compute_state_root_is_listed_first(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    response = compute_state_sync(preql, "duck_db", None, tmp_path)

    assert len(response.assets) == 2
    # Roots must appear before non-roots
    assert response.assets[0].is_root is True
    assert response.assets[1].is_root is False


def test_compute_state_missing_table_marks_stale_or_unknown(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    response = compute_state_sync(preql, "duck_db", None, tmp_path)

    by_id = {a.id: a for a in response.assets}
    assert "raw" in by_id
    # derived_missing_table does not exist → stale (watermark behind root) or unknown
    assert "derived" in by_id
    assert by_id["derived"].status in ("stale", "unknown")


def test_compute_state_summary_counts(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    response = compute_state_sync(preql, "duck_db", None, tmp_path)

    s = response.summary
    # root is a structural attribute (can overlap with fresh/stale/unknown)
    assert s.total == s.stale + s.fresh + s.unknown
    assert s.root <= s.total


def test_compute_state_watermark_serialized(tmp_path: Path) -> None:
    """Watermarks (if any) must be serialized as WatermarkInfo dicts."""
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    response = compute_state_sync(preql, "duck_db", None, tmp_path)

    # The root datasource (raw) may have a watermark for 'version'.
    by_id = {a.id: a for a in response.assets}
    # watermarks is always a dict; keys are concept names when present
    assert isinstance(by_id["raw"].watermarks, dict)
