"""Tests for serve_helpers/state_computation.py.

Serve returns the shared ``StateSnapshot`` verbatim — there is no serve-local
state shape — so these assert on the snapshot contract.
"""

import textwrap
from pathlib import Path

import pytest

from trilogy.execution.state.snapshot import StateSnapshot
from trilogy.scripts.serve_helpers.state_computation import compute_state_snapshot_sync

# A minimal DuckDB-compatible trilogy file with one root datasource.
SIMPLE_PREQL = textwrap.dedent("""\
    key id int;

    root datasource raw (
        id
    )
    grain (id)
    query '''select 1 as id''';
""")

# Two datasources: root + a derived table that won't exist → stale/unknown.
INCREMENTAL_PREQL = textwrap.dedent("""\
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
""")


# ── no-dialect error ──────────────────────────────────────────────────────────


def test_compute_state_raises_without_dialect(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    with pytest.raises(ValueError, match="No dialect"):
        compute_state_snapshot_sync(preql, "generic", None, tmp_path)


# ── happy path via engine param ───────────────────────────────────────────────


def _datasources(snapshot: StateSnapshot) -> dict:
    return {
        ds.datasource_id: ds for asset in snapshot.assets for ds in asset.datasources
    }


def test_compute_state_with_duckdb_engine(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    snapshot = compute_state_snapshot_sync(preql, "duck_db", None, tmp_path)

    assert snapshot.target == "test.preql"
    assert snapshot.dialect == "duck_db"
    assert snapshot.summary.total == 1
    assert _datasources(snapshot)["raw"].is_root is True


# ── happy path via config file ────────────────────────────────────────────────


def test_compute_state_with_config_dialect(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(SIMPLE_PREQL)
    toml = tmp_path / "trilogy.toml"
    toml.write_text('[engine]\ndialect = "duckdb"\n')

    snapshot = compute_state_snapshot_sync(preql, "generic", toml, tmp_path)
    assert snapshot.target == "test.preql"
    assert snapshot.summary.total >= 1


# ── asset status classifications ──────────────────────────────────────────────


def test_compute_state_reports_both_datasources(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    snapshot = compute_state_snapshot_sync(preql, "duck_db", None, tmp_path)

    by_id = _datasources(snapshot)
    assert by_id["raw"].is_root is True
    assert by_id["derived"].is_root is False


def test_compute_state_missing_table_marks_stale_or_unknown(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    snapshot = compute_state_snapshot_sync(preql, "duck_db", None, tmp_path)

    by_id = _datasources(snapshot)
    # derived_missing_table does not exist -> stale (watermark behind root) or unknown
    assert by_id["derived"].status in ("stale", "unknown")


def test_compute_state_summary_counts(tmp_path: Path) -> None:
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    snapshot = compute_state_snapshot_sync(preql, "duck_db", None, tmp_path)

    s = snapshot.summary
    assert s.total == s.stale + s.fresh + s.unknown
    assert s.managed <= s.total


def test_compute_state_records_column_bindings(tmp_path: Path) -> None:
    """The snapshot carries physical column -> logical concept bindings, which
    the old serve-local shape had no room for."""
    preql = tmp_path / "test.preql"
    preql.write_text(INCREMENTAL_PREQL)
    snapshot = compute_state_snapshot_sync(preql, "duck_db", None, tmp_path)

    columns = _datasources(snapshot)["derived"].columns
    assert {c.column for c in columns} == {"id", "version"}
    assert all(c.concept_address for c in columns)
