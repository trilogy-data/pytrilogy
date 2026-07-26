"""Tests for the ``trilogy state`` command and ``--state-file`` snapshots.

The state snapshot (``trilogy/execution/state/snapshot.py``) is the
machine-facing state contract: assets keyed by physical address, per-datasource
watermarks (observed vs expected), staleness reasons, and physical column ->
logical concept mappings. ``trilogy state`` computes it read-only;
``run``/``refresh --state-file`` write the same snapshot post-execution.

The directory workspace uses file-backed (parquet) datasources — the
managed-asset shape — so refresh writes survive across CLI invocations.
"""

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.execution.state import StateSnapshot
from trilogy.scripts.trilogy import cli

SOURCE_PREQL = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{src}`;
"""

TARGET_PREQL = """import source;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""

SINGLE_FILE_PREQL = """key item_id int;
property item_id.updated_at datetime;

root datasource source_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
query '''
SELECT 1 as item_id, TIMESTAMP '2024-01-10 12:00:00' as updated_at
''';

datasource target_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
address target_items_table
incremental by updated_at;
"""


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _workspace(tmp_path: Path, build_target: bool = True) -> tuple[Path, Path]:
    """Root parquet source + derived incremental parquet persist, so real
    watermarks exist on both sides. Returns (src_parquet, target_parquet)."""
    src = tmp_path / "src_events.parquet"
    target = tmp_path / "target_events.parquet"
    con = duckdb.connect()
    con.execute(f"""COPY (
            SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts
            UNION ALL
            SELECT 2, TIMESTAMP '2024-01-15 12:00:00'
        ) TO '{src.as_posix()}' (FORMAT PARQUET)""")
    if build_target:
        # Derived asset matches the root exactly -> fresh.
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{src.as_posix()}')) "
            f"TO '{target.as_posix()}' (FORMAT PARQUET)"
        )
    con.close()
    (tmp_path / "source.preql").write_text(
        SOURCE_PREQL.format(src=src.as_posix()), encoding="utf-8"
    )
    (tmp_path / "base.preql").write_text(
        TARGET_PREQL.format(target=target.as_posix()), encoding="utf-8"
    )
    return src, target


def _make_root_ahead(src: Path) -> None:
    """Advance the root source past the derived asset's watermark."""
    con = duckdb.connect()
    con.execute(f"""COPY (
            SELECT * FROM read_parquet('{src.as_posix()}')
            UNION ALL
            SELECT 3, TIMESTAMP '2024-02-01 12:00:00'
        ) TO '{src.as_posix()}' (FORMAT PARQUET)""")
    con.close()


def _load_snapshot(path: Path) -> StateSnapshot:
    return StateSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _asset(snapshot: StateSnapshot, address_suffix: str):
    """Find the asset whose physical address ends with the suffix (addresses
    are absolute file paths for file-backed datasources)."""
    match = [a for a in snapshot.assets if a.address.endswith(address_suffix)]
    assert match, (
        f"no asset address ending {address_suffix}: "
        f"{[a.address for a in snapshot.assets]}"
    )
    return match[0]


def test_state_snapshot_directory(runner, tmp_path):
    _workspace(tmp_path)
    snap = tmp_path / "snap.json"
    report = tmp_path / "r.jsonl"
    result = runner.invoke(
        cli,
        [
            "state",
            str(tmp_path),
            "duckdb",
            "--output",
            str(snap),
            "--report-file",
            str(report),
        ],
    )
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    # Assets are keyed by physical address, not datasource id or script.
    target = _asset(snapshot, "target_events.parquet")
    root = _asset(snapshot, "src_events.parquet")
    assert target.managed is True  # trilogy owns refresh for the derived asset
    assert target.status == "fresh"
    assert root.managed is False

    # Datasources carry physical column -> logical concept mappings.
    ds = target.datasources[0]
    assert ds.datasource_id == "target_events"
    assert ds.is_root is False
    columns = {c.column: c.concept_address for c in ds.columns}
    assert set(columns) == {"ev_id", "ev_ts"}
    for concept_address in columns.values():
        assert concept_address  # non-empty logical binding
    assert any(c.endswith("ev_ts") for c in columns.values())

    assert snapshot.summary.total == len(snapshot.assets)
    assert snapshot.summary.managed >= 1

    # The report records the snapshot write and a terminal summary.
    records = [
        json.loads(line) for line in report.read_text(encoding="utf-8").splitlines()
    ]
    snapshot_records = [r for r in records if r["type"] == "state_snapshot"]
    assert snapshot_records and snapshot_records[0]["path"] == str(snap)
    assert records[-1]["type"] == "summary"
    assert records[-1]["success"] is True


def test_state_stale_detection(runner, tmp_path):
    src, _ = _workspace(tmp_path)
    _make_root_ahead(src)

    snap = tmp_path / "snap.json"
    result = runner.invoke(
        cli, ["state", str(tmp_path), "duckdb", "--output", str(snap)]
    )
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    target = _asset(snapshot, "target_events.parquet")
    assert target.status == "stale"

    ds = target.datasources[0]
    assert ds.status == "stale"
    assert ds.stale_reason and "behind" in ds.stale_reason

    observed = {w.key: w for w in ds.observed_watermarks}
    expected = {w.key: w for w in ds.expected_watermarks}
    assert "ev_ts" in observed
    assert "ev_ts" in expected
    assert observed["ev_ts"].value and "2024-01-15" in observed["ev_ts"].value
    assert expected["ev_ts"].value and "2024-02-01" in expected["ev_ts"].value

    assert snapshot.summary.stale >= 1


def test_snapshot_round_trip(runner, tmp_path):
    _workspace(tmp_path)
    snap = tmp_path / "snap.json"
    result = runner.invoke(
        cli, ["state", str(tmp_path), "duckdb", "--output", str(snap)]
    )
    assert result.exit_code == 0, result.output

    data = json.loads(snap.read_text(encoding="utf-8"))
    model = StateSnapshot.model_validate(data)
    assert model.model_dump(mode="json") == data


def test_refresh_state_file(runner, tmp_path):
    # No derived parquet yet -> stale -> refresh builds it.
    _workspace(tmp_path, build_target=False)
    state_file = tmp_path / "post_refresh.json"
    result = runner.invoke(
        cli,
        ["refresh", str(tmp_path), "duckdb", "--state-file", str(state_file)],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "target_events.parquet").exists()

    snapshot = _load_snapshot(state_file)
    target = _asset(snapshot, "target_events.parquet")
    assert target.status == "fresh"
    assert target.managed is True
    assert snapshot.summary.stale == 0


def test_state_read_only(runner, tmp_path):
    """The state command probes but never writes warehouse state — even when
    assets are stale."""
    src, target = _workspace(tmp_path)
    _make_root_ahead(src)

    def asset_bytes() -> tuple[bytes, bytes]:
        return src.read_bytes(), target.read_bytes()

    before = asset_bytes()
    for _ in range(2):
        result = runner.invoke(cli, ["state", str(tmp_path), "duckdb"])
        assert result.exit_code == 0, result.output
    assert asset_bytes() == before


def test_state_single_file(runner, tmp_path):
    script = tmp_path / "single.preql"
    script.write_text(SINGLE_FILE_PREQL, encoding="utf-8")
    snap = tmp_path / "snap.json"
    result = runner.invoke(cli, ["state", str(script), "duckdb", "--output", str(snap)])
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    target = _asset(snapshot, "target_items_table")
    assert target.managed is True
    ds = target.datasources[0]
    assert ds.datasource_id == "target_items"
    assert {c.column for c in ds.columns} == {"item_id", "updated_at"}
    assert all(c.concept_address for c in ds.columns)
