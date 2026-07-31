"""End-to-end CLI round trip for partitioned state, against a real DuckDB file.

Everything here goes through the actual ``trilogy`` CLI and asserts on the
warehouse afterwards, because the pieces that break in a round trip are the ones
unit tests stub: the state file is written and re-read as JSON, values survive
rendering and restoration, the seeded store is installed by the CLI's own
scoping, and the refresh filter reaches SQL.

The out-of-band DELETE is the whole trick — it makes the warehouse and the
recorded snapshot disagree on purpose, so "did this run probe or trust the
file?" becomes directly observable.
"""

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.execution.state import StateSnapshot
from trilogy.scripts.trilogy import cli

MODEL = """key order_id int;
property order_id.order_date date;
property order_id.region string;
property order_id.updated_at datetime;

root datasource raw_orders (
    order_id: order_id,
    order_date: order_date,
    region: region,
    updated_at: updated_at,
)
grain (order_id)
file `{src}`;

auto order_count <- count(order_id) by order_date, region;
auto max_updated_at <- max(updated_at) by order_date, region;

datasource daily_orders (
    order_date: order_date,
    region: region,
    order_count: order_count,
    max_updated_at: max_updated_at,
)
grain (order_date, region)
address daily_orders
freshness by max_updated_at
partition by order_date
;
"""

# One partition's worth of work — the unit an orchestrator fans out.
BUILD = """import model;

parameter load_date date;

APPEND daily_orders
WHERE order_date = load_date
;
"""

CONFIG = """[engine]
dialect = "duck_db"

[engine.config]
path = "{warehouse}"
"""

ROWS = """
SELECT 1 AS order_id, DATE '2024-01-01' AS order_date, 'north' AS region,
       TIMESTAMP '2024-01-05 06:00:00' AS updated_at
UNION ALL SELECT 2, DATE '2024-01-02', 'north', TIMESTAMP '2024-01-05 06:01:00'
UNION ALL SELECT 3, DATE '2024-01-03', 'south', TIMESTAMP '2024-01-05 06:02:00'
"""

HOLE = "2024-01-02"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    src = tmp_path / "orders.parquet"
    con = duckdb.connect()
    con.execute(f"COPY ({ROWS}) TO '{src.as_posix()}' (FORMAT PARQUET)")
    con.close()

    (tmp_path / "model.preql").write_text(
        MODEL.format(src=src.as_posix()), encoding="utf-8"
    )
    (tmp_path / "build.preql").write_text(BUILD, encoding="utf-8")
    (tmp_path / "trilogy.toml").write_text(
        CONFIG.format(warehouse=(tmp_path / "warehouse.duckdb").as_posix()),
        encoding="utf-8",
    )
    return tmp_path


def _warehouse(workspace: Path):
    return duckdb.connect(str(workspace / "warehouse.duckdb"))


def _slice_counts(workspace: Path) -> dict:
    con = _warehouse(workspace)
    try:
        rows = con.execute(
            "SELECT order_date, count(*) FROM daily_orders GROUP BY 1"
        ).fetchall()
    finally:
        con.close()
    return {str(day): count for day, count in rows}


def _drop_slice(workspace: Path, day: str) -> None:
    con = _warehouse(workspace)
    try:
        con.execute(f"DELETE FROM daily_orders WHERE order_date = DATE '{day}'")
        con.commit()
    finally:
        con.close()


def _snapshot(path: Path) -> StateSnapshot:
    return StateSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _partitions(snapshot: StateSnapshot) -> dict:
    return {
        partition.partition_id: partition.status
        for asset in snapshot.assets
        for ds_state in asset.datasources
        for partition in ds_state.partitions
    }


def _run(runner, *args) -> str:
    result = runner.invoke(cli, list(args))
    assert result.exit_code in (0, 2), result.output
    return result.output


def _build(runner, workspace: Path) -> None:
    _run(runner, "refresh", str(workspace / "model.preql"))


def test_partition_flag_refreshes_only_its_slice(runner, workspace: Path):
    """`--partition` names the slice the run owns; the hole beside it stays a
    hole, because nothing asked for it."""
    _build(runner, workspace)
    _drop_slice(workspace, HOLE)
    _drop_slice(workspace, "2024-01-03")

    _run(
        runner,
        "refresh",
        str(workspace / "model.preql"),
        "--partition",
        "local.order_date=2024-01-03",
    )
    counts = _slice_counts(workspace)
    assert "2024-01-03" in counts, "the named slice was rebuilt"
    assert HOLE not in counts, "the neighbouring hole was not"


def test_partition_flag_implies_state_partition(runner, workspace: Path):
    """The pair cannot drift: a run told to rebuild one slice reports on one
    slice, without the caller having to say so twice."""
    _build(runner, workspace)
    _drop_slice(workspace, HOLE)
    state = workspace / "state.json"

    _run(
        runner,
        "refresh",
        str(workspace / "model.preql"),
        "--partition",
        f"local.order_date={HOLE}",
        "--state-file",
        str(state),
    )
    snapshot = _snapshot(state)
    assert set(_partitions(snapshot)) == {f"order_date={HOLE}"}

    ds_state = next(
        ds
        for asset in snapshot.assets
        for ds in asset.datasources
        if ds.partition_by
    )
    assert ds_state.partitions_complete is False, "it is a mergeable delta"
    # The aggregate survives scoping — otherwise a fan-out where every run is
    # partition-targeted would never report totals at all.
    assert ds_state.partition_summary is not None
    assert ds_state.partition_summary.total == 3
    assert ds_state.partition_summary.reported == 1
    assert ds_state.partition_summary.truncated is True


def test_explicit_state_partition_still_wins(runner, workspace: Path):
    """The implication is a default, not a lock: a caller that genuinely wants
    the two to differ can still say so."""
    _build(runner, workspace)
    state = workspace / "state.json"

    _run(
        runner,
        "refresh",
        str(workspace / "model.preql"),
        "--partition",
        f"local.order_date={HOLE}",
        "--state-partition",
        "order_date=2024-01-03",
        "--state-file",
        str(state),
    )
    assert set(_partitions(_snapshot(state))) == {"order_date=2024-01-03"}


def _build_one_slice(runner, workspace: Path, day: str, state_file: Path) -> str:
    """One per-partition worker: build a slice, publish a delta scoped to it."""
    _run(
        runner,
        "run",
        str(workspace / "build.preql"),
        "--param",
        f"load_date={day}",
        "--state-file",
        str(state_file),
        "--state-partition",
        f"order_date={day}",
    )
    return str(state_file)


def test_snapshot_records_every_slice_after_a_build(runner, workspace):
    state = workspace / "state.json"
    _build(runner, workspace)
    _run(runner, "state", str(workspace / "model.preql"), "-o", str(state))

    snapshot = _snapshot(state)
    assert _partitions(snapshot) == {
        "order_date=2024-01-01": "fresh",
        "order_date=2024-01-02": "fresh",
        "order_date=2024-01-03": "fresh",
    }


def test_live_probe_sees_a_hole_a_table_watermark_cannot(runner, workspace):
    """The dropped day's rows are older than the table MAX, so only per-slice
    state can see it."""
    state = workspace / "state.json"
    _build(runner, workspace)
    _drop_slice(workspace, HOLE)
    _run(runner, "state", str(workspace / "model.preql"), "-o", str(state))

    snapshot = _snapshot(state)
    assert _partitions(snapshot)[f"order_date={HOLE}"] == "stale"
    assert snapshot.summary.stale == 1


def test_refresh_fills_the_hole_and_spares_its_neighbours(runner, workspace):
    _build(runner, workspace)
    # A marker row on a healthy slice: it does not move that slice's watermark,
    # so it survives only if the refresh genuinely leaves the slice alone.
    con = _warehouse(workspace)
    con.execute(
        "INSERT INTO daily_orders VALUES "
        "(DATE '2024-01-01', 'marker', 99, TIMESTAMP '2024-01-05 06:00:00')"
    )
    con.commit()
    con.close()
    _drop_slice(workspace, HOLE)
    assert _slice_counts(workspace) == {"2024-01-01": 2, "2024-01-03": 1}

    output = _run(runner, "refresh", str(workspace / "model.preql"))
    assert f"stale partition(s): order_date={HOLE}" in output
    assert _slice_counts(workspace) == {
        "2024-01-01": 2,  # marker survived — untouched
        "2024-01-02": 1,  # refilled
        "2024-01-03": 1,
    }


def test_state_input_is_trusted_over_the_warehouse(runner, workspace):
    """A complete snapshot seeds slices exactly as it seeds watermarks: the run
    trusts the file and does not re-probe, even though the warehouse has since
    lost a partition."""
    state = workspace / "state.json"
    _build(runner, workspace)
    _run(runner, "state", str(workspace / "model.preql"), "-o", str(state))
    _drop_slice(workspace, HOLE)

    output = _run(
        runner,
        "refresh",
        str(workspace / "model.preql"),
        "--state-input",
        str(state),
    )
    assert "No stale assets found" in output
    assert f"order_date={HOLE}" not in output
    # The hole is still there: the run believed the snapshot, as instructed.
    assert HOLE not in _slice_counts(workspace)


def test_a_partition_scoped_delta_is_not_trusted_for_seeding(runner, workspace):
    """A scoped delta speaks for only its own slices, so seeding from it would
    report every other slice as absent. It falls back to a live probe instead."""
    delta = workspace / "delta.json"
    _build(runner, workspace)
    _build_one_slice(runner, workspace, "2024-01-01", delta)
    recorded = _snapshot(delta)
    assert any(
        ds_state.partitions_complete is False
        for asset in recorded.assets
        for ds_state in asset.datasources
        if ds_state.partition_by
    )

    _drop_slice(workspace, HOLE)
    output = _run(
        runner,
        "refresh",
        str(workspace / "model.preql"),
        "--state-input",
        str(delta),
    )
    assert f"stale partition(s): order_date={HOLE}" in output
    assert _slice_counts(workspace)[HOLE] == 1


def test_merge_reassembles_a_fanned_out_build(runner, workspace):
    """The orchestrator loop end to end: per-partition runs write scoped deltas,
    `state-merge` folds them into one file that matches a whole-asset probe."""
    _build(runner, workspace)
    deltas = [
        _build_one_slice(runner, workspace, day, workspace / f"delta_{day}.json")
        for day in ("2024-01-01", "2024-01-02", "2024-01-03")
    ]

    base = workspace / "base.json"
    merged = workspace / "merged.json"
    _run(runner, "state", str(workspace / "model.preql"), "-o", str(base))
    _run(runner, "state-merge", str(base), *deltas, "-o", str(merged))

    assert _partitions(_snapshot(merged)) == _partitions(_snapshot(base))
    assert _snapshot(merged).summary.stale == 0
