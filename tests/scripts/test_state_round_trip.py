"""End-to-end validation of the persisted state round trip.

One invocation refreshes and writes ``--state-file``; a *later* invocation over
a *different model* — different concept and datasource names, same physical
addresses — consumes it with ``--state-input`` and plans off the recorded
observations instead of re-probing. This is the orchestrator contract: the
state lives outside the process, and physical address is the join key.
"""

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.execution.state import StateSnapshot
from trilogy.scripts.trilogy import cli

WRITER_SOURCE = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{src}`;
"""

WRITER_TARGET = """import source;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""

# A different model over the same two files: renamed concepts, renamed
# datasources, different script layout. Only the physical addresses match.
READER_MODEL = """key event_key int;
property event_key.event_time datetime;

root datasource upstream_feed (
    ev_id: event_key,
    ev_ts: event_time
)
grain (event_key)
file `{src}`;

datasource warehouse_events (
    ev_id: event_key,
    ev_ts: event_time
)
grain (event_key)
file `{target}`
incremental by event_time;
"""


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_src(src: Path, rows: str) -> None:
    con = duckdb.connect()
    con.execute(f"COPY ({rows}) TO '{src.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _copy_parquet(src: Path, target: Path) -> None:
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src.as_posix()}')) "
        f"TO '{target.as_posix()}' (FORMAT PARQUET)"
    )
    con.close()


def _writer_workspace(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Model A (the writer) plus its root parquet. Target is not built yet, so
    the first refresh has real work to do."""
    workspace = tmp_path / "writer"
    workspace.mkdir()
    src = tmp_path / "src_events.parquet"
    target = tmp_path / "target_events.parquet"
    _write_src(
        src,
        "SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts "
        "UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'",
    )
    (workspace / "source.preql").write_text(
        WRITER_SOURCE.format(src=src.as_posix()), encoding="utf-8"
    )
    (workspace / "base.preql").write_text(
        WRITER_TARGET.format(target=target.as_posix()), encoding="utf-8"
    )
    return workspace, src, target


def _reader_workspace(tmp_path: Path, src: Path, target: Path) -> Path:
    workspace = tmp_path / "reader"
    workspace.mkdir()
    (workspace / "model.preql").write_text(
        READER_MODEL.format(src=src.as_posix(), target=target.as_posix()),
        encoding="utf-8",
    )
    return workspace


def _snapshot(path: Path) -> StateSnapshot:
    return StateSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def test_state_round_trip_across_models(runner, tmp_path):
    workspace, src, target = _writer_workspace(tmp_path)
    state_file = tmp_path / "state.json"

    # --- Run 1: refresh model A, persisting the resulting state ---------------
    first = runner.invoke(
        cli, ["refresh", str(workspace), "duckdb", "--state-file", str(state_file)]
    )
    assert first.exit_code == 0, first.output
    assert target.exists()

    snapshot = _snapshot(state_file)
    recorded = next(
        a for a in snapshot.assets if a.address.endswith("target_events.parquet")
    )
    assert recorded.status == "fresh"
    assert recorded.managed is True
    observed = {w.key: w.value for w in recorded.datasources[0].observed_watermarks}
    assert observed["local.ev_ts"] and "2024-01-15" in observed["local.ev_ts"]

    # --- Drift: the root advances, and the target is brought up to date out of
    # band. A live probe would now call the target fresh; the recorded state
    # (2024-01-15) says otherwise. -------------------------------------------
    _write_src(
        src,
        "SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts "
        "UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00' "
        "UNION ALL SELECT 3, TIMESTAMP '2024-02-01 12:00:00'",
    )
    _copy_parquet(src, target)

    reader = _reader_workspace(tmp_path, src, target)

    # --- Control: model B without the state file probes live and sees nothing
    # to do (exit 2 is refresh's "everything up to date"). --------------------
    control = runner.invoke(cli, ["refresh", str(reader), "duckdb"])
    assert control.exit_code == 2, control.output

    # --- Run 2: same model B, now seeded from model A's state file. The
    # recorded watermark is behind the root, so the asset refreshes. ----------
    second_state = tmp_path / "state2.json"
    second = runner.invoke(
        cli,
        [
            "refresh",
            str(reader),
            "duckdb",
            "--state-input",
            str(state_file),
            "--state-file",
            str(second_state),
        ],
    )
    assert second.exit_code == 0, second.output
    assert "Seeding asset state from" in second.output
    # Seeding matched by physical address onto model B's own datasource name.
    assert "warehouse_events" in second.output

    # The state written back out reflects the post-refresh truth.
    after = _snapshot(second_state)
    refreshed = next(
        a for a in after.assets if a.address.endswith("target_events.parquet")
    )
    assert refreshed.status == "fresh"
    assert after.summary.stale == 0
    # Model B records under ITS concept address; the seeded key was re-keyed
    # onto it through the shared physical column.
    reobserved = {w.key: w.value for w in refreshed.datasources[0].observed_watermarks}
    assert (
        reobserved["local.event_time"]
        and "2024-02-01" in reobserved["local.event_time"]
    )


PORTABLE_SOURCE = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{src}`;
"""

PORTABLE_TARGET = """import source;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""


def _checkout(root: Path, rows: str) -> tuple[Path, Path]:
    """A self-contained project: scripts at the root, data under ``data/``.
    Every absolute path differs between two checkouts; every key must not."""
    root.mkdir(parents=True)
    (root / "data").mkdir()
    src = root / "data" / "src_events.parquet"
    target = root / "data" / "target_events.parquet"
    _write_src(src, rows)
    (root / "source.preql").write_text(
        PORTABLE_SOURCE.format(src=src.as_posix()), encoding="utf-8"
    )
    (root / "base.preql").write_text(
        PORTABLE_TARGET.format(target=target.as_posix()), encoding="utf-8"
    )
    return src, target


BASE_ROWS = (
    "SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts "
    "UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'"
)
AHEAD_ROWS = BASE_ROWS + " UNION ALL SELECT 3, TIMESTAMP '2024-02-01 12:00:00'"


def test_state_survives_a_different_checkout(runner, tmp_path):
    """The point of stable keys: state recorded in one checkout seeds a run in
    another, where every absolute path is different.

    This is what an orchestrator does — each run materializes the project into
    a fresh scratch directory — so a key containing the checkout path would
    match nothing and silently degrade to a live probe.
    """
    a = tmp_path / "checkout_a" / "proj"
    _checkout(a, BASE_ROWS)
    state_file = tmp_path / "state.json"

    first = runner.invoke(
        cli, ["refresh", str(a), "duckdb", "--state-file", str(state_file)]
    )
    assert first.exit_code == 0, first.output

    snapshot = _snapshot(state_file)
    keys = {asset.address for asset in snapshot.assets}
    # Purely physical, project-relative pointers. Neither mentions the checkout.
    assert keys == {"data/target_events.parquet", "data/src_events.parquet"}
    owners = {a.address: a.owner_script for a in snapshot.assets}
    assert owners["data/target_events.parquet"] == "base.preql"
    assert owners["data/src_events.parquet"] is None  # no script builds it

    # --- checkout B: same project, different paths. Its root is already ahead
    # and its target was built out of band, so a live probe sees nothing to do.
    b = tmp_path / "checkout_b" / "proj"
    src_b, target_b = _checkout(b, AHEAD_ROWS)
    _copy_parquet(src_b, target_b)

    control = runner.invoke(cli, ["refresh", str(b), "duckdb"])
    assert control.exit_code == 2, control.output  # 2 == everything up to date

    # Seeded with checkout A's state, the recorded watermark (2024-01-15) is
    # behind B's root (2024-02-01), so the asset refreshes.
    seeded = runner.invoke(
        cli, ["refresh", str(b), "duckdb", "--state-input", str(state_file)]
    )
    assert seeded.exit_code == 0, seeded.output
    assert "Seeding asset state from" in seeded.output
    assert "target_events" in seeded.output


def _nested_checkout(root: Path, rows: str) -> tuple[Path, Path]:
    """Scripts under ``models/``, data under ``data/``, ``trilogy.toml`` at the
    root. Keys must anchor on the config's directory — relativizing against the
    script's own directory cannot express ``../data/...`` and would silently
    fall back to absolute, unportable paths."""
    (root / "models").mkdir(parents=True)
    (root / "data").mkdir(parents=True)
    (root / "trilogy.toml").write_text(
        "[engine]\ndialect = 'duck_db'\n", encoding="utf-8"
    )
    src = root / "data" / "src_events.parquet"
    target = root / "data" / "target_events.parquet"
    _write_src(src, rows)
    (root / "models" / "source.preql").write_text(
        PORTABLE_SOURCE.format(src=src.as_posix()), encoding="utf-8"
    )
    (root / "models" / "base.preql").write_text(
        PORTABLE_TARGET.format(target=target.as_posix()), encoding="utf-8"
    )
    return src, target


def test_subdirectory_scripts_key_against_the_config_root(runner, tmp_path):
    """``trilogy.toml`` is the project root, so a script in a subdirectory keys
    its assets exactly like one at the top — and the snapshot stays portable."""
    a = tmp_path / "checkout_a" / "proj"
    _nested_checkout(a, BASE_ROWS)
    state_file = tmp_path / "state.json"

    first = runner.invoke(
        cli,
        ["refresh", str(a / "models"), "duckdb", "--state-file", str(state_file)],
    )
    assert first.exit_code == 0, first.output

    snapshot = _snapshot(state_file)
    assert {asset.address for asset in snapshot.assets} == {
        "data/target_events.parquet",
        "data/src_events.parquet",
    }
    owners = {a.address: a.owner_script for a in snapshot.assets}
    assert owners["data/target_events.parquet"] == "models/base.preql"
    # The defining script is recorded project-relative too, not as a basename.
    scripts = {ds.script for asset in snapshot.assets for ds in asset.datasources}
    assert scripts == {"models/base.preql"}

    b = tmp_path / "checkout_b" / "proj"
    src_b, target_b = _nested_checkout(b, AHEAD_ROWS)
    _copy_parquet(src_b, target_b)

    control = runner.invoke(cli, ["refresh", str(b / "models"), "duckdb"])
    assert control.exit_code == 2, control.output

    seeded = runner.invoke(
        cli,
        ["refresh", str(b / "models"), "duckdb", "--state-input", str(state_file)],
    )
    assert seeded.exit_code == 0, seeded.output
    assert "Seeding asset state from" in seeded.output
    assert "target_events" in seeded.output


def test_state_input_from_environment(runner, tmp_path, monkeypatch):
    """TRILOGY_STATE_INPUT / TRILOGY_STATE_FILE are the flag-free path an
    orchestrator uses when it controls the process environment."""
    workspace, src, target = _writer_workspace(tmp_path)
    state_file = tmp_path / "state.json"
    monkeypatch.setenv("TRILOGY_STATE_FILE", str(state_file))

    first = runner.invoke(cli, ["refresh", str(workspace), "duckdb"])
    assert first.exit_code == 0, first.output
    assert state_file.exists()

    _write_src(
        src,
        "SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts "
        "UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00' "
        "UNION ALL SELECT 3, TIMESTAMP '2024-02-01 12:00:00'",
    )
    _copy_parquet(src, target)

    monkeypatch.delenv("TRILOGY_STATE_FILE")
    monkeypatch.setenv("TRILOGY_STATE_INPUT", str(state_file))
    second = runner.invoke(cli, ["refresh", str(workspace), "duckdb"])
    assert second.exit_code == 0, second.output
    assert "Seeding asset state from" in second.output


def test_missing_state_input_is_a_usage_error(runner, tmp_path):
    workspace, _, _ = _writer_workspace(tmp_path)
    result = runner.invoke(
        cli,
        [
            "refresh",
            str(workspace),
            "duckdb",
            "--state-input",
            str(tmp_path / "nope.json"),
        ],
    )
    assert result.exit_code == 2
    assert "does not exist" in result.output
