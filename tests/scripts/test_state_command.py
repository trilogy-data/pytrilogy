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

from trilogy.execution.state import StateSnapshot, query_digest
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

SINGLE_FILE_PREQL_ROOT_SQL = (
    "\nSELECT 1 as item_id, TIMESTAMP '2024-01-10 12:00:00' as updated_at\n"
)

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
    """Find the asset whose key ends with the suffix (project-local files are
    keyed by project-relative path; external addresses pass through verbatim;
    procedures carry a ``script::``/``query::`` label)."""
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
    # The managed file asset is keyed by its project-relative path — stable
    # across runs and checkouts, unlike the absolute path.
    target = _asset(snapshot, "target_events.parquet")
    assert target.address == "target_events.parquet"
    assert target.managed is True  # trilogy owns refresh for the derived asset
    # The owning script is attribute data, never part of the physical key.
    assert target.owner_script == "base.preql"
    assert target.status == "fresh"
    # The root file source is still an asset — trilogy just doesn't refresh it.
    # Unmanaged means managed=False, never an omission.
    root = _asset(snapshot, "src_events.parquet")
    assert root.address == "src_events.parquet"
    assert root.managed is False
    assert root.owner_script is None  # nothing in the project builds it
    assert root.datasources[0].is_root is True

    # Datasources carry physical column -> logical concept mappings.
    ds = target.datasources[0]
    assert ds.datasource_id == "target_events"
    assert ds.is_root is False
    assert ds.script == "base.preql"  # project-relative, not checkout-absolute
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


def test_refresh_state_file_phased_observations(runner, tmp_path):
    """A refresh records begin (state as found), the plan's verdict, and end
    (state left behind) — observations a reader can derive its own staleness
    tag from, instead of trusting a baked point-in-time boolean."""
    src, _ = _workspace(tmp_path)
    _make_root_ahead(src)

    state_file = tmp_path / "post_refresh.json"
    result = runner.invoke(
        cli,
        ["refresh", str(tmp_path), "duckdb", "--state-file", str(state_file)],
    )
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(state_file)
    target = _asset(snapshot, "target_events.parquet")
    ds = target.datasources[0]

    assert [o.phase for o in ds.observations] == ["begin", "end"]
    begin, end = ds.observations
    begin_values = {w.key: w.value for w in begin.observed_watermarks}
    end_values = {w.key: w.value for w in end.observed_watermarks}
    # Found behind (the pre-refresh probe), left refreshed.
    assert begin_values["ev_ts"] and "2024-01-15" in begin_values["ev_ts"]
    assert end_values["ev_ts"] and "2024-02-01" in end_values["ev_ts"]
    assert begin.probed_at and end.probed_at
    # The end phase carries the freshest expected values the run attests to.
    assert {w.key for w in end.expected_watermarks} == {"ev_ts"}
    assert all(w.probed_at for w in end.observed_watermarks)

    # Both phases carry BOTH sides, so the reader can re-derive each verdict
    # without trusting ds.plan: behind at begin, caught up at end.
    begin_expected = {w.key: w.value for w in begin.expected_watermarks}
    end_expected = {w.key: w.value for w in end.expected_watermarks}
    assert begin_expected["ev_ts"] and "2024-02-01" in begin_expected["ev_ts"]
    assert begin_values["ev_ts"] < begin_expected["ev_ts"]  # stale, derived
    assert end_values["ev_ts"] == end_expected["ev_ts"]  # fresh, derived

    # The plan's verdict rides as an auditable input, not a trusted class.
    assert ds.plan is not None
    assert ds.plan.judged_stale is True
    assert ds.plan.kind == "sql"
    assert ds.plan.forced is False
    assert ds.plan.reason and "behind" in ds.plan.reason


def test_state_op_emits_end_only_observation(runner, tmp_path):
    """Non-refresh operations have no plan: a single end-phase observation."""
    _workspace(tmp_path)
    snap = tmp_path / "snap.json"
    result = runner.invoke(
        cli, ["state", str(tmp_path), "duckdb", "--output", str(snap)]
    )
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    target = _asset(snapshot, "target_events.parquet")
    ds = target.datasources[0]
    assert [o.phase for o in ds.observations] == ["end"]
    assert ds.plan is None


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


PY_SOURCE_PREQL = """key item_id int;
property item_id.value string;

root datasource raw_items (
    item_id: item_id,
    value: value
)
grain (item_id)
file `./ingest/load.py`;
"""


def test_python_source_datasource_is_tracked_and_labeled(runner, tmp_path):
    """A Python datasource script is real state to report — it is a procedure
    that produces rows. It is keyed by its project-relative path with a
    ``script::`` label, so a reader can tell a procedure from a data file."""
    (tmp_path / "ingest").mkdir()
    (tmp_path / "ingest" / "load.py").write_text("# emits arrow\n", encoding="utf-8")
    script = tmp_path / "model.preql"
    script.write_text(PY_SOURCE_PREQL, encoding="utf-8")
    snap = tmp_path / "snap.json"

    result = runner.invoke(cli, ["state", str(script), "duckdb", "--output", str(snap)])
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    asset = _asset(snapshot, "load.py")
    assert asset.address == "script::ingest/load.py"
    assert str(tmp_path) not in asset.address  # portable across checkouts
    assert asset.datasources[0].is_root is True
    # Unmanaged: trilogy reports its state but does not drive it.
    assert asset.managed is False
    assert asset.owner_script is None


def test_two_datasources_over_one_script_are_one_asset(runner, tmp_path):
    """Identity is physical. Two datasource declarations reading the same
    script are one asset, however many logical views exist."""
    (tmp_path / "ingest").mkdir()
    (tmp_path / "ingest" / "load.py").write_text("# emits arrow\n", encoding="utf-8")
    script = tmp_path / "model.preql"
    script.write_text(
        PY_SOURCE_PREQL + """
key other_id int;

root datasource raw_again (
    item_id: other_id
)
grain (other_id)
file `./ingest/load.py`;
""",
        encoding="utf-8",
    )
    snap = tmp_path / "snap.json"

    result = runner.invoke(cli, ["state", str(script), "duckdb", "--output", str(snap)])
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    matching = [a for a in snapshot.assets if a.address == "script::ingest/load.py"]
    assert len(matching) == 1, [a.address for a in snapshot.assets]
    assert {d.datasource_id for d in matching[0].datasources} == {
        "raw_items",
        "raw_again",
    }


def test_inline_query_root_is_labeled_by_digest(runner, tmp_path):
    """A root whose address is inline SQL has no table or file of its own. Its
    key is a short label + digest, never the raw multi-line SQL — which is
    unusable as an identity and churns whenever the query is reformatted."""
    script = tmp_path / "single.preql"
    script.write_text(SINGLE_FILE_PREQL, encoding="utf-8")
    snap = tmp_path / "snap.json"
    result = runner.invoke(cli, ["state", str(script), "duckdb", "--output", str(snap)])
    assert result.exit_code == 0, result.output

    snapshot = _load_snapshot(snap)
    query_assets = [a for a in snapshot.assets if a.address.startswith("query::")]
    assert len(query_assets) == 1, [a.address for a in snapshot.assets]
    root = query_assets[0]
    assert root.address == f"query::{query_digest(SINGLE_FILE_PREQL_ROOT_SQL)}"
    assert "SELECT" not in root.address
    # Present but unmanaged: trilogy reports its state, never refreshes it.
    assert root.managed is False
    assert root.datasources[0].is_root is True
    assert root.datasources[0].datasource_id == "source_items"


def test_reformatting_a_query_root_keeps_its_key(runner, tmp_path):
    """The digest is whitespace-normalized, so reindenting a query does not
    orphan the state an orchestrator recorded against it."""

    def key_for(source: str, name: str) -> str:
        script = tmp_path / name
        script.write_text(source, encoding="utf-8")
        snap = tmp_path / f"{name}.json"
        result = runner.invoke(
            cli, ["state", str(script), "duckdb", "--output", str(snap)]
        )
        assert result.exit_code == 0, result.output
        assets = _load_snapshot(snap).assets
        return next(a.address for a in assets if a.address.startswith("query::"))

    reindented = SINGLE_FILE_PREQL.replace(
        "SELECT 1 as item_id", "    SELECT   1 as item_id"
    )
    assert reindented != SINGLE_FILE_PREQL
    assert key_for(SINGLE_FILE_PREQL, "a.preql") == key_for(reindented, "b.preql")
