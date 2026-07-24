from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.execution.state import (
    BaseStateStore,
    RefreshAssetError,
    RefreshKind,
    refresh_stale_assets,
    run_refresh_script,
)


def _executor_with_refreshable_root(probe_path: str, refresh_path: str):
    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.value string;

        root datasource raw (id: id, value: value)
        grain (id)
        address raw_table;

        datasource derived (id: id, value: value)
        grain (id)
        address derived_table
        freshness by id;
        """)
    e.execute_raw_sql("CREATE TABLE raw_table (id INTEGER, value VARCHAR)")
    e.execute_raw_sql("INSERT INTO raw_table VALUES (1, 'a'), (2, 'b')")

    raw = e.environment.datasources["raw"]
    e.environment.datasources["raw"] = raw.model_copy(
        update={"freshness_probe": probe_path, "refresh_script": refresh_path}
    )
    return e


def test_refreshable_root_probe_fresh_no_script():
    """Probe returns true → script does NOT run, no stale asset emitted."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=True
    ) as mock_probe, patch(
        "trilogy.execution.state.state_store.run_refresh_script"
    ) as mock_refresh:
        store = BaseStateStore()
        stale = store.get_stale_assets(e.environment, e)

    mock_probe.assert_called_once_with("/fake/probe.py")
    mock_refresh.assert_not_called()
    script_stale = [a for a in stale if a.kind == RefreshKind.SCRIPT]
    assert script_stale == []


def test_refreshable_root_probe_stale_emits_script_kind():
    """Probe returns false → StaleAsset with kind=SCRIPT is emitted."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ):
        store = BaseStateStore()
        stale = store.get_stale_assets(e.environment, e)

    script_stale = [a for a in stale if a.kind == RefreshKind.SCRIPT]
    assert len(script_stale) == 1
    assert script_stale[0].datasource_id == "raw"
    assert "refreshable root probe" in script_stale[0].reason


def test_root_with_only_probe_no_script_is_still_skipped():
    """A root with freshness_probe but no refresh_script remains untouchable."""
    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key rid int;

        root datasource r (rid: rid)
        grain (rid)
        query '''SELECT 1 as rid''';
        """)
    ds = e.environment.datasources["r"]
    e.environment.datasources["r"] = ds.model_copy(
        update={"freshness_probe": "/fake/p.py"}
    )

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ) as mock_probe:
        store = BaseStateStore()
        stale = store.get_stale_assets(e.environment, e)

    # No refresh_script → root remains skipped, probe is never called for it
    mock_probe.assert_not_called()
    assert len(stale) == 0


def test_refresh_runs_script_when_probe_stale():
    """End-to-end: stale probe triggers run_refresh_script call."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    probe_calls = {"n": 0}

    def probe_side_effect(_path):
        probe_calls["n"] += 1
        return probe_calls["n"] > 1  # first call (initial) False, rest True

    refreshed: list[tuple[str, str]] = []

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe",
        side_effect=probe_side_effect,
    ), patch("trilogy.execution.state.state_store.run_refresh_script") as mock_refresh:
        result = refresh_stale_assets(
            e, on_refresh=lambda i, r: refreshed.append((i, r))
        )

    mock_refresh.assert_called_once()
    args, kwargs = mock_refresh.call_args
    assert args[0] == "/fake/refresh.py"
    assert "cwd" in kwargs
    assert result.refreshed_count >= 1
    assert any(rid == "raw" for rid, _ in refreshed)


def test_refresh_hard_errors_when_probe_still_false_after_script():
    """Script exits 0 but probe still says stale → RefreshAssetError."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ), patch("trilogy.execution.state.state_store.run_refresh_script"), pytest.raises(RefreshAssetError) as exc_info:
        refresh_stale_assets(e)

    assert "still returned false" in str(exc_info.value)
    assert "/fake/refresh.py" in str(exc_info.value)


def test_refresh_propagates_script_error():
    """Script raises → RefreshAssetError wraps it with the asset id."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ), patch(
        "trilogy.execution.state.state_store.run_refresh_script",
        side_effect=RuntimeError("bad script"),
    ), pytest.raises(RefreshAssetError) as exc_info:
        refresh_stale_assets(e)

    assert exc_info.value.datasource_id == "raw"
    assert "bad script" in str(exc_info.value)


def test_refresh_dry_run_does_not_invoke_script():
    """Dry-run: probe is checked, but refresh_script is not invoked."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ), patch("trilogy.execution.state.state_store.run_refresh_script") as mock_refresh:
        result = refresh_stale_assets(e, dry_run=True)

    mock_refresh.assert_not_called()
    assert result.stale_count >= 1


def test_refresh_dry_run_emits_dry_run_marker_to_query_callback():
    """on_refresh_query receives a # dry-run line for script-kind assets."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")
    seen: list[tuple[str, str]] = []

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=False
    ), patch("trilogy.execution.state.state_store.run_refresh_script"):
        refresh_stale_assets(
            e,
            dry_run=True,
            on_refresh_query=lambda i, q: seen.append((i, q)),
        )

    script_lines = [q for i, q in seen if i == "raw"]
    assert script_lines, "expected on_refresh_query for script-kind asset"
    assert "/fake/refresh.py" in script_lines[0]
    assert "dry-run" in script_lines[0]


def test_refresh_forced_runs_script_even_when_probe_fresh():
    """force_sources should run the refresh script for refreshable roots."""
    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=True
    ), patch("trilogy.execution.state.state_store.run_refresh_script") as mock_refresh:
        result = refresh_stale_assets(e, force_sources={"raw"})

    mock_refresh.assert_called_once()
    assert result.refreshed_count >= 1


def test_cross_script_cascade_dependent_refreshes():
    """Refreshable root stale → script runs → dependent that LOOKED FRESH at
    plan time gets cascade-refreshed because invalidation lets re-eval see the
    post-refresh watermark.

    Setup: D's watermark equals R's pre-refresh watermark. At plan time D looks
    fresh. The refresh script "bumps" R's data (we simulate via a side_effect
    that mutates the DB). Cascade phase re-evaluates D — now stale → refresh.
    """
    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.v int;

        root datasource r (id: id, v: v) grain (id) address r_table;

        datasource d (id: id, v: v) grain (id) address d_table
            incremental by v;
        """)
    e.execute_raw_sql("CREATE TABLE r_table (id INTEGER, v INTEGER)")
    e.execute_raw_sql("INSERT INTO r_table VALUES (1, 100)")
    e.execute_raw_sql("CREATE TABLE d_table (id INTEGER, v INTEGER)")
    e.execute_raw_sql("INSERT INTO d_table VALUES (1, 100)")  # D matches R initially

    raw = e.environment.datasources["r"]
    e.environment.datasources["r"] = raw.model_copy(
        update={
            "freshness_probe": "/fake/probe.py",
            "refresh_script": "/fake/refresh.py",
        }
    )

    def fake_refresh(_path, cwd=None):
        # Simulate the script bumping R's value past D's.
        e.execute_raw_sql("UPDATE r_table SET v = 200 WHERE id = 1")

    probe_calls = {"n": 0}

    def fake_probe(_path):
        probe_calls["n"] += 1
        # Initial probe: false (R is stale). Post-refresh probe: true.
        return probe_calls["n"] > 1

    refreshed: list[str] = []
    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe",
        side_effect=fake_probe,
    ), patch(
        "trilogy.execution.state.state_store.run_refresh_script",
        side_effect=fake_refresh,
    ):
        result = refresh_stale_assets(e, on_refresh=lambda i, _r: refreshed.append(i))

    assert "r" in refreshed, f"expected R to refresh, got {refreshed}"
    assert "d" in refreshed, (
        f"expected D to cascade-refresh after R bumped its watermark, "
        f"got {refreshed} (count={result.refreshed_count})"
    )
    assert result.refreshed_count == 2


def test_probe_memoization_same_path_called_once():
    """Same probe path used by 2 datasources → subprocess called once."""
    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.v int;

        root datasource r1 (id: id, v: v) grain (id) address r1_table;
        root datasource r2 (id: id, v: v) grain (id) address r2_table;

        datasource d (id: id, v: v) grain (id) address d_table
            incremental by v;
        """)
    e.execute_raw_sql("CREATE TABLE r1_table (id INTEGER, v INTEGER)")
    e.execute_raw_sql("CREATE TABLE r2_table (id INTEGER, v INTEGER)")
    e.execute_raw_sql("CREATE TABLE d_table (id INTEGER, v INTEGER)")

    for name in ("r1", "r2"):
        ds = e.environment.datasources[name]
        e.environment.datasources[name] = ds.model_copy(
            update={
                "freshness_probe": "/fake/shared_probe.py",
                "refresh_script": f"/fake/{name}_refresh.py",
            }
        )

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=True
    ) as mock_probe:
        store = BaseStateStore()
        store.get_stale_assets(e.environment, e)

    # Both r1 and r2 share '/fake/shared_probe.py' → only one subprocess call.
    paths_called = [c.args[0] for c in mock_probe.call_args_list]
    assert paths_called.count("/fake/shared_probe.py") == 1


def test_directory_mode_cascade_unknown_node_refreshes():
    """Directory-mode equivalent of cross-script cascade: a managed node with
    empty `assets` (an "unknown" node downstream of a refreshable root) must
    re-evaluate at execute time and refresh if it's now stale.

    Simulates: at preview, D probed fresh against R's pre-refresh watermark, so
    D's ManagedRefreshNode has assets=[]. After R's script ran (bumping R's
    data via the test's mock), D's node executes via execute_managed_node_for_refresh,
    which calls is_stale(D) — sees D is now behind R's new watermark → refreshes.
    """
    from trilogy.scripts.dependency import ManagedRefreshNode, ScriptNode
    from trilogy.scripts.refresh import execute_managed_node_for_refresh

    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.v int;

        root datasource r (id: id, v: v) grain (id) address r_table;

        datasource d (id: id, v: v) grain (id) address d_table
            incremental by v;
        """)
    e.execute_raw_sql("CREATE TABLE r_table (id INTEGER, v INTEGER)")
    # R now has v=200 (post-refresh state, simulating that R's script already ran)
    e.execute_raw_sql("INSERT INTO r_table VALUES (1, 200)")
    e.execute_raw_sql("CREATE TABLE d_table (id INTEGER, v INTEGER)")
    # D still at v=100 — was fresh when probed against pre-refresh R, now stale.
    e.execute_raw_sql("INSERT INTO d_table VALUES (1, 100)")

    fake_owner = ScriptNode(path=__file__)
    # Empty `assets`: no pre-classified staleness. This is the "unknown" case.
    d_node = ManagedRefreshNode(
        address="d_table",
        owner_script=fake_owner,
        assets=[],
        datasource_ids=("d",),
    )

    stats = execute_managed_node_for_refresh(
        e,
        d_node,
        quiet=True,
        print_watermarks=False,
        interactive=False,
        dry_run=False,
    )

    # Deferred eval found D stale (v=100 < R's v=200), refreshed it.
    assert (
        stats.update_count == 1
    ), f"expected D's unknown node to deferred-refresh, got update_count={stats.update_count}"


def test_directory_mode_cascade_fresh_dependent_skips():
    """Inverse of the cascade test: an 'unknown' node that's actually fresh at
    execute time should skip cleanly (update_count==0), proving deferred eval
    correctly distinguishes 'unknown' from 'stale'.
    """
    from trilogy.scripts.dependency import ManagedRefreshNode, ScriptNode
    from trilogy.scripts.refresh import execute_managed_node_for_refresh

    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.v int;

        root datasource r (id: id, v: v) grain (id) address r_table;

        datasource d (id: id, v: v) grain (id) address d_table
            incremental by v;
        """)
    e.execute_raw_sql("CREATE TABLE r_table (id INTEGER, v INTEGER)")
    e.execute_raw_sql("INSERT INTO r_table VALUES (1, 200)")
    e.execute_raw_sql("CREATE TABLE d_table (id INTEGER, v INTEGER)")
    # D matches the new R watermark — actually fresh, no cascade needed.
    e.execute_raw_sql("INSERT INTO d_table VALUES (1, 200)")

    fake_owner = ScriptNode(path=__file__)
    d_node = ManagedRefreshNode(
        address="d_table",
        owner_script=fake_owner,
        assets=[],
        datasource_ids=("d",),
    )

    stats = execute_managed_node_for_refresh(
        e,
        d_node,
        quiet=True,
        print_watermarks=False,
        interactive=False,
        dry_run=False,
    )

    assert stats.update_count == 0


def test_invalidate_address_evicts_cache():
    """invalidate_address drops watermark and probe entries for that address."""
    from trilogy.execution.state.watermarks import (
        DatasourceWatermark,
        UpdateKey,
        UpdateKeyType,
    )

    e = _executor_with_refreshable_root("/fake/probe.py", "/fake/refresh.py")
    store = BaseStateStore()
    store.watermarks["raw"] = DatasourceWatermark(
        keys={
            "id": UpdateKey(
                concept_name="id", type=UpdateKeyType.INCREMENTAL_KEY, value=5
            )
        }
    )
    store._probe_results["/fake/probe.py"] = True
    store.concept_max_watermarks["id"] = UpdateKey(
        concept_name="id", type=UpdateKeyType.INCREMENTAL_KEY, value=5
    )

    raw_ds = e.environment.datasources["raw"]
    store.invalidate_address(e.environment, raw_ds.safe_address)

    assert "raw" not in store.watermarks
    assert "/fake/probe.py" not in store._probe_results
    assert store.concept_max_watermarks == {}


# --- run_refresh_script unit tests ---


def test_run_refresh_script_success(tmp_path):
    script = tmp_path / "ok.py"
    script.write_text("print('ran')")
    # Returns None on success (exit 0)
    assert run_refresh_script(str(script)) is None


def test_run_refresh_script_nonzero_exit(tmp_path):
    script = tmp_path / "fail.py"
    script.write_text("import sys; print('boom', file=sys.stderr); sys.exit(3)")
    with pytest.raises(RuntimeError, match="exit 3"):
        run_refresh_script(str(script))


def test_run_refresh_script_honors_cwd(tmp_path):
    """cwd is passed through to subprocess so script-relative writes land there."""
    script = tmp_path / "cwd.py"
    script.write_text(
        "import os, pathlib; " "pathlib.Path('marker.txt').write_text(os.getcwd())"
    )
    run_refresh_script(str(script), cwd=str(tmp_path))
    marker = tmp_path / "marker.txt"
    assert marker.exists()
    assert str(tmp_path).lower() in marker.read_text().lower()


# --- parse-time validation tests ---


def test_parse_error_refresh_on_non_root():
    e = Dialects.DUCK_DB.default_executor()
    with pytest.raises(Exception, match="only valid on root"):
        e.execute_text("""
            key id int;
            datasource d (id: id)
            grain (id)
            address d_table
            freshness by `/fake/probe.py`
            refresh `/fake/refresh.py`;
            """)


def test_parse_error_refresh_without_freshness_probe():
    e = Dialects.DUCK_DB.default_executor()
    with pytest.raises(Exception, match="freshness by"):
        e.execute_text("""
            key id int;
            root datasource d (id: id)
            grain (id)
            address d_table
            refresh `/fake/refresh.py`;
            """)


def test_parse_error_refresh_on_query_address():
    e = Dialects.DUCK_DB.default_executor()
    with pytest.raises(Exception, match="query-backed"):
        e.execute_text("""
            key id int;
            root datasource d (id: id)
            grain (id)
            query '''SELECT 1 as id'''
            freshness by `/fake/probe.py`
            refresh `/fake/refresh.py`;
            """)


def test_render_round_trip_preserves_refresh_script():
    """Round-trip parse → render → reparse preserves refresh_script field."""
    from trilogy.parsing.render import Renderer

    e = Dialects.DUCK_DB.default_executor()
    e.execute_text("""
        key id int;
        property id.value string;

        root datasource raw (id: id, value: value)
        grain (id)
        address raw_t
        freshness by `/fake/probe.py`
        refresh `/fake/refresh.py`;
        """)
    ds = e.environment.datasources["raw"]
    rendered = Renderer().to_string(ds)
    # Path may be normalized by the OS; just check the refresh keyword + path tail.
    assert "refresh `" in rendered
    assert "refresh.py" in rendered
