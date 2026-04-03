"""Tests for display_refresh: watermark and refresh pipeline display functions."""

import importlib
import sys
from contextlib import contextmanager
from datetime import datetime
from io import StringIO
from pathlib import Path

import pytest

import trilogy.scripts.display_core as display_core
from trilogy.scripts.display_models import ManagedDataGroup, StaleDataSourceEntry
from trilogy.scripts.display_refresh import (
    _common_prefix,
    _ProbeProgressContext,
    probe_progress,
    root_probe_progress,
    show_asset_status_summary,
    show_dry_run_queries,
    show_grouped_refresh_assets,
    show_managed_asset_list,
    show_refresh_plan,
    show_root_concepts,
    show_root_probe_breakdown,
    show_stale_assets,
)

RICH_AVAILABLE = False
if importlib.util.find_spec("rich") is not None:
    RICH_AVAILABLE = True


@contextmanager
def capture_stdout():
    old = sys.stdout
    buf = StringIO()
    sys.stdout = buf
    try:
        yield buf
    finally:
        sys.stdout = old


@contextmanager
def capture_rich_output():
    if display_core.RICH_AVAILABLE and RICH_AVAILABLE:
        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=120)
        original = display_core.console
        display_core.console = test_console
        try:
            yield buf
        finally:
            display_core.console = original
    else:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        buf = StringIO()
        sys.stdout = buf
        sys.stderr = buf
        try:
            yield buf
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


@pytest.fixture(params=[True, False], ids=["rich", "fallback"])
def rich_mode(request):
    if request.param and not RICH_AVAILABLE:
        pytest.skip("Rich not available")
    original = display_core.RICH_AVAILABLE
    display_core.RICH_AVAILABLE = request.param
    if not request.param:
        display_core.console = None
    else:
        try:
            from rich.console import Console

            display_core.console = Console()
        except ImportError:
            display_core.RICH_AVAILABLE = False
    yield request.param
    display_core.RICH_AVAILABLE = original
    if original:
        try:
            from rich.console import Console

            display_core.console = Console()
        except ImportError:
            display_core.console = None
    else:
        display_core.console = None


def _make_watermarks():
    from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
    from trilogy.execution.state.watermarks import DatasourceWatermark

    return {
        "source_a": DatasourceWatermark(
            keys={
                "ts_field": UpdateKey(
                    concept_name="ts_field",
                    type=UpdateKeyType.UPDATE_TIME,
                    value=datetime(2026, 1, 1),
                )
            }
        ),
        "source_b": DatasourceWatermark(keys={}),
    }


def _make_env_max():
    from trilogy.core.models.datasource import UpdateKey, UpdateKeyType

    return {
        "ts_field": UpdateKey(
            concept_name="ts_field",
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=datetime(2026, 3, 1),
        )
    }


class TestCommonPrefix:
    def test_empty_list(self):
        assert _common_prefix([]) == ""

    def test_single_string(self):
        # single string: returns directory prefix up to last separator
        assert _common_prefix(["/a/b/c"]) == "/a/b/"

    def test_common_path_prefix(self):
        result = _common_prefix(["/data/a/file.parquet", "/data/b/file.parquet"])
        assert result == "/data/"

    def test_no_common_prefix(self):
        assert _common_prefix(["abc", "xyz"]) == ""

    def test_identical_strings(self):
        result = _common_prefix(["/data/file.parquet", "/data/file.parquet"])
        assert result == "/data/"


class TestShowManagedAssetList:
    def test_outputs_addresses(self, rich_mode):
        with capture_rich_output() as buf:
            show_managed_asset_list(["schema.table_a", "schema.table_b"])
            captured = buf.getvalue()
        assert "schema.table_a" in captured
        assert "schema.table_b" in captured

    def test_empty_list(self, rich_mode):
        with capture_rich_output() as buf:
            show_managed_asset_list([])
            captured = buf.getvalue()
        assert "Managed assets found" in captured


class TestShowRootConcepts:
    def test_shows_concepts(self, rich_mode):
        data = {"root/a.parquet": {"ts_field", "row_count"}}
        with capture_rich_output() as buf:
            show_root_concepts(data)
            captured = buf.getvalue()
        assert "ts_field" in captured
        assert "row_count" in captured

    def test_empty_dict_produces_no_output(self, rich_mode):
        with capture_rich_output() as buf:
            show_root_concepts({})
            captured = buf.getvalue()
        assert captured == ""

    def test_common_prefix_stripped(self, rich_mode):
        data = {
            "/data/root/a.parquet": {"ts_field"},
            "/data/root/b.parquet": {"ts_field"},
        }
        with capture_rich_output() as buf:
            show_root_concepts(data)
            captured = buf.getvalue()
        assert "ts_field" in captured
        # prefix should be stripped from displayed address
        assert "a.parquet" in captured


class TestShowStaleAssets:
    def test_shows_datasource_and_reason(self, rich_mode):
        assets = [
            StaleDataSourceEntry(
                datasource_id="my_ds",
                referenced_in=[Path("etl.preql")],
                reason="watermark exceeded",
            )
        ]
        with capture_rich_output() as buf:
            show_stale_assets(assets)
            captured = buf.getvalue()
        assert "my_ds" in captured
        assert "watermark exceeded" in captured

    def test_empty_list(self, rich_mode):
        with capture_rich_output() as buf:
            show_stale_assets([])
            captured = buf.getvalue()
        assert "Stale" in captured


class TestShowGroupedRefreshAssets:
    def test_shows_grouped_data(self, rich_mode):
        group = ManagedDataGroup(
            data_address="schema.my_table",
            datasources=[
                StaleDataSourceEntry(
                    datasource_id="ds_1",
                    referenced_in=[Path("etl.preql")],
                    reason="stale",
                )
            ],
            common_reason="out of date",
        )
        with capture_rich_output() as buf:
            show_grouped_refresh_assets([group])
            captured = buf.getvalue()
        assert "schema.my_table" in captured
        assert "ds_1" in captured

    def test_multiple_datasources_per_group(self, rich_mode):
        group = ManagedDataGroup(
            data_address="schema.tbl",
            datasources=[
                StaleDataSourceEntry(
                    datasource_id="ds_1",
                    referenced_in=[Path("a.preql")],
                    reason=None,
                ),
                StaleDataSourceEntry(
                    datasource_id="ds_2",
                    referenced_in=[Path("b.preql")],
                    reason="stale",
                ),
            ],
            common_reason=None,
        )
        with capture_rich_output() as buf:
            show_grouped_refresh_assets([group])
            captured = buf.getvalue()
        assert "ds_1" in captured
        assert "ds_2" in captured


class TestShowRootProbeBreakdown:
    def test_shows_concept_and_max(self, rich_mode):
        from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
        from trilogy.execution.state.watermarks import DatasourceWatermark

        root_watermarks = {
            "root_ds": DatasourceWatermark(
                keys={
                    "ts_field": UpdateKey(
                        concept_name="ts_field",
                        type=UpdateKeyType.UPDATE_TIME,
                        value=datetime(2026, 1, 15),
                    )
                }
            )
        }
        from trilogy.core.models.datasource import UpdateKey, UpdateKeyType

        concept_max = {
            "ts_field": UpdateKey(
                concept_name="ts_field",
                type=UpdateKeyType.UPDATE_TIME,
                value=datetime(2026, 1, 15),
            )
        }
        with capture_rich_output() as buf:
            show_root_probe_breakdown(root_watermarks, concept_max)
            captured = buf.getvalue()
        assert "ts_field" in captured
        assert "root_ds" in captured

    def test_empty_concept_max_produces_no_output(self, rich_mode):
        with capture_rich_output() as buf:
            show_root_probe_breakdown({}, {})
            captured = buf.getvalue()
        assert captured == ""


class TestShowAssetStatusSummary:
    def test_shows_status(self, rich_mode):
        from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
        from trilogy.execution.state.watermarks import DatasourceWatermark

        watermarks = {
            "ds_1": DatasourceWatermark(
                keys={
                    "ts": UpdateKey(
                        concept_name="ts",
                        type=UpdateKeyType.UPDATE_TIME,
                        value=datetime(2026, 1, 1),
                    )
                }
            )
        }
        address_map = {"ds_1": "schema.tbl"}
        stale_assets = [
            StaleDataSourceEntry(
                datasource_id="ds_1",
                referenced_in=[Path("etl.preql")],
                reason="too old",
            )
        ]
        with capture_rich_output() as buf:
            show_asset_status_summary(watermarks, address_map, stale_assets)
            captured = buf.getvalue()
        assert "schema.tbl" in captured
        assert "ds_1" in captured
        assert "too old" in captured

    def test_up_to_date_status(self, rich_mode):
        from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
        from trilogy.execution.state.watermarks import DatasourceWatermark

        watermarks = {
            "ds_1": DatasourceWatermark(
                keys={
                    "ts": UpdateKey(
                        concept_name="ts",
                        type=UpdateKeyType.UPDATE_TIME,
                        value=datetime(2026, 2, 1),
                    )
                }
            )
        }
        with capture_rich_output() as buf:
            show_asset_status_summary(watermarks, {}, [])
            captured = buf.getvalue()
        assert "up to date" in captured

    def test_with_env_max(self, rich_mode):
        watermarks = _make_watermarks()
        env_max = _make_env_max()
        with capture_rich_output() as buf:
            show_asset_status_summary(watermarks, {}, [], env_max=env_max)
            captured = buf.getvalue()
        assert "ts_field" in captured


class TestShowDryRunQueries:
    def test_shows_sql(self, rich_mode):
        from pathlib import Path

        from trilogy.scripts.common import ExecutionStats, RefreshQuery
        from trilogy.scripts.dependency import ScriptNode
        from trilogy.scripts.parallel_execution import ExecutionResult

        node = ScriptNode(path=Path("etl.preql"))
        stats = ExecutionStats(
            refresh_queries=[RefreshQuery(datasource_id="my_ds", sql="SELECT 1")]
        )
        result = ExecutionResult(node=node, success=True, stats=stats, duration=1.0)
        with capture_rich_output() as buf:
            show_dry_run_queries([result])
            captured = buf.getvalue()
        assert "SELECT" in captured
        assert "my_ds" in captured

    def test_skips_failed_results(self, rich_mode):
        from pathlib import Path

        from trilogy.scripts.dependency import ScriptNode
        from trilogy.scripts.parallel_execution import ExecutionResult

        node = ScriptNode(path=Path("etl.preql"))
        result = ExecutionResult(node=node, success=False, stats=None, duration=0.1)
        with capture_rich_output() as buf:
            show_dry_run_queries([result])
            captured = buf.getvalue()
        assert "SELECT" not in captured


class TestShowRefreshPlan:
    def test_delegates_to_show_grouped_and_watermarks(self, rich_mode):
        watermarks = _make_watermarks()
        group = ManagedDataGroup(
            data_address="schema.tbl",
            datasources=[
                StaleDataSourceEntry(
                    datasource_id="ds_1",
                    referenced_in=[Path("etl.preql")],
                    reason="stale",
                )
            ],
        )
        with capture_rich_output() as buf:
            show_refresh_plan([], watermarks, grouped_assets=[group])
            captured = buf.getvalue()
        assert "source_a" in captured  # from watermarks
        assert "ds_1" in captured  # from grouped_assets

    def test_falls_back_to_stale_assets(self, rich_mode):
        watermarks = _make_watermarks()
        stale = [
            StaleDataSourceEntry(
                datasource_id="ds_x",
                referenced_in=[Path("etl.preql")],
                reason="expired",
            )
        ]
        with capture_rich_output() as buf:
            show_refresh_plan(stale, watermarks)
            captured = buf.getvalue()
        assert "ds_x" in captured
        assert "expired" in captured


class TestProbeProgressContext:
    def test_enter_exit_no_rich(self):
        original = display_core.RICH_AVAILABLE
        display_core.RICH_AVAILABLE = False
        orig_console = display_core.console
        display_core.console = None
        try:
            ctx = probe_progress(3)
            with ctx:
                assert ctx._progress is None
                ctx.advance()  # no-op when no Rich
        finally:
            display_core.RICH_AVAILABLE = original
            display_core.console = orig_console

    def test_root_probe_label(self):
        ctx = root_probe_progress(5)
        assert ctx._task_label == "Probing root watermarks"
        assert ctx._total == 5

    def test_probe_progress_label(self):
        ctx = probe_progress(2)
        assert ctx._task_label == "Checking assets"
        assert ctx._total == 2

    def test_register_futures_updates_getter(self):
        from unittest.mock import MagicMock

        ctx = _ProbeProgressContext(1)
        future = MagicMock()
        future.done.return_value = True
        from pathlib import Path

        from trilogy.scripts.dependency import ScriptNode

        node = ScriptNode(path=Path("x.preql"))
        ctx.register_futures({future: node})
        assert ctx._stderr_cap.get_context is not None
        # calling it should work
        result = ctx._stderr_cap.get_context()
        assert isinstance(result, str)


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich not available")
class TestProbeProgressContextRich:
    def test_enter_advance_exit_with_rich(self):
        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=80)
        orig = display_core.console
        orig_avail = display_core.RICH_AVAILABLE
        display_core.console = test_console
        display_core.RICH_AVAILABLE = True
        try:
            ctx = probe_progress(3)
            with ctx:
                assert ctx._progress is not None
                ctx.advance()
        finally:
            display_core.console = orig
            display_core.RICH_AVAILABLE = orig_avail

    def test_register_futures_with_rich(self):
        from io import StringIO
        from unittest.mock import MagicMock

        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=80)
        orig = display_core.console
        orig_avail = display_core.RICH_AVAILABLE
        display_core.console = test_console
        display_core.RICH_AVAILABLE = True
        try:
            future = MagicMock()
            future.done.return_value = True
            from trilogy.scripts.dependency import ScriptNode

            node = ScriptNode(path=Path("x.preql"))
            futures = {future: node}
            ctx = probe_progress(2)
            with ctx:
                ctx.register_futures(futures)
                ctx.advance()
        finally:
            display_core.console = orig
            display_core.RICH_AVAILABLE = orig_avail
