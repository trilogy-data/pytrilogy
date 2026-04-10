from pathlib import Path
from unittest.mock import patch

import pytest
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    DatasourceWatermark,
    StaleAsset,
    refresh_stale_assets,
)
from trilogy.execution.state.state_store import BaseStateStore
from trilogy.scripts.common import CLIRuntimeParams, RefreshParams
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.display import (
    ManagedDataGroup,
    StaleDataSourceEntry,
    is_rich_available,
    set_rich_mode,
    show_refresh_plan,
    show_stale_assets,
    show_watermarks,
)
from trilogy.scripts.refresh import (
    _preview_directory_refresh,
    execute_managed_node_for_refresh,
)
from trilogy.scripts.single_execution import (
    execute_refresh_mode,
    execute_script_for_refresh,
)
from trilogy.scripts.trilogy import cli

requires_rich = pytest.mark.skipif(
    not is_rich_available(), reason="rich is not installed"
)


def _sample_watermarks() -> dict[str, DatasourceWatermark]:
    return {
        "orders": DatasourceWatermark(
            keys={
                "order_date": UpdateKey(
                    concept_name="order_date",
                    type=UpdateKeyType.INCREMENTAL_KEY,
                    value="2024-01-15",
                )
            }
        ),
        "users": DatasourceWatermark(keys={}),
    }


def _sample_stale_assets() -> list[StaleAsset]:
    return [
        StaleAsset(
            datasource_id="target_orders",
            reason="incremental key 'order_date' behind: 2024-01-10 < 2024-01-15",
        ),
    ]


def _sample_grouped_assets() -> list[ManagedDataGroup]:
    return [
        ManagedDataGroup(
            data_address="target_orders",
            common_reason="forced rebuild",
            datasources=[
                StaleDataSourceEntry(
                    datasource_id="target_orders",
                    referenced_in=[Path("base.preql"), Path("consumer.preql")],
                ),
            ],
        )
    ]


def test_show_watermarks_empty(capsys):
    """Test formatting when watermarks dict is empty."""
    with set_rich_mode(False):
        watermarks: dict[str, DatasourceWatermark] = {}
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out


def test_show_watermarks_no_keys(capsys):
    """Test formatting when datasource has no watermark keys."""
    with set_rich_mode(False):
        watermarks = {"ds1": DatasourceWatermark(keys={})}
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "ds1: (no watermarks)" in captured.out


def test_show_watermarks_with_keys(capsys):
    """Test formatting datasources with watermark keys."""
    with set_rich_mode(False):
        watermarks = {
            "orders": DatasourceWatermark(
                keys={
                    "order_date": UpdateKey(
                        concept_name="order_date",
                        type=UpdateKeyType.INCREMENTAL_KEY,
                        value="2024-01-15",
                    )
                }
            )
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "orders.order_date: 2024-01-15 (incremental_key)" in captured.out


def test_show_watermarks_multiple_datasources(capsys):
    """Test formatting multiple datasources with various watermark configurations."""
    with set_rich_mode(False):
        watermarks = {
            "users": DatasourceWatermark(keys={}),
            "events": DatasourceWatermark(
                keys={
                    "event_time": UpdateKey(
                        concept_name="event_time",
                        type=UpdateKeyType.UPDATE_TIME,
                        value="2024-06-01T12:00:00",
                    ),
                    "event_id": UpdateKey(
                        concept_name="event_id",
                        type=UpdateKeyType.INCREMENTAL_KEY,
                        value=12345,
                    ),
                }
            ),
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "users: (no watermarks)" in captured.out
    assert "events.event_id: 12345 (incremental_key)" in captured.out
    assert "events.event_time: 2024-06-01T12:00:00 (update_time)" in captured.out


def test_show_watermarks_sorted_output(capsys):
    """Test that datasources are printed in sorted order."""
    with set_rich_mode(False):
        watermarks = {
            "zebra": DatasourceWatermark(keys={}),
            "alpha": DatasourceWatermark(keys={}),
            "beta": DatasourceWatermark(keys={}),
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    alpha_pos = captured.out.find("alpha")
    beta_pos = captured.out.find("beta")
    zebra_pos = captured.out.find("zebra")
    assert alpha_pos < beta_pos < zebra_pos


@requires_rich
def test_show_watermarks_rich(capsys):
    with set_rich_mode(True):
        show_watermarks(_sample_watermarks())
    captured = capsys.readouterr()
    assert "Datasource Watermarks" in captured.out
    assert "orders" in captured.out
    assert "order_date" in captured.out
    assert "2024-01-15" in captured.out
    assert "users" in captured.out
    assert "(no watermarks)" in captured.out


def test_show_stale_assets_plain(capsys):
    with set_rich_mode(False):
        show_stale_assets(_sample_stale_assets())
    captured = capsys.readouterr()
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out
    assert "behind" in captured.out


@requires_rich
def test_show_stale_assets_rich(capsys):
    with set_rich_mode(True):
        show_stale_assets(_sample_stale_assets())
    captured = capsys.readouterr()
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out


def test_show_refresh_plan_plain(capsys):
    with set_rich_mode(False):
        show_refresh_plan(_sample_stale_assets(), _sample_watermarks())
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out


def test_show_refresh_plan_grouped_plain(capsys):
    with set_rich_mode(False):
        show_refresh_plan(
            _sample_stale_assets(),
            _sample_watermarks(),
            grouped_assets=_sample_grouped_assets(),
        )
    captured = capsys.readouterr()
    assert "base.preql" in captured.out
    assert "consumer.preql" in captured.out
    assert "forced rebuild" in captured.out
    assert "target_orders" in captured.out


@requires_rich
def test_show_refresh_plan_rich(capsys):
    with set_rich_mode(True):
        show_refresh_plan(_sample_stale_assets(), _sample_watermarks())
    captured = capsys.readouterr()
    assert "Datasource Watermarks" in captured.out
    assert "Stale Assets to Refresh" in captured.out


def test_refresh_stale_assets_on_approval_declined():
    """on_approval returning False should skip refresh."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        ''';

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;
        """
    )

    result = refresh_stale_assets(
        executor,
        on_approval=lambda assets, wm: False,
        force_sources={"target_items"},
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 0


def test_refresh_stale_assets_on_approval_accepted():
    """on_approval returning True should proceed with refresh."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        ''';

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;
        """
    )

    result = refresh_stale_assets(
        executor,
        on_approval=lambda assets, wm: True,
        force_sources={"target_items"},
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 1


def test_refresh_stale_assets_forced():
    """Test that force_sources forces rebuild regardless of staleness."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        ''';

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;

        RAW_SQL('''
        INSERT INTO target_items_table
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        ''');
        """
    )

    refreshed_assets: list[tuple[str, str]] = []

    def track_refresh(asset_id: str, reason: str) -> None:
        refreshed_assets.append((asset_id, reason))

    # Without force, target_items should NOT be refreshed (it's up to date)
    result = refresh_stale_assets(executor, on_refresh=track_refresh)
    assert result.stale_count == 0
    assert result.refreshed_count == 0
    assert len(refreshed_assets) == 0

    # With force, target_items SHOULD be refreshed
    result = refresh_stale_assets(
        executor, on_refresh=track_refresh, force_sources={"target_items"}
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 1
    assert len(refreshed_assets) == 1
    assert refreshed_assets[0][0] == "target_items"
    assert refreshed_assets[0][1] == "forced rebuild"


STALE_SCRIPT = """\
key item_id int;
property item_id.name string;
property item_id.updated_at datetime;

root datasource source_items (
    item_id: item_id,
    name: name,
    updated_at: updated_at
)
grain (item_id)
query '''
SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
''';

datasource target_items (
    item_id: item_id,
    name: name,
    updated_at: updated_at
)
grain (item_id)
address target_items_table
freshness by updated_at;

CREATE IF NOT EXISTS DATASOURCE target_items;
"""


def test_execute_script_for_refresh_with_watermarks(tmp_path, capsys):
    """Cover on_watermarks callback and refreshed success path."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    node = ScriptNode(path=script)

    with set_rich_mode(False):
        stats = execute_script_for_refresh(
            executor,
            node,
            print_watermarks=True,
            force_sources=frozenset({"target_items"}),
        )

    assert stats.update_count == 1
    captured = capsys.readouterr()
    assert "Asset Status:" in captured.out
    assert "Refreshed 1 asset(s)" in captured.out


def test_execute_script_for_refresh_interactive_declined(tmp_path, capsys):
    """Cover interactive=True with user declining approval."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    node = ScriptNode(path=script)

    with set_rich_mode(False), patch("click.confirm", return_value=False):
        stats = execute_script_for_refresh(
            executor,
            node,
            force_sources=frozenset({"target_items"}),
            interactive=True,
        )

    assert stats.update_count == 0
    captured = capsys.readouterr()
    assert "Refresh skipped by user" in captured.out


def test_execute_script_for_refresh_dry_run(tmp_path, capsys):
    """dry_run=True should print SQL without executing it."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    node = ScriptNode(path=script)

    with set_rich_mode(False):
        stats = execute_script_for_refresh(
            executor,
            node,
            force_sources=frozenset({"target_items"}),
            dry_run=True,
        )

    captured = capsys.readouterr()
    assert "SELECT" in captured.out, captured.out
    assert "Dry run" in captured.out, captured.out
    assert stats.update_count == 1
    assert stats.refresh_queries, "Should have collected SQL"


def test_execute_refresh_mode_dry_run(capsys):
    """execute_refresh_mode with dry_run=True should print SQL and not execute."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(STALE_SCRIPT)

    with set_rich_mode(False):
        result = execute_refresh_mode(
            executor,
            force_sources={"target_items"},
            dry_run=True,
        )

    captured = capsys.readouterr()
    assert "SELECT" in captured.out, captured.out
    assert "Dry run" in captured.out, captured.out
    assert result.refreshed_count == 1


def test_execute_refresh_mode_with_watermarks(capsys):
    """Cover show_watermarks call in single_execution.execute_refresh_mode."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(STALE_SCRIPT)

    with set_rich_mode(False):
        result = execute_refresh_mode(
            executor,
            force_sources={"target_items"},
            print_watermarks=True,
        )

    assert result.refreshed_count == 1
    captured = capsys.readouterr()
    assert "Asset Status:" in captured.out


def test_refresh_directory_interactive_aggregates_plan(tmp_path):
    source_file = tmp_path / "source.preql"
    source_file.write_text(
        """
key event_id int;
property event_id.event_ts datetime;

root datasource source_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
query '''
SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
UNION ALL
SELECT 2 as event_id, TIMESTAMP '2024-01-15 12:00:00' as event_ts
''';
"""
    )

    base_file = tmp_path / "base.preql"
    base_file.write_text(
        """
import source;

datasource target_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
address target_events_table
incremental by event_ts;
"""
    )

    consumer_file = tmp_path / "consumer.preql"
    consumer_file.write_text(
        """
import base;
"""
    )

    runner = CliRunner()
    with set_rich_mode(False), patch("click.confirm", return_value=False) as confirm:
        result = runner.invoke(
            cli,
            ["refresh", str(tmp_path), "duckdb", "--interactive"],
        )

    assert result.exit_code == 2, result.output
    assert confirm.call_count == 1
    assert "target_events_table" in result.output
    assert "incremental key 'event_ts' behind" in result.output
    assert "Asset Status:" in result.output


def test_refresh_stale_assets_excludes_peer_stale_from_planner():
    """When multiple datasources are stale, each refresh hides the not-yet-refreshed
    peers from the query planner so it cannot route through stale/missing sources."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key rec_id int;
        property rec_id.rec_ts datetime;

        root datasource rec_root (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        query '''SELECT 1 as rec_id, TIMESTAMP '2024-01-20' as rec_ts''';

        datasource rec_stage (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        address rec_stage_table
        incremental by rec_ts;

        datasource rec_final (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        address rec_final_table
        incremental by rec_ts;
        """
    )

    # Snapshot which datasources are visible in the environment at each update call.
    env_at_each_refresh: list[set[str]] = []
    original_update = executor.update_datasource

    def tracking_update(datasource, keys=None, dry_run: bool = False):
        env_at_each_refresh.append(set(executor.environment.datasources.keys()))
        return original_update(datasource, keys)

    executor.update_datasource = tracking_update  # type: ignore[method-assign]

    result = refresh_stale_assets(executor)

    assert result.refreshed_count == 2
    assert len(env_at_each_refresh) == 2

    first_visible = env_at_each_refresh[0]
    second_visible = env_at_each_refresh[1]

    assert "rec_root" in first_visible
    assert "rec_root" in second_visible

    # During the first refresh, exactly one of the two stale peers is visible
    # (the other is hidden to prevent the planner routing through it).
    stale_visible_first = first_visible & {"rec_stage", "rec_final"}
    assert (
        len(stale_visible_first) == 1
    ), f"Expected one stale datasource visible during first refresh, got: {stale_visible_first}"

    # During the second refresh the first peer has been refreshed and is restored,
    # so both stale datasources are present again.
    stale_visible_second = second_visible & {"rec_stage", "rec_final"}
    assert (
        len(stale_visible_second) == 2
    ), f"Expected both stale datasources visible during second refresh, got: {stale_visible_second}"


# --- Helpers for physical graph tests ---

_SOURCE_SCRIPT = """\
key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
query '''SELECT 1 as ev_id, TIMESTAMP '2024-01-10 12:00:00' as ev_ts''';
"""

_STALE_DS_SCRIPT = """\
import {dep};

datasource {name} (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
address {name}_table
incremental by ev_ts;
"""


def _make_cli_params(tmp_path) -> CLIRuntimeParams:
    return CLIRuntimeParams(
        input=str(tmp_path),
        dialect=Dialects.DUCK_DB,
        refresh_params=RefreshParams(force_sources=frozenset()),
    )


def test_phys_graph_independent_scripts_no_edges(tmp_path):
    """Two scripts with no import relationship → phys_graph has no edges."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "alpha.preql").write_text(
        _STALE_DS_SCRIPT.format(dep="source", name="alpha")
    )
    (tmp_path / "beta.preql").write_text(
        _STALE_DS_SCRIPT.format(dep="source", name="beta")
    )

    cli_params = _make_cli_params(tmp_path)
    with patch("click.confirm", return_value=True):
        approved, phys_graph = _preview_directory_refresh(cli_params, tmp_path)

    assert approved
    assert phys_graph is not None
    assert phys_graph.number_of_nodes() == 2
    assert phys_graph.number_of_edges() == 0


def test_phys_graph_dependent_scripts_edge_direction(tmp_path):
    """Script B imports A → phys_graph edge goes from node(A) to node(B)."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "base.preql").write_text(
        _STALE_DS_SCRIPT.format(dep="source", name="base")
    )
    (tmp_path / "derived.preql").write_text(
        """\
import base;

datasource derived (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
address derived_table
incremental by ev_ts;
"""
    )

    cli_params = _make_cli_params(tmp_path)
    with patch("click.confirm", return_value=True):
        approved, phys_graph = _preview_directory_refresh(cli_params, tmp_path)

    assert approved
    assert phys_graph is not None
    assert phys_graph.number_of_nodes() == 2
    assert phys_graph.number_of_edges() >= 1

    # Edge must go from base_table node → derived_table node
    nodes_by_addr = {n.address: n for n in phys_graph.nodes}
    assert "base_table" in nodes_by_addr
    assert "derived_table" in nodes_by_addr
    base_node = nodes_by_addr["base_table"]
    derived_node = nodes_by_addr["derived_table"]
    assert phys_graph.has_edge(base_node, derived_node)
    assert not phys_graph.has_edge(derived_node, base_node)


def test_phys_graph_no_stale_script_excluded(tmp_path):
    """A script with no stale assets does not appear in the phys_graph."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    # Only base has a stale datasource; consumer imports base but persists nothing stale
    (tmp_path / "base.preql").write_text(
        _STALE_DS_SCRIPT.format(dep="source", name="base")
    )
    (tmp_path / "consumer.preql").write_text("import base;\n")

    cli_params = _make_cli_params(tmp_path)
    with patch("click.confirm", return_value=True):
        approved, phys_graph = _preview_directory_refresh(cli_params, tmp_path)

    assert approved
    assert phys_graph is not None
    assert phys_graph.number_of_nodes() == 1
    node = next(iter(phys_graph.nodes))
    assert node.address == "base_table"


# Two scripts that define different ds_ids but share the same physical address
_SHARED_ADDR_SCRIPT = """\
import {dep};

datasource {name} (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
address shared_physical_table
incremental by ev_ts;
"""


def test_preview_directory_probes_each_physical_asset_once(tmp_path):
    """When two scripts define datasources at the same physical address, the
    address is probed exactly once — not once per script."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "alpha.preql").write_text(
        _SHARED_ADDR_SCRIPT.format(dep="source", name="alpha")
    )
    (tmp_path / "beta.preql").write_text(
        _SHARED_ADDR_SCRIPT.format(dep="source", name="beta")
    )

    probe_calls: list[str] = []
    original = BaseStateStore.watermark_asset

    def counting(self, datasource, executor):  # type: ignore[override]
        probe_calls.append(datasource.identifier)
        return original(self, datasource, executor)

    cli_params = _make_cli_params(tmp_path)
    with patch.object(BaseStateStore, "watermark_asset", counting):
        _preview_directory_refresh(cli_params, tmp_path)

    # alpha and beta share the same physical address — only one should be probed
    assert probe_calls.count("alpha") + probe_calls.count("beta") == 1


def test_preview_directory_excludes_root_addresses_from_list(tmp_path):
    """Root datasources (marked with the 'root' modifier) must not appear in the
    physical-asset list — only non-root addresses should be shown and probed."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "alpha.preql").write_text(
        _STALE_DS_SCRIPT.format(dep="source", name="alpha")
    )

    import trilogy.scripts.display as display_mod

    listed: list[list[str]] = []
    original = display_mod.show_managed_asset_list

    def capturing(addresses: list[str]) -> None:
        listed.append(list(addresses))
        return original(addresses)

    cli_params = _make_cli_params(tmp_path)
    with patch.object(display_mod, "show_managed_asset_list", capturing):
        _preview_directory_refresh(cli_params, tmp_path)

    assert listed, "show_managed_asset_list should have been called"
    # Only the non-root address (alpha_table) should appear — the root datasource
    # (src_events) is used only for watermark comparison and must be excluded
    assert listed[0] == ["alpha_table"]


def test_preview_directory_import_only_script_does_not_reprobe(tmp_path):
    """A script that only imports another script should not trigger a second
    probe for the physical assets it inherits via that import."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "alpha.preql").write_text(
        _SHARED_ADDR_SCRIPT.format(dep="source", name="alpha")
    )
    # consumer only imports alpha — no new datasources, just inherits alpha's env
    (tmp_path / "consumer.preql").write_text("import alpha;\n")

    probe_calls: list[str] = []
    original = BaseStateStore.watermark_asset

    def counting(self, datasource, executor):  # type: ignore[override]
        probe_calls.append(datasource.identifier)
        return original(self, datasource, executor)

    cli_params = _make_cli_params(tmp_path)
    with patch.object(BaseStateStore, "watermark_asset", counting):
        _preview_directory_refresh(cli_params, tmp_path)

    # alpha owns shared_physical_table; consumer's import must not cause a second probe
    assert probe_calls.count("alpha") == 1


def test_preview_directory_root_probe_uses_matching_namespace(tmp_path):
    """Root probes must run in a script whose concept namespace matches the target."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "base.preql").write_text(
        """\
import source as src;

datasource target_events (
    ev_id: src.ev_id,
    ev_ts: src.ev_ts
)
grain (src.ev_id)
address target_events_table
incremental by src.ev_ts;
"""
    )

    cli_params = _make_cli_params(tmp_path)
    approved, phys_graph = _preview_directory_refresh(cli_params, tmp_path)

    assert approved
    assert phys_graph is not None
    assert {node.address for node in phys_graph.nodes} == {"target_events_table"}


def test_preview_directory_relative_input_matches_absolute_behavior(
    tmp_path, monkeypatch
):
    """Relative refresh paths should resolve to the same script ownership as absolute ones."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "base.preql").write_text(
        """\
import source as src;

datasource target_events (
    ev_id: src.ev_id,
    ev_ts: src.ev_ts
)
grain (src.ev_id)
address target_events_table
incremental by src.ev_ts;
"""
    )

    monkeypatch.chdir(tmp_path.parent)
    relative_input = Path(tmp_path.name)
    cli_params = CLIRuntimeParams(
        input=str(relative_input),
        dialect=Dialects.DUCK_DB,
        refresh_params=RefreshParams(force_sources=frozenset()),
    )

    approved, phys_graph = _preview_directory_refresh(cli_params, relative_input)

    assert approved
    assert phys_graph is not None
    assert {node.address for node in phys_graph.nodes} == {"target_events_table"}


def test_preview_directory_fails_when_root_probe_coverage_is_missing(tmp_path, capsys):
    """Silent root probe misses should fail validation instead of reporting up to date."""
    (tmp_path / "source.preql").write_text(_SOURCE_SCRIPT)
    (tmp_path / "base.preql").write_text(
        """\
import source as src;

datasource target_events (
    ev_id: src.ev_id,
    ev_ts: src.ev_ts
)
grain (src.ev_id)
address target_events_table
incremental by src.ev_ts;
"""
    )

    cli_params = _make_cli_params(tmp_path)
    with set_rich_mode(False), patch(
        "trilogy.scripts.refresh._collect_root_watermarks", return_value={}
    ):
        with pytest.raises(Exit) as exc_info:
            _preview_directory_refresh(cli_params, tmp_path)

    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    assert "root watermark concepts were planned but never collected" in captured.out


def test_execute_physical_node_prints_refreshed_count(tmp_path, capsys):
    """execute_managed_node_for_refresh prints a summary after refreshing."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    with open(script) as f:
        executor.parse_text(f.read(), root=script)

    from trilogy.execution.state import StaleAsset
    from trilogy.scripts.dependency import ManagedRefreshNode, ScriptNode

    node = ManagedRefreshNode(
        address="target_items_table",
        owner_script=ScriptNode(path=script),
        assets=[StaleAsset(datasource_id="target_items", reason="forced rebuild")],
    )

    with set_rich_mode(False):
        stats = execute_managed_node_for_refresh(
            executor,
            node,
            quiet=False,
            print_watermarks=False,
            interactive=False,
            dry_run=False,
        )

    assert stats.update_count == 1
    captured = capsys.readouterr()
    assert "Refreshed 1 asset(s)" in captured.out
    assert "target_items_table" in captured.out
