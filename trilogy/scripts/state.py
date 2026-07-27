"""State command for Trilogy CLI - read-only asset state / watermark snapshot.

Probes watermarks and staleness for a file or directory of trilogy scripts and
emits a :class:`~trilogy.execution.state.snapshot.StateSnapshot`: per physical
address, the observed and expected watermarks, staleness status + reason, and
the physical column -> logical concept mappings. Never writes warehouse state.

The same snapshot is produced post-execution by ``run``/``refresh``
``--state-file`` (see :func:`maybe_write_state_snapshot`).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path as PathlibPath

from click import UNPROCESSED, argument, option, pass_context
from click import Path as ClickPath
from click.exceptions import Exit

from trilogy.core.models.datasource import Datasource
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig
from trilogy.execution.report import emit_report, get_report_sink, report_run
from trilogy.execution.state.persistence import (
    ENV_STATE_FILE,
    read_state_snapshot,
    resolve_state_input,
    snapshot_store_factory,
)
from trilogy.execution.state.snapshot import (
    DatasourceState,
    StateSnapshot,
    address_type_of,
    build_datasource_state,
    merge_into_snapshot,
    project_relative_path,
    stable_asset_key,
)
from trilogy.execution.state.state_store import state_store_factory
from trilogy.execution.state.watermarks import StaleAsset
from trilogy.scripts.click_utils import report_options, validate_dialect
from trilogy.scripts.common import (
    CLIRuntimeParams,
    handle_execution_exception,
)


def _merge_dicts(parts: list[dict]) -> dict:
    merged: dict = {}
    for part in parts:
        for key, value in part.items():
            merged.setdefault(key, value)
    return merged


def project_root_for(config: RuntimeConfig, fallback: PathlibPath) -> PathlibPath:
    """The directory asset keys are relativized against.

    ``trilogy.toml`` marks the project root (``find_trilogy_config`` walks up
    to it), so it is the only anchor every invocation of a project agrees on:
    scripts run from a subdirectory, a single script run directly, and the
    whole directory run at once all produce the same keys. Falling back to the
    input's own directory would key ``<proj>/models/base.preql``'s assets
    relative to ``models/`` — and anything under ``<proj>/data/`` could not be
    expressed at all, silently reverting to absolute, unportable paths.

    Without a config file the input directory is the best anchor available.
    """
    if config.source_path is not None:
        return config.source_path.parent.resolve()
    return fallback.resolve()


def _asset_key(ds: Datasource, address: str, project_root: PathlibPath) -> str:
    """Stable snapshot identity for a physical address."""
    return stable_asset_key(address, address_type_of(ds), project_root)


def _snapshot_from_directory(
    cli_params: CLIRuntimeParams, input_path: PathlibPath, run_id: str | None
) -> StateSnapshot:
    """Directory snapshot via the shared refresh probe (phases 1/2a/2b)."""
    from trilogy.scripts.refresh import probe_directory_state

    probe = probe_directory_state(cli_params, input_path)

    stale_map: dict[str, StaleAsset] = {}
    for _, plan in probe.plans_by_node:
        for asset in plan.refresh_assets:
            stale_map.setdefault(asset.datasource_id, asset)
    merged_watermarks = _merge_dicts([p.watermarks for _, p in probe.plans_by_node])
    # Root watermarks are collected in phase 2a and injected into plans, but a
    # root probed only there should still show its observed values.
    for ds_id, wm in probe.all_root_watermarks.items():
        merged_watermarks.setdefault(ds_id, wm)
    merged_concept_max = _merge_dicts(
        [p.concept_max_watermarks for _, p in probe.plans_by_node]
    )

    project_root = project_root_for(probe.config, input_path)

    keys_by_address: dict[str, str] = {}
    entries: list[tuple[str, DatasourceState]] = []
    for ds_id, ds in probe.ds_objects.items():
        address = probe.address_map.get(ds_id, ds.safe_address)
        key = _asset_key(ds, address, project_root)
        keys_by_address.setdefault(address, key)
        scripts = probe.ds_to_scripts.get(ds_id) or []
        entries.append(
            (
                key,
                build_datasource_state(
                    ds,
                    merged_watermarks.get(ds_id),
                    stale_map.get(ds_id),
                    merged_concept_max,
                    script=(
                        project_relative_path(str(scripts[0].path), project_root)
                        if scripts
                        else None
                    ),
                ),
            )
        )

    return merge_into_snapshot(
        entries,
        managed_addresses={
            keys_by_address[addr]
            for addr in probe.probe_addrs
            if addr in keys_by_address
        },
        # Only where trilogy manages the address: addr_to_owner also names the
        # script that merely *declares* an unmanaged root, which nothing builds.
        owner_scripts={
            keys_by_address[addr]: project_relative_path(str(owner.path), project_root)
            for addr, owner in probe.addr_to_owner.items()
            if addr in keys_by_address and addr in probe.probe_addrs
        },
        run_id=run_id,
        target=str(input_path),
        dialect=probe.edialect.value,
    )


def _snapshot_from_file(
    cli_params: CLIRuntimeParams, input_path: PathlibPath, run_id: str | None
) -> StateSnapshot:
    """Single-file snapshot: parse, watermark, and classify in one executor."""
    from trilogy.execution.state import new_state_store
    from trilogy.scripts.common import (
        create_executor_for_script,
        merge_runtime_config,
        resolve_input_information,
    )
    from trilogy.scripts.dependency import ScriptNode

    _, directory, _, _, config = resolve_input_information(
        str(input_path), cli_params.config_path
    )
    edialect, _ = merge_runtime_config(cli_params, config)
    project_root = project_root_for(config, PathlibPath(directory))

    node = ScriptNode(path=input_path.resolve())
    executor = create_executor_for_script(
        node,
        cli_params.param,
        cli_params.conn_args,
        edialect,
        cli_params.debug,
        config,
        cli_params.debug_file,
    )
    try:
        from trilogy.utility import safe_open

        with safe_open(node.path) as f:
            executor.parse_text(f.read(), root=node.path)

        store = new_state_store()
        watermarks = store.watermark_all_assets(executor.environment, executor)
        stale_assets = store.get_stale_assets(executor.environment, executor)
        stale_map = {a.datasource_id: a for a in stale_assets}

        managed = {
            ds.safe_address
            for ds in executor.environment.datasources.values()
            if ds.is_managed
        }

        script_attr = project_relative_path(str(node.path), project_root)
        keys_by_address: dict[str, str] = {}
        entries: list[tuple[str, DatasourceState]] = []
        for ds in executor.environment.datasources.values():
            key = _asset_key(ds, ds.safe_address, project_root)
            keys_by_address.setdefault(ds.safe_address, key)
            entries.append(
                (
                    key,
                    build_datasource_state(
                        ds,
                        watermarks.get(ds.identifier),
                        stale_map.get(ds.identifier),
                        store.concept_max_watermarks,
                        script=script_attr,
                    ),
                )
            )
        return merge_into_snapshot(
            entries,
            managed_addresses={
                keys_by_address[addr] for addr in managed if addr in keys_by_address
            },
            # The single script builds every managed address it binds.
            owner_scripts={
                keys_by_address[addr]: script_attr
                for addr in managed
                if addr in keys_by_address
            },
            run_id=run_id,
            target=str(input_path),
            dialect=edialect.value,
        )
    finally:
        executor.close()


def compute_state_snapshot(
    cli_params: CLIRuntimeParams, run_id: str | None = None
) -> StateSnapshot:
    """Probe state for the input path and build a snapshot. Read-only."""
    input_path = PathlibPath(cli_params.input)
    if input_path.is_dir():
        return _snapshot_from_directory(cli_params, input_path, run_id)
    if input_path.is_file():
        return _snapshot_from_file(cli_params, input_path, run_id)
    raise ValueError(
        f"State snapshot requires a file or directory input; got '{cli_params.input}'"
    )


@contextmanager
def state_input_scope(
    state_input: str | None, cli_params: CLIRuntimeParams | None = None
) -> Iterator[None]:
    """Seed the invocation's state stores from a persisted snapshot.

    The other half of ``--state-file``: an orchestrator that kept the snapshot
    from the last run hands it back here, and every store built inside the block
    trusts the recorded managed-asset observations instead of re-probing the
    warehouse for them. A no-op when no snapshot is configured.

    ``cli_params`` supplies the project root recorded keys are relative to —
    the same anchor the writer used. Without it the store falls back to each
    environment's working path, which is only the project root when the script
    sits at the top level."""
    path = resolve_state_input(state_input)
    if path is None:
        yield
        return
    from trilogy.scripts.common import resolve_input_information
    from trilogy.scripts.display import print_info

    project_root: PathlibPath | None = None
    if cli_params is not None:
        _, directory, _, _, config = resolve_input_information(
            cli_params.input, cli_params.config_path
        )
        project_root = project_root_for(config, PathlibPath(directory))

    snapshot = read_state_snapshot(path)
    print_info(
        f"Seeding asset state from {path} "
        f"({snapshot.summary.total} asset(s), run_id={snapshot.run_id})"
    )
    with state_store_factory(snapshot_store_factory(snapshot, project_root)):
        yield


def write_state_snapshot(snapshot: StateSnapshot, path: PathlibPath) -> None:
    """Write a snapshot as JSON and record its location in the report."""
    from trilogy.scripts.display import print_info

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    emit_report(
        "state_snapshot",
        path=str(path),
        summary=snapshot.summary.model_dump(),
    )
    print_info(f"State snapshot written to {path}")


def maybe_write_state_snapshot(
    cli_params: CLIRuntimeParams, state_file: str | None
) -> None:
    """Post-execution snapshot hook for run/refresh (--state-file /
    TRILOGY_STATE_FILE). Best-effort by contract: failures warn and emit an
    ``error`` report record but never raise — the run's own exit code stands."""
    path_str = state_file or os.environ.get(ENV_STATE_FILE, "").strip() or None
    if not path_str:
        return
    try:
        sink = get_report_sink()
        snapshot = compute_state_snapshot(
            cli_params, run_id=sink.run_id if sink else None
        )
        write_state_snapshot(snapshot, PathlibPath(path_str))
    except Exception as e:
        from trilogy.scripts.display import print_warning

        emit_report(
            "error",
            error_type=type(e).__name__,
            message=f"state snapshot failed: {e}",
        )
        print_warning(f"State snapshot failed: {e}")


def _show_snapshot(snapshot: StateSnapshot) -> None:
    """Human-facing rendering; in json display mode this emits a single
    ``state_snapshot`` event on the stdout stream."""
    from trilogy.scripts.display import emit_event, is_json_mode, print_info

    if is_json_mode():
        emit_event("state_snapshot", **snapshot.model_dump())
        return
    s = snapshot.summary
    print_info(
        f"Assets: {s.total} total, {s.managed} managed — "
        f"{s.fresh} fresh, {s.stale} stale, {s.unknown} unknown"
    )
    for asset in snapshot.assets:
        marker = {"fresh": "✓", "stale": "✗", "unknown": "?"}[asset.status]
        print_info(f"  {marker} {asset.address} [{asset.status}]")
        for ds_state in asset.datasources:
            if ds_state.stale_reason:
                print_info(f"      {ds_state.datasource_id}: {ds_state.stale_reason}")


@argument("input", type=ClickPath(), default=".")
@argument("dialect", type=str, required=False)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    type=int,
    default=None,
    help="Maximum parallel workers for directory probing",
)
@option(
    "--config",
    type=ClickPath(exists=True),
    help="Path to trilogy.toml configuration file",
)
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--output",
    "-o",
    "output",
    type=ClickPath(),
    default=None,
    help="Write the state snapshot as JSON to this path",
)
@report_options
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def state(
    ctx,
    input,
    dialect: str | None,
    param,
    parallelism: int | None,
    config,
    env,
    output: str | None,
    report_file: str | None,
    run_id: str | None,
    conn_args,
):
    """Probe and report asset state (watermarks, staleness) without refreshing.

    Read-only: issues watermark/staleness queries against the warehouse but
    never writes. Exit 0 on success, 1 on error.
    """
    validate_dialect(dialect, "state")

    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=PathlibPath(config) if config else None,
        env=env,
    )

    try:
        with report_run(
            "state",
            report_file,
            run_id,
            target=str(input)[:200],
            dialect=dialect,
            parallelism=parallelism,
            config_path=str(config) if config else None,
        ):
            sink = get_report_sink()
            snapshot = compute_state_snapshot(
                cli_params, run_id=sink.run_id if sink else None
            )
            if output:
                write_state_snapshot(snapshot, PathlibPath(output))
            else:
                emit_report("state_snapshot", **snapshot.model_dump())
            _show_snapshot(snapshot)
            emit_report(
                "summary",
                success=True,
                exit_code=0,
                total=snapshot.summary.total,
                succeeded=snapshot.summary.fresh,
                failed=0,
                skipped=0,
                partial_failure=False,
            )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
