"""CLI for deployment environments: ``trilogy env ...``.

An environment is a prefixed parallel build of a project's managed tables
(``orders`` -> ``dev_orders``). Build into it with ``trilogy run/refresh
--environment dev`` (or ``trilogy env activate dev``), verify, then promote to
production with ``trilogy env publish dev`` — a two-phase rename cutover with
rollback on failure.
"""

import os
from dataclasses import dataclass
from pathlib import Path

import click
from click import UNPROCESSED, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.constants import logger
from trilogy.core.models.datasource import Address
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig
from trilogy.execution.envs import (
    RENAMEABLE_TYPES,
    EnvActivation,
    EnvironmentManager,
    apply_env_prefix,
    env_backup_address,
    is_remote_location,
    parse_tracked_entry,
    rewritable_address,
)
from trilogy.scripts.click_utils import dry_run_option, validate_dialect
from trilogy.scripts.common import (
    CLIRuntimeParams,
    create_executor,
    get_runtime_config,
    merge_runtime_config,
    resolve_input,
)
from trilogy.scripts.display import (
    print_error,
    print_info,
    print_success,
    print_warning,
)


def _anchor_dir(input_str: str) -> Path:
    """Directory used for config discovery and the project-name fallback.

    Inline query text is not a path (and can be an invalid one on Windows);
    anything that isn't an existing path anchors to the working directory.
    """
    try:
        candidate = Path(input_str)
        if candidate.is_file():
            return candidate.parent
        if candidate.is_dir():
            return candidate
    except OSError:
        pass
    return Path(".")


def manager_for(config: RuntimeConfig, anchor: Path) -> EnvironmentManager:
    project = config.project_name or (
        config.source_path.parent.resolve().name
        if config.source_path
        else anchor.resolve().name
    )
    return EnvironmentManager(project, home=config.environments_home)


def resolve_activation(
    environment_flag: str | None,
    input_str: str,
    config_path: Path | None,
) -> EnvActivation | None:
    """The environment a command should build into: flag > activated > None.

    An explicitly-flagged environment is registered on first use, so cloud
    orchestrators can pass ``--environment <label>`` without a create step.
    """
    anchor = _anchor_dir(input_str)
    config = get_runtime_config(anchor, config_path)
    manager = manager_for(config, anchor)
    name = environment_flag or manager.get_active()
    if not name:
        return None
    manager.ensure(name)
    return EnvActivation(name=name, manager=manager)


def announce_activation(activation: EnvActivation | None) -> None:
    if activation:
        print_info(
            f"Deployment environment '{activation.name}': managed tables "
            f"prefixed '{activation.name}_'."
        )


@dataclass(frozen=True)
class PublishAsset:
    """One cutover unit: a table renamed via SQL, or a local file renamed on
    the filesystem (``os.replace``, atomic on one volume). File addresses are
    stored resolved-absolute — they are script-relative in the model."""

    env_address: str
    prod_address: str
    is_file: bool = False


def _rename_sql(dialect: Dialects, old: str, new: str) -> str:
    """RENAME TO takes the unqualified table name on standard dialects."""
    new_table = new.split(".")[-1]
    if dialect == Dialects.SQL_SERVER:
        return f"EXEC sp_rename '{old}', '{new_table}'"
    return f"ALTER TABLE {old} RENAME TO {new_table}"


def _drop_sql(address: str) -> str:
    return f"DROP TABLE IF EXISTS {address}"


def _asset_exists(executor: Executor, asset: PublishAsset, address: str) -> bool:
    if asset.is_file:
        return Path(address).exists()
    return _table_exists(executor, address)


def _rename_asset(
    executor: Executor, dialect: Dialects, asset: PublishAsset, old: str, new: str
) -> None:
    if asset.is_file:
        os.replace(old, new)
    else:
        executor.execute_raw_sql(_rename_sql(dialect, old, new))


def _drop_asset(executor: Executor, asset: PublishAsset, address: str) -> None:
    if asset.is_file:
        Path(address).unlink(missing_ok=True)
    else:
        executor.execute_raw_sql(_drop_sql(address))


def _describe_rename(dialect: Dialects, asset: PublishAsset, old: str, new: str) -> str:
    if asset.is_file:
        return f"rename {old} -> {new}"
    return _rename_sql(dialect, old, new)


def _table_exists(executor: Executor, address: str) -> bool:
    try:
        executor.execute_raw_sql(f"SELECT 1 FROM {address} WHERE 1=0")
        return True
    except Exception:
        _try_rollback(executor)
        return False


def _try_rollback(executor: Executor) -> None:
    """Clear an aborted transaction (Postgres poisons the connection otherwise)."""
    try:
        executor.execute_raw_sql("ROLLBACK")
    except Exception as e:
        logger.debug(f"Rollback after failed probe was a no-op: {e}")


def _classify_publish_address(
    address: Address,
    script_dir: Path,
    env_name: str,
    unsupported: list[str],
) -> PublishAsset | None:
    """A PublishAsset for one rewritable address, or None when rejected.

    Tables cut over by SQL rename. Local single-file outputs cut over by
    filesystem rename. Everything else the build namespaced but publish cannot
    promote atomically — remote objects (no atomic rename), globs, file
    arrays, split read/write locations — is collected as unsupported so the
    command aborts before touching anything.
    """
    if address.type in RENAMEABLE_TYPES:
        base = address.location
        return PublishAsset(
            env_address=apply_env_prefix(base, env_name), prod_address=base
        )
    location = address.write_location or address.location
    if (
        is_remote_location(location)
        or address.is_glob
        or address.additional_locations
        or (address.write_location and address.write_location != address.location)
    ):
        unsupported.append(location)
        return None
    prod_path = Path(location)
    if not prod_path.is_absolute():
        prod_path = script_dir / prod_path
    prod = str(prod_path.resolve())
    return PublishAsset(
        env_address=apply_env_prefix(prod, env_name, is_file=True),
        prod_address=prod,
        is_file=True,
    )


def _collect_publish_assets(
    input_path: Path,
    cli_params: CLIRuntimeParams,
    edialect: Dialects,
    config: RuntimeConfig,
    env_name: str,
) -> list[PublishAsset]:
    """Cutover units for every managed asset, deduplicated by prod address.

    Parses the current scripts — the cutover promotes what the code of today
    defines, not what an older build happened to create. Classification goes
    through ``rewritable_address``, the same predicate the build's transform
    uses: publish must consider exactly what a build would have prefixed.
    """
    from trilogy.core.models.datasource import Datasource
    from trilogy.core.statements.execute import ProcessedQueryPersist
    from trilogy.scripts.common import create_executor_for_script
    from trilogy.scripts.dependency import ScriptNode

    assets: dict[str, PublishAsset] = {}
    unsupported: list[str] = []

    def consider(ds: Datasource, script_dir: Path) -> None:
        address = rewritable_address(ds)
        if address is None:
            return
        asset = _classify_publish_address(address, script_dir, env_name, unsupported)
        if asset is not None:
            assets.setdefault(asset.prod_address, asset)

    for file_path in resolve_input(input_path):
        node = ScriptNode(path=file_path)
        executor = create_executor_for_script(
            node,
            cli_params.param,
            cli_params.conn_args,
            edialect,
            cli_params.debug,
            config,
            cli_params.debug_file,
        )
        script_dir = node.path.parent
        try:
            with open(node.path, "r", encoding="utf-8") as f:
                queries = executor.parse_text(f.read(), root=node.path)
            for ds in executor.environment.datasources.values():
                consider(ds, script_dir)
            # Persist targets without a declared datasource are only carried
            # on the processed statement, not registered in the environment.
            for q in queries:
                if isinstance(q, ProcessedQueryPersist):
                    consider(q.datasource, script_dir)
        finally:
            executor.close()

    if unsupported:
        print_error(
            "Publish cannot promote these managed outputs (remote, glob, "
            "multi-file, or split read/write addresses are not yet supported): "
            f"{', '.join(sorted(set(unsupported)))}"
        )
        raise Exit(1)
    return list(assets.values())


@click.group("env")
def env() -> None:
    """Manage deployment environments (namespaced builds + publish cutover)."""


def _resolve_env_name(manager: EnvironmentManager, name: str | None) -> str:
    resolved = name or manager.get_active()
    if not resolved:
        print_error("No environment named and none activated.")
        raise Exit(2)
    return resolved


def _current_code_fingerprint(input: str, param: tuple[str, ...], config):
    from trilogy.execution.model_fingerprint import build_project_fingerprint
    from trilogy.scripts.environment import parse_env_params
    from trilogy.scripts.state import project_root_for

    input_path = Path(input)
    files = resolve_input(input_path)
    anchor = _anchor_dir(input)
    project_root = project_root_for(config, anchor)
    env_params = parse_env_params(param)
    return build_project_fingerprint(files, project_root, env_params)


def record_env_fingerprint(
    activation: EnvActivation,
    input: str,
    param: tuple[str, ...],
    config_path: Path | None = None,
) -> None:
    """Record the current code's fingerprint for the scripts just built.

    Called after a successful run/refresh under an environment; only the
    scripts in scope are updated. Never fails the command it rides on, and
    silently skips inline-query inputs (no script to fingerprint).
    """
    from trilogy.execution.model_fingerprint import update_project_fingerprint

    try:
        if not Path(input).exists():
            return
    except OSError:
        return
    try:
        config = get_runtime_config(_anchor_dir(input), config_path)
        fingerprint = _current_code_fingerprint(input, param, config)
        if fingerprint.scripts:
            update_project_fingerprint(activation.manager, activation.name, fingerprint)
    except Exception as e:
        logger.warning(f"Could not record environment fingerprint: {e}")


def load_env_model_baseline(activation: EnvActivation) -> dict[str, str] | None:
    """The env's recorded fingerprint as a staleness baseline, or None.

    Installed around a refresh so the state store treats datasources whose
    current definition differs from what the env was built with as stale —
    watermark comparison alone cannot see a model change. Never fails the
    command it rides on.
    """
    from trilogy.execution.model_fingerprint import (
        fingerprint_baseline,
        load_project_fingerprint,
    )

    try:
        recorded = load_project_fingerprint(activation.manager, activation.name)
    except Exception as e:
        logger.debug(f"Model baseline unavailable: {e}")
        return None
    if recorded is None:
        return None
    return fingerprint_baseline(recorded) or None


def _show_section_diff(label: str, section) -> None:
    for key in section.added:
        print_info(f"    + {label} {key}")
    for key in section.removed:
        print_info(f"    - {label} {key}")
    for old, new in section.renamed.items():
        print_info(f"    ~ {label} {old} -> {new} (renamed, no rebuild)")
    for key, kind in section.changed.items():
        print_info(f"    ~ {label} {key} ({kind.value})")


def _show_project_diff(diff) -> None:
    for script in diff.added_scripts:
        print_info(f"  + script {script}")
    for script in diff.removed_scripts:
        print_info(f"  - script {script}")
    for script, script_diff in diff.changed_scripts.items():
        print_info(f"  ~ script {script}")
        _show_section_diff("concept", script_diff.concepts)
        _show_section_diff("datasource", script_diff.datasources)
        if script_diff.extras_changed:
            print_info("    ~ merges/functions/types changed")
    if diff.invalidated_datasources:
        print_warning("Datasources needing rebuild:")
        for entry in diff.invalidated_datasources:
            location = f" @ {entry.location}" if entry.location else ""
            print_warning(
                f"  {entry.datasource_id}{location} "
                f"[{entry.script}] ({entry.kind.value})"
            )
    else:
        print_info("No datasources need rebuild.")


@env.command("fingerprint")
@argument("name", required=False, default=None)
@argument("input", type=click.Path(), default=".")
@option("--param", multiple=True)
@option("--config", "config_path", type=click.Path(exists=True), default=None)
def env_fingerprint(name: str | None, input: str, param, config_path: str | None):
    """Record the current model code as an environment's fingerprint.

    Normally recorded automatically by a successful run/refresh under the
    environment; use this to (re)record explicitly. NAME defaults to the
    activated environment.
    """
    from trilogy.execution.model_fingerprint import update_project_fingerprint

    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager = manager_for(config, anchor)
    env_name = _resolve_env_name(manager, name)
    manager.ensure(env_name)
    fingerprint = _current_code_fingerprint(input, param, config)
    update_project_fingerprint(manager, env_name, fingerprint)
    print_success(
        f"Recorded fingerprint for '{env_name}': {len(fingerprint.scripts)} "
        f"script(s), root {fingerprint.root[:12]}."
    )


@env.command("diff")
@argument("name", required=False, default=None)
@argument("input", type=click.Path(), default=".")
@option(
    "--against",
    default=None,
    help="Compare against this environment's recorded fingerprint instead of "
    "the current code",
)
@option("--param", multiple=True)
@option("--config", "config_path", type=click.Path(exists=True), default=None)
@option("--json", "as_json", is_flag=True, default=False, help="Emit the diff as JSON")
def env_diff(
    name: str | None,
    input: str,
    against: str | None,
    param,
    config_path: str | None,
    as_json: bool,
):
    """Diff an environment's recorded model against the current code.

    With --against, diffs two environments' recorded models instead. Exit
    codes: 0 = identical, 1 = different, 2 = error.
    """
    from trilogy.core.fingerprint import FingerprintError
    from trilogy.execution.model_fingerprint import (
        diff_project_fingerprints,
        load_project_fingerprint,
    )

    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager = manager_for(config, anchor)
    env_name = _resolve_env_name(manager, name)

    def recorded(label: str):
        try:
            fingerprint = load_project_fingerprint(manager, label)
        except FingerprintError as e:
            print_error(str(e))
            raise Exit(2) from e
        if fingerprint is None:
            print_error(
                f"Environment '{label}' has no recorded fingerprint. Run "
                f"'trilogy env fingerprint {label}' or a run/refresh under it."
            )
            raise Exit(2)
        return fingerprint

    base = recorded(env_name)
    if against:
        other = recorded(against)
        other_label = f"environment '{against}'"
    else:
        try:
            other = _current_code_fingerprint(input, param, config)
        except FileNotFoundError as e:
            print_error(str(e))
            raise Exit(2) from e
        other_label = "current code"

    diff = diff_project_fingerprints(base, other)
    if as_json:
        click.echo(diff.model_dump_json(indent=2))
    elif diff.identical:
        print_success(f"'{env_name}' is identical to {other_label}.")
    else:
        print_info(f"'{env_name}' vs {other_label}:")
        _show_project_diff(diff)
    if not diff.identical:
        raise Exit(1)


@env.command("create")
@argument("name")
@argument("input", type=click.Path(), default=".")
@option("--config", "config_path", type=click.Path(exists=True), default=None)
def env_create(name: str, input: str, config_path: str | None):
    """Create a new deployment environment."""
    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager = manager_for(config, anchor)
    try:
        manager.create(name)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e
    print_success(f"Created environment '{name}'.")


@env.command("list")
@argument("input", type=click.Path(), default=".")
@option("--config", "config_path", type=click.Path(exists=True), default=None)
def env_list(input: str, config_path: str | None):
    """List deployment environments."""
    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager = manager_for(config, anchor)
    envs = manager.list_envs()
    if not envs:
        print_info("No environments found.")
        return
    active = manager.get_active()
    for meta in envs:
        marker = " (active)" if meta.name == active else ""
        print_info(
            f"  {meta.name}{marker}  created {meta.created_at[:10]}, "
            f"{len(meta.tracked_assets)} tracked asset(s)"
        )


@env.command("activate")
@argument("name")
@argument("input", type=click.Path(), default=".")
@option("--config", "config_path", type=click.Path(exists=True), default=None)
def env_activate(name: str, input: str, config_path: str | None):
    """Make an environment the default for run/refresh in this project."""
    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager = manager_for(config, anchor)
    try:
        manager.activate(name)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e
    print_success(f"Activated environment '{name}'.")


@env.command("deactivate")
@argument("input", type=click.Path(), default=".")
@option("--config", "config_path", type=click.Path(exists=True), default=None)
def env_deactivate(input: str, config_path: str | None):
    """Clear the activated environment."""
    anchor = _anchor_dir(input)
    config = get_runtime_config(anchor, Path(config_path) if config_path else None)
    manager_for(config, anchor).deactivate()
    print_info("No active environment.")


@env.command("delete", context_settings={"ignore_unknown_options": True})
@argument("name")
@argument("input", type=click.Path(), default=".")
@argument("dialect", type=str, required=False)
@option("--param", multiple=True)
@option("--config", "config_path", type=click.Path(exists=True), default=None)
@option(
    "--drop-assets/--no-drop-assets",
    default=True,
    help="Drop the environment's tracked warehouse tables before deregistering",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def env_delete(
    ctx,
    name: str,
    input: str,
    dialect: str | None,
    param,
    config_path: str | None,
    drop_assets: bool,
    conn_args,
):
    """Delete an environment and (by default) drop its warehouse tables."""
    validate_dialect(dialect, "env delete")
    anchor = _anchor_dir(input)
    cfg_path = Path(config_path) if config_path else None
    config = get_runtime_config(anchor, cfg_path)
    manager = manager_for(config, anchor)
    try:
        meta = manager.get_meta(name)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    if drop_assets and meta.tracked_assets:
        tracked = [parse_tracked_entry(entry) for entry in meta.tracked_assets]
        for kind, address in tracked:
            if kind == "file":
                Path(address).unlink(missing_ok=True)
                print_info(f"  Removed {address}")
        table_addresses = [addr for kind, addr in tracked if kind == "table"]
        if table_addresses:
            cli_params = CLIRuntimeParams(
                input=input,
                dialect=Dialects(dialect) if dialect else None,
                param=param,
                conn_args=conn_args,
                debug=ctx.obj.get("DEBUG", False),
                debug_file=ctx.obj.get("DEBUG_FILE"),
                config_path=cfg_path,
            )
            edialect, _ = merge_runtime_config(cli_params, config)
            executor = create_executor(
                param, anchor, conn_args, edialect, cli_params.debug, config
            )
            try:
                for address in table_addresses:
                    executor.execute_raw_sql(_drop_sql(address))
                    print_info(f"  Dropped {address}")
            finally:
                executor.close()

    manager.delete(name)
    print_success(f"Deleted environment '{name}'.")


@env.command("publish", context_settings={"ignore_unknown_options": True})
@argument("name")
@argument("input", type=click.Path(), default=".")
@argument("dialect", type=str, required=False)
@option("--param", multiple=True)
@option("--config", "config_path", type=click.Path(exists=True), default=None)
@dry_run_option("Show the cutover plan without executing")
@option(
    "--keep-backups",
    is_flag=True,
    default=False,
    help="Keep the pre-cutover production tables as *__pub_backup",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def env_publish(
    ctx,
    name: str,
    input: str,
    dialect: str | None,
    param,
    config_path: str | None,
    dry_run: bool,
    keep_backups: bool,
    conn_args,
):
    """Promote an environment's tables to production via rename cutover.

    Two phases: current production tables are renamed to backups, then the
    environment's tables are renamed into their place. Any failure rolls the
    cutover back; backups are dropped on success unless --keep-backups.
    """
    validate_dialect(dialect, "env publish")
    anchor = _anchor_dir(input)
    cfg_path = Path(config_path) if config_path else None
    config = get_runtime_config(anchor, cfg_path)
    manager = manager_for(config, anchor)
    manager.ensure(name)

    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj.get("DEBUG", False),
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=cfg_path,
    )
    edialect, _ = merge_runtime_config(cli_params, config)

    input_path = Path(input)
    assets = _collect_publish_assets(input_path, cli_params, edialect, config, name)
    if not assets:
        print_info("No managed assets found to publish.")
        return

    executor = create_executor(
        param, anchor, conn_args, edialect, cli_params.debug, config
    )
    try:
        missing = [
            asset.env_address
            for asset in assets
            if not _asset_exists(executor, asset, asset.env_address)
        ]
        if len(missing) == len(assets):
            print_error(
                f"Environment '{name}' has no built assets — run "
                f"'trilogy refresh --environment {name}' first."
            )
            raise Exit(1)
        if missing:
            print_error(
                "Aborting: environment is missing built tables for: "
                + ", ".join(missing)
            )
            raise Exit(1)
        _run_publish(executor, edialect, assets, dry_run, keep_backups)
    finally:
        executor.close()

    if not dry_run:
        manager.clear_tracked_assets(name)
        print_success(f"Published environment '{name}' to production.")


def _run_publish(
    executor: Executor,
    dialect: Dialects,
    assets: list[PublishAsset],
    dry_run: bool,
    keep_backups: bool,
) -> None:
    def run(asset: PublishAsset, old: str, new: str) -> None:
        if dry_run:
            print_info(f"  [dry-run] {_describe_rename(dialect, asset, old, new)}")
        else:
            _rename_asset(executor, dialect, asset, old, new)

    backups: list[tuple[str, PublishAsset]] = []  # (backup_addr, asset)
    print_info("Phase 1: backing up production assets...")
    for asset in assets:
        prod_addr = asset.prod_address
        if not _asset_exists(executor, asset, prod_addr):
            print_info(f"  {prod_addr} does not exist yet (first publish)")
            continue
        backup_addr = env_backup_address(prod_addr)
        if not dry_run:
            # A stale backup from an interrupted publish blocks the rename.
            _drop_asset(executor, asset, backup_addr)
        try:
            run(asset, prod_addr, backup_addr)
            backups.append((backup_addr, asset))
            print_info(f"  {prod_addr} -> {backup_addr}")
        except Exception as e:
            print_error(f"Phase 1 failed on {prod_addr}: {e}")
            _try_rollback(executor)
            _restore_backups(executor, dialect, backups)
            raise Exit(1) from e

    print_info("Phase 2: promoting environment assets...")
    promoted: list[PublishAsset] = []
    for asset in assets:
        try:
            run(asset, asset.env_address, asset.prod_address)
            promoted.append(asset)
            print_info(f"  {asset.env_address} -> {asset.prod_address}")
        except Exception as e:
            print_error(f"Phase 2 failed on {asset.env_address}: {e}")
            _try_rollback(executor)
            for done in reversed(promoted):
                _attempt(executor, dialect, done, done.prod_address, done.env_address)
            _restore_backups(executor, dialect, backups)
            raise Exit(1) from e

    if dry_run:
        print_info("[dry-run] Publish plan complete — no changes made.")
        return

    if keep_backups:
        for backup_addr, _ in backups:
            print_info(f"  Kept backup {backup_addr}")
        return
    for backup_addr, asset in backups:
        try:
            _drop_asset(executor, asset, backup_addr)
        except Exception as e:
            print_warning(f"  Could not drop backup {backup_addr}: {e} (drop manually)")


def _restore_backups(
    executor: Executor, dialect: Dialects, backups: list[tuple[str, PublishAsset]]
) -> None:
    if not backups:
        return
    print_info("Rolling back...")
    for backup_addr, asset in reversed(backups):
        if _attempt(executor, dialect, asset, backup_addr, asset.prod_address):
            print_info(f"  Restored {backup_addr} -> {asset.prod_address}")
        else:
            print_error(f"  MANUAL FIX NEEDED: could not restore {backup_addr}")


def _attempt(
    executor: Executor, dialect: Dialects, asset: PublishAsset, old: str, new: str
) -> bool:
    try:
        _rename_asset(executor, dialect, asset, old, new)
        return True
    except Exception:
        _try_rollback(executor)
        return False
