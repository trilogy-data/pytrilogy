"""CLI commands for trilogy environment management."""

from pathlib import Path

import click
from click import Path as ClickPath
from click import argument, option, pass_context
from click.exceptions import Exit

from trilogy.dialect.enums import Dialects
from trilogy.execution.address import apply_env_prefix, env_backup_address
from trilogy.execution.config import RuntimeConfig
from trilogy.execution.state.env_manager import EnvironmentManager
from trilogy.scripts.click_utils import validate_dialect
from trilogy.scripts.display import print_error, print_info, print_success


def _get_manager(config: RuntimeConfig, input_path: Path) -> EnvironmentManager:
    project_name = config.project_name or input_path.resolve().name
    return EnvironmentManager(
        project_name=project_name,
        state_home=config.state.home,
    )


def _resolve_env_name(
    cli_env: str | None,
    manager: EnvironmentManager,
) -> str | None:
    """Return explicitly-provided env name, or fall back to active env."""
    if cli_env:
        return cli_env
    return manager.get_active()


def apply_environment_to_executor(executor, env_name: str) -> None:
    """Transform physical addresses of non-root datasources in-place for env isolation."""
    from trilogy.core.models.datasource import Address

    for ds in executor.environment.datasources.values():
        if ds.is_root:
            continue
        if isinstance(ds.address, Address):
            ds.address.location = apply_env_prefix(ds.address.location, env_name)
        else:
            # Bare string address (normalised to Address by validator, but just in case)
            ds.address = Address(location=apply_env_prefix(str(ds.address), env_name))


def _rename_sql(dialect: Dialects, old: str, new: str) -> str:
    """Generate a RENAME statement appropriate for the dialect.

    For most SQL dialects the syntax is:
        ALTER TABLE <schema>.<old_table> RENAME TO <new_table>
    where RENAME TO takes only the unqualified table name.
    SQL Server uses a different stored-procedure form.
    """
    new_table = new.split(".")[-1]
    if dialect == Dialects.SQL_SERVER:
        return f"EXEC sp_rename '{old}', '{new_table}'"
    return f"ALTER TABLE {old} RENAME TO {new_table}"


def _drop_sql(dialect: Dialects, address: str) -> str:
    return f"DROP TABLE IF EXISTS {address}"


def _collect_non_root_addresses(
    input_path: Path,
    cli_params,
    edialect: Dialects,
    config: RuntimeConfig,
    env_name: str,
) -> list[tuple[str, str]]:
    """Return list of (env_address, target_address) for non-root datasources.

    Parses all scripts and deduplicates by physical address.
    """
    from collections import OrderedDict

    from trilogy.scripts.common import create_executor_for_script
    from trilogy.scripts.dependency import ScriptNode

    seen: OrderedDict[str, str] = OrderedDict()

    if input_path.is_dir():
        files = sorted(input_path.glob("**/*.preql"))
    else:
        files = [input_path]

    for file_path in files:
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
        try:
            with open(node.path, "r") as f:
                executor.parse_text(f.read(), root=node.path)
            for ds in executor.environment.datasources.values():
                if ds.is_root:
                    continue
                base_addr = ds.safe_address
                env_addr = apply_env_prefix(base_addr, env_name)
                seen.setdefault(env_addr, base_addr)
        finally:
            executor.close()

    return list(seen.items())


# ── Command group ─────────────────────────────────────────────────────────────


@click.group()
@argument("input", type=ClickPath(), default=".")
@option("--config", type=ClickPath(exists=True), help="Path to trilogy.toml")
@pass_context
def env(ctx, input, config):
    """Manage trilogy environments."""
    ctx.ensure_object(dict)
    ctx.obj["ENV_INPUT"] = input
    ctx.obj["ENV_CONFIG_PATH"] = Path(config) if config else None


@env.command("create")
@argument("name")
@pass_context
def env_create(ctx, name: str):
    """Create a new environment."""
    from trilogy.scripts.common import get_runtime_config

    input_path = Path(ctx.obj["ENV_INPUT"])
    config_path = ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, config_path)
    manager = _get_manager(config, input_path)
    try:
        manager.create(name)
        print_success(f"Created environment '{name}'.")
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e


@env.command("activate")
@argument("name")
@pass_context
def env_activate(ctx, name: str):
    """Set the active environment."""
    from trilogy.scripts.common import get_runtime_config

    input_path = Path(ctx.obj["ENV_INPUT"])
    config_path = ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, config_path)
    manager = _get_manager(config, input_path)
    try:
        manager.activate(name)
        print_success(f"Activated environment '{name}'.")
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e


@env.command("deactivate")
@pass_context
def env_deactivate(ctx):
    """Clear the active environment."""
    from trilogy.scripts.common import get_runtime_config

    input_path = Path(ctx.obj["ENV_INPUT"])
    config_path = ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, config_path)
    manager = _get_manager(config, input_path)
    manager.deactivate()
    print_info("No active environment.")


@env.command("list")
@pass_context
def env_list(ctx):
    """List all environments."""
    from trilogy.scripts.common import get_runtime_config

    input_path = Path(ctx.obj["ENV_INPUT"])
    config_path = ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, config_path)
    manager = _get_manager(config, input_path)
    envs = manager.list_envs()
    active = manager.get_active()
    if not envs:
        print_info("No environments found.")
        return
    for meta in envs:
        marker = " *" if meta.name == active else ""
        print_info(f"  {meta.name}{marker}  (created {meta.created_at[:10]})")


@env.command("delete")
@argument("name")
@option(
    "--drop-assets/--no-drop-assets",
    default=True,
    help="Drop remote assets tracked by this environment",
)
@option("--dialect", type=str, default=None)
@option("--param", multiple=True)
@option("--env", "-e", "env_vars", multiple=True)
@option("--config-path", type=ClickPath(exists=True), default=None)
@pass_context
def env_delete(
    ctx, name: str, drop_assets: bool, dialect, param, env_vars, config_path
):
    """Delete an environment and optionally drop its remote assets."""
    from trilogy.execution.config import apply_env_vars
    from trilogy.scripts.common import (
        CLIRuntimeParams,
        create_executor,
        get_runtime_config,
        merge_runtime_config,
    )
    from trilogy.scripts.environment import parse_env_vars

    input_path = Path(ctx.obj["ENV_INPUT"])
    cfg_path = Path(config_path) if config_path else ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, cfg_path)
    manager = _get_manager(config, input_path)

    drop_fn = None
    if drop_assets:
        try:
            meta = manager.get_meta(name)
        except ValueError as e:
            print_error(str(e))
            raise Exit(1) from e

        if meta.tracked_assets:
            cli_params = CLIRuntimeParams(
                input=str(input_path),
                dialect=Dialects(dialect) if dialect else None,
                param=param,
                env=env_vars,
                debug=ctx.obj.get("DEBUG", False),
                debug_file=ctx.obj.get("DEBUG_FILE"),
                config_path=cfg_path,
            )
            edialect, _ = merge_runtime_config(cli_params, config)
            if env_vars:
                try:
                    apply_env_vars(parse_env_vars(env_vars))
                except ValueError as e:
                    print_error(str(e))
                    raise Exit(1) from e
            exec_ = create_executor(
                param,
                input_path if input_path.is_dir() else input_path.parent,
                cli_params.conn_args,
                edialect,
                cli_params.debug,
                config,
                cli_params.debug_file,
            )
            try:
                exec_.connect()

                def drop_fn(addresses: list[str]) -> None:
                    for addr in addresses:
                        sql = _drop_sql(edialect, addr)
                        try:
                            exec_.execute_raw_sql(sql)
                            print_info(f"  Dropped {addr}")
                        except Exception as drop_exc:
                            print_error(f"  Could not drop {addr}: {drop_exc}")

            except Exception as e:
                print_error(f"Could not connect to drop assets: {e}")
                raise Exit(1) from e

    try:
        manager.delete(name, drop_assets_fn=drop_fn)
        print_success(f"Deleted environment '{name}'.")
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e


@env.command("publish")
@argument("name")
@argument("dialect", type=str, required=False)
@option("--param", multiple=True)
@option("--env", "-e", "env_vars", multiple=True)
@option(
    "--dry-run",
    "-n",
    is_flag=True,
    default=False,
    help="Show rename operations without executing",
)
@argument("conn_args", nargs=-1, type=click.UNPROCESSED)
@pass_context
def env_publish(ctx, name: str, dialect, param, env_vars, dry_run, conn_args):
    """Atomically promote an environment's assets to production.

    Performs a two-phase rename:
      1. Rename each current production asset to a backup.
      2. Rename each environment asset to the production name.
    Drops backups on full success; rolls back on failure.
    """
    from trilogy.execution.config import apply_env_vars
    from trilogy.scripts.common import (
        CLIRuntimeParams,
        create_executor,
        get_runtime_config,
        merge_runtime_config,
    )
    from trilogy.scripts.environment import parse_env_vars

    input_path = Path(ctx.obj["ENV_INPUT"])
    cfg_path = ctx.obj["ENV_CONFIG_PATH"]
    config = get_runtime_config(input_path, cfg_path)
    manager = _get_manager(config, input_path)

    # Verify environment exists
    try:
        manager.get_meta(name)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    cli_params = CLIRuntimeParams(
        input=str(input_path),
        dialect=Dialects(dialect) if dialect else None,
        param=param,
        conn_args=conn_args,
        env=env_vars,
        debug=ctx.obj.get("DEBUG", False),
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=cfg_path,
    )
    validate_dialect(dialect, "env publish")

    try:
        edialect, _ = merge_runtime_config(cli_params, config)
    except Exit:
        raise

    if env_vars:
        try:
            apply_env_vars(parse_env_vars(env_vars))
        except ValueError as e:
            print_error(str(e))
            raise Exit(1) from e

    # Collect non-root datasource address pairs
    pairs = _collect_non_root_addresses(input_path, cli_params, edialect, config, name)
    if not pairs:
        print_info("No non-root assets found to publish.")
        return

    exec_ = create_executor(
        param,
        input_path if input_path.is_dir() else input_path.parent,
        conn_args,
        edialect,
        cli_params.debug,
        config,
        cli_params.debug_file,
    )
    try:
        exec_.connect()
        _run_publish(exec_, edialect, pairs, manager, name, dry_run)
    finally:
        exec_.close()


def _run_publish(
    exec_,
    dialect: Dialects,
    pairs: list[tuple[str, str]],
    manager: EnvironmentManager,
    env_name: str,
    dry_run: bool,
) -> None:
    """Two-phase commit: backup current prod → promote env assets → drop backups."""
    backups: list[tuple[str, str]] = []  # (backup_addr, target_addr)

    def run(sql: str) -> None:
        if dry_run:
            print_info(f"  [dry-run] {sql}")
        else:
            exec_.execute_raw_sql(sql)

    # ── Phase 1: rename prod → backup ────────────────────────────────────────
    print_info("Phase 1: backing up production assets...")
    for env_addr, target_addr in pairs:
        backup_addr = env_backup_address(target_addr)
        sql = _rename_sql(dialect, target_addr, backup_addr)
        try:
            run(sql)
            backups.append((backup_addr, target_addr))
            print_info(f"  {target_addr} → {backup_addr}")
        except Exception as e:
            # If the target doesn't exist yet that's fine — first publish
            if _is_missing_table_error(e):
                print_info(f"  {target_addr} does not exist yet, skipping backup")
            else:
                print_error(f"Phase 1 failed on {target_addr}: {e}")
                _rollback(exec_, dialect, backups, dry_run)
                raise Exit(1) from e

    # ── Phase 2: rename env → prod ───────────────────────────────────────────
    print_info("Phase 2: promoting environment assets...")
    promoted: list[tuple[str, str]] = []  # (env_addr, target_addr)
    for env_addr, target_addr in pairs:
        sql = _rename_sql(dialect, env_addr, target_addr)
        try:
            run(sql)
            promoted.append((env_addr, target_addr))
            print_info(f"  {env_addr} → {target_addr}")
        except Exception as e:
            print_error(f"Phase 2 failed on {env_addr}: {e}")
            # Rollback phase 2 (un-rename promoted assets)
            for promoted_env, promoted_target in reversed(promoted):
                try:
                    rollback_sql = _rename_sql(dialect, promoted_target, promoted_env)
                    exec_.execute_raw_sql(rollback_sql)
                except Exception:
                    pass
            _rollback(exec_, dialect, backups, dry_run)
            raise Exit(1) from e

    if dry_run:
        print_info("[dry-run] Publish complete — no changes made.")
        return

    # ── Cleanup: drop backups ────────────────────────────────────────────────
    print_info("Cleaning up backups...")
    for backup_addr, _ in backups:
        try:
            exec_.execute_raw_sql(_drop_sql(dialect, backup_addr))
            print_info(f"  Dropped {backup_addr}")
        except Exception as e:
            print_error(
                f"  Could not drop backup {backup_addr}: {e} (manual cleanup needed)"
            )

    # Copy env state → main state
    env_wm = manager.load_state(env_name)
    if env_wm:
        manager.save_main_state(env_wm)

    # Clear tracked assets from env (they're now prod)
    try:
        meta = manager.get_meta(env_name)
        meta.tracked_assets = []
        manager._save_meta(meta)
    except Exception:
        pass

    print_success(f"Published environment '{env_name}' to production.")


def _rollback(
    exec_, dialect: Dialects, backups: list[tuple[str, str]], dry_run: bool
) -> None:
    """Restore backed-up assets to their original names."""
    if not backups:
        return
    print_info("Rolling back...")
    for backup_addr, target_addr in reversed(backups):
        sql = _rename_sql(dialect, backup_addr, target_addr)
        try:
            if not dry_run:
                exec_.execute_raw_sql(sql)
            print_info(f"  Restored {backup_addr} → {target_addr}")
        except Exception as rb_exc:
            print_error(f"  Rollback of {backup_addr} failed: {rb_exc}")


def _is_missing_table_error(e: Exception) -> bool:
    """Heuristic: does this error indicate the table simply doesn't exist?"""
    msg = str(e).lower()
    return any(
        phrase in msg
        for phrase in (
            "does not exist",
            "not found",
            "unknown table",
            "no such table",
            "doesn't exist",
        )
    )
