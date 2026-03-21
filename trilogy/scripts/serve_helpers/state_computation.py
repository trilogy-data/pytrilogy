"""Compute datasource asset state for the serve command's /state endpoint."""

from pathlib import Path

from trilogy.execution.config import (
    RuntimeConfig,
    apply_env_vars,
    load_config_file,
    load_env_file,
)
from trilogy.execution.state.state_store import BaseStateStore
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.serve_helpers.models import (
    AssetState,
    StateResponse,
    StateSummary,
    WatermarkInfo,
)


def compute_state_sync(
    target_path: Path,
    engine: str,
    config_path: Path | None,
    directory: Path,
) -> StateResponse:
    """Parse a trilogy file, watermark its datasources, and return serialized state.

    Runs synchronously — call via run_in_executor from async contexts.

    Raises:
        ValueError: If no dialect can be determined.
        Exception: Propagated from executor creation or DB queries.
    """
    from trilogy.dialect.enums import Dialects
    from trilogy.scripts.common import create_executor_for_script

    if config_path:
        config = load_config_file(config_path)
        for env_file in config.env_files:
            env_vars = load_env_file(env_file)
            if env_vars:
                apply_env_vars(env_vars)
    else:
        config = RuntimeConfig(startup_trilogy=[], startup_sql=[])

    if engine != "generic":
        edialect = Dialects(engine)
    elif config.engine_dialect:
        edialect = config.engine_dialect
    else:
        raise ValueError(
            "No dialect configured. Set engine.dialect in trilogy.toml or pass an engine to 'trilogy serve'."
        )

    node = ScriptNode(path=target_path)
    exec = create_executor_for_script(
        node=node,
        param=(),
        conn_args=(),
        edialect=edialect,
        debug=False,
        config=config,
    )

    with open(target_path) as f:
        exec.parse_text(f.read())

    state_store = BaseStateStore()
    watermarks = state_store.watermark_all_assets(exec.environment, exec)
    stale_assets = state_store.get_stale_assets(exec.environment, exec)
    stale_map = {a.datasource_id: a.reason for a in stale_assets}

    assets: list[AssetState] = []
    for ds in exec.environment.datasources.values():
        wm = watermarks.get(ds.identifier)
        watermark_info: dict[str, WatermarkInfo] = {}
        if wm:
            for key, update_key in wm.keys.items():
                watermark_info[key] = WatermarkInfo(
                    type=update_key.type.value,
                    value=(
                        str(update_key.value) if update_key.value is not None else None
                    ),
                )

        if ds.identifier in stale_map:
            status: str = "stale"
            stale_reason: str | None = stale_map[ds.identifier]
        elif wm is not None:
            status = "fresh"
            stale_reason = None
        else:
            status = "unknown"
            stale_reason = None

        assets.append(
            AssetState(
                id=ds.identifier,
                is_root=ds.is_root,
                status=status,  # type: ignore[arg-type]
                stale_reason=stale_reason,
                watermarks=watermark_info,
            )
        )

    # Root assets first, then alphabetical
    assets.sort(key=lambda a: (not a.is_root, a.id))

    total = len(assets)
    return StateResponse(
        target=str(target_path.relative_to(directory)).replace("\\", "/"),
        assets=assets,
        summary=StateSummary(
            total=total,
            root=sum(1 for a in assets if a.is_root),
            stale=sum(1 for a in assets if a.status == "stale"),
            fresh=sum(1 for a in assets if a.status == "fresh"),
            unknown=sum(1 for a in assets if a.status == "unknown"),
        ),
    )
