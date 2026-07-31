"""Asset state for the serve command's /state endpoint.

Serve does NOT compute state of its own, and does NOT have its own shape for it:
it calls the same :func:`~trilogy.scripts.state.compute_state_snapshot` the CLI
does and returns the ``StateSnapshot`` verbatim. That snapshot is the
interchange format shared by the studio UI, the cloud service, and the local
CLI's state files — two computations, or two renderings, of "the same" state is
how consumers come to disagree (see the slice-aware refresh note in
``execution/state/AGENTS.md``).
"""

from pathlib import Path

from trilogy.execution.config import RuntimeConfig, load_config_file
from trilogy.execution.state.snapshot import StateSnapshot
from trilogy.execution.state.state_store import StateStore


def compute_state_snapshot_sync(
    target_path: Path,
    engine: str,
    config_path: Path | None,
    directory: Path,
    state_store: "StateStore | None" = None,
) -> StateSnapshot:
    """State snapshot for a served file OR directory.

    Delegates to :func:`~trilogy.scripts.state.compute_state_snapshot`, the same
    entrypoint ``trilogy state`` uses — so serve gets the directory probe (which
    resolves cross-script ownership and dedupes by physical address) for free,
    and cannot drift from the CLI.

    Runs synchronously — call via run_in_executor from async contexts.

    Raises:
        ValueError: If no dialect can be determined.
        Exception: Propagated from executor creation or DB queries.
    """
    from trilogy.dialect.enums import Dialects
    from trilogy.execution.state.state_store import state_store_factory
    from trilogy.scripts.common import CLIRuntimeParams
    from trilogy.scripts.state import compute_state_snapshot

    if config_path:
        config = load_config_file(config_path)
    else:
        config = RuntimeConfig(startup_trilogy=[], startup_sql=[])

    # Resolved here rather than left to merge_runtime_config, which exits the
    # process on a missing dialect — a server needs a 400, not a shutdown.
    if engine != "generic":
        edialect = Dialects(engine)
    elif config.engine_dialect:
        edialect = config.engine_dialect
    else:
        raise ValueError(
            "No dialect configured. Set engine.dialect in trilogy.toml or pass an engine to 'trilogy serve'."
        )

    cli_params = CLIRuntimeParams(
        input=str(target_path),
        dialect=edialect,
        config_path=config_path,
    )
    factory = (lambda cache: state_store) if state_store is not None else None
    with state_store_factory(factory):
        snapshot = compute_state_snapshot(cli_params)
    # The served target is relative to the served directory; the CLI records the
    # path it was invoked with.
    return snapshot.model_copy(
        update={"target": relative_target(target_path, directory)}
    )


def relative_target(target_path: Path, directory: Path) -> str:
    """The served target's identity: its path relative to the served directory.

    Also the state cache's key, so it must stay stable for a given target
    regardless of how the client spelled it (``.`` and ``./`` resolve alike).
    """
    return str(target_path.relative_to(directory)).replace("\\", "/")
