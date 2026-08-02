"""Init command for Trilogy CLI - creates a new default workspace."""

from pathlib import Path

import click
from click import argument, option, pass_context

from trilogy.dialect.enums import Dialects
from trilogy.scripts.click_utils import validate_dialect

# display_core, not the display re-export hub: init writes a handful of files
# and should start instantly, so it skips the execution/refresh display modules.
from trilogy.scripts.display_core import print_error, print_info, print_success
from trilogy.scripts.display_init import show_init_header
from trilogy.scripts.project_config import MODEL_ROOT_DIR

# Default hello world script content
HELLO_WORLD_SCRIPT = """# Welcome to Trilogy!
# This is a simple example script to get you started.

# Define a simple concept
key user_id int;

# Create a sample datasource
datasource users (
    user_id
)
grain (user_id)
query '''
    SELECT 1 as user_id
    UNION ALL
    SELECT 2 as user_id
    UNION ALL
    SELECT 3 as user_id
''';

# Query the data
SELECT
    user_id
;
"""

# Connection parameters accepted under [engine.config], mirroring the per-dialect
# configs in trilogy.dialect.config. Required keys are written uncommented with a
# placeholder so the shape of a working config is visible; optional keys are
# commented out. Dialects absent here take no file-level connection config.
DIALECT_CONNECTION_HINTS: dict[Dialects, list[tuple[str, str, bool]]] = {
    Dialects.DUCK_DB: [
        # db_location resolves relative to this file; omit for an in-memory db.
        ("db_location", '"local.duckdb"', False),
        ("enable_python_datasources", "true", False),
    ],
    Dialects.SQLITE: [
        ("db_location", '"local.sqlite"', False),
    ],
    Dialects.BIGQUERY: [
        # Auth is application-default credentials; project defaults to theirs.
        ("project", '"my-gcp-project"', False),
        ("staging_dataset", '"my_dataset"', False),
        ("staging_uri", '"gs://my-bucket/trilogy-staging"', False),
    ],
    Dialects.SNOWFLAKE: [
        ("account", '"my-account"', True),
        ("username", '"CHANGE_ME"', True),
        ("password", '"CHANGE_ME"', True),
        ("database", '"my_database"', False),
        ("schema", '"public"', False),
    ],
    Dialects.POSTGRES: [
        ("host", '"localhost"', True),
        ("port", "5432", True),
        ("username", '"CHANGE_ME"', True),
        ("password", '"CHANGE_ME"', True),
        ("database", '"postgres"', True),
    ],
    Dialects.MYSQL: [
        ("host", '"localhost"', True),
        ("username", '"CHANGE_ME"', True),
        ("password", '"CHANGE_ME"', True),
        ("database", '"mysql"', True),
        ("port", "3306", False),
        ("charset", '"utf8mb4"', False),
    ],
    Dialects.SQL_SERVER: [
        ("host", '"localhost"', True),
        ("port", "1433", True),
        ("username", '"CHANGE_ME"', True),
        ("password", '"CHANGE_ME"', True),
        ("database", '"master"', True),
    ],
    Dialects.PRESTO: [
        ("host", '"localhost"', True),
        ("port", "8080", True),
        ("username", '"CHANGE_ME"', True),
        ("password", '"CHANGE_ME"', True),
        ("catalog", '"hive"', True),
        ("schema", '"default"', False),
    ],
}


def connection_section(dialect: Dialects) -> list[str]:
    hints = DIALECT_CONNECTION_HINTS.get(dialect)
    if not hints:
        return [
            f"# {dialect.value} takes no connection parameters from this file;",
            "# pass them as CLI arguments instead, e.g.",
            f"#   trilogy run model.preql {dialect.value} --host localhost",
        ]
    lines = ["[engine.config]", f"# Connection parameters for {dialect.value}."]
    if any(key == "password" for key, _, _ in hints):
        lines.append('# Keep secrets out of this file: password = "${env:MY_PASSWORD}"')
    for key, example, required in hints:
        lines.append(f"{key} = {example}" if required else f"# {key} = {example}")
    return lines


def render_config(dialect: Dialects | None) -> str:
    """The trilogy.toml written by init, pinned to *dialect* if one was given."""
    lines = [
        "# Trilogy Configuration File",
        "# Learn more at: https://github.com/trilogy-data/pytrilogy",
        "",
        "[engine]",
        "# Default dialect for execution",
        f'dialect = "{dialect.value}"' if dialect else '# dialect = "duck_db"',
        "",
        "# Max parallelism for multi-script execution",
        "# parallelism = 3",
        "",
    ]
    if dialect:
        lines.extend(connection_section(dialect))
        lines.append("")
    lines.extend(
        [
            "[setup]",
            "# Startup scripts to run before execution",
            "# trilogy = []",
            "# sql = []",
            "",
        ]
    )
    return "\n".join(lines)


def resolve_dialect(path: str, dialect: str | None) -> Dialects | None:
    """Parse the dialect argument, rejecting a dialect passed in the path slot."""
    if dialect is None:
        # `trilogy init bigquery` would otherwise create a workspace in ./bigquery.
        if not Path(path).is_dir():
            try:
                named = Dialects(path)
            except ValueError:
                return None
            raise click.UsageError(
                f"'{path}' is a dialect, not a path. The path comes first.\n"
                f"  Try: trilogy init . {named.value}"
            )
        return None
    validate_dialect(dialect, "init")
    try:
        return Dialects(dialect)
    except ValueError as exc:
        valid = ", ".join(d.value for d in Dialects)
        raise click.UsageError(
            f"'{dialect}' is not a valid dialect. Choose one of: {valid}."
        ) from exc


@argument("path", type=str, required=False, default=".")
@argument("dialect", type=str, required=False, default=None)
@option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help=(
        "Initialize into a directory that already has a trilogy.toml, "
        "overwriting it. Every other file is left alone."
    ),
)
@pass_context
def init(ctx, path: str, dialect: str | None, force: bool):
    """Create a new default Trilogy workspace.

    Initializes a new workspace with default configuration and structure:
    - trilogy.toml: Configuration file
    - root/: Directory for data models (where `trilogy ingest` writes)
    - jobs/: Directory for job scripts
    - hello_world.preql: Example script

    Args:
        path: Path where the workspace should be created (default: current directory)
        dialect: Engine to configure, e.g. `trilogy init . bigquery` (default: unset)
        force: Overwrite an existing trilogy.toml rather than refusing
    """
    edialect = resolve_dialect(path, dialect)
    workspace_path = Path(path).resolve()

    # Check if path already has trilogy files
    config_path = workspace_path / "trilogy.toml"
    config_exists = config_path.exists()

    show_init_header(
        str(workspace_path),
        edialect.value if edialect else None,
        overwrite=config_exists and force,
    )

    if config_exists and not force:
        print_error(
            f"Workspace already initialized at {workspace_path} (trilogy.toml exists). "
            "Use --force to overwrite it."
        )
        raise SystemExit(1)

    # Create base directory if it doesn't exist
    workspace_path.mkdir(parents=True, exist_ok=True)

    # Create subdirectories
    for subdir in [MODEL_ROOT_DIR, "jobs"]:
        subdir_path = workspace_path / subdir
        subdir_path.mkdir(parents=True, exist_ok=True)
        print_info(f"Created directory: {subdir}/")

    # Create trilogy.toml
    config_path.write_text(render_config(edialect))
    verb = "Overwrote" if config_exists else "Created"
    suffix = f" (dialect: {edialect.value})" if edialect else ""
    print_info(f"{verb} configuration: trilogy.toml{suffix}")

    # Create hello_world.preql. An existing one is never clobbered — --force is
    # about the config, and the example may have been edited into a real model.
    hello_world_path = workspace_path / "hello_world.preql"
    if hello_world_path.exists():
        print_info("Kept existing script: hello_world.preql")
    else:
        hello_world_path.write_text(HELLO_WORLD_SCRIPT)
        print_info("Created example script: hello_world.preql")

    next_steps = ""
    if edialect and any(
        required for _, _, required in DIALECT_CONNECTION_HINTS.get(edialect, [])
    ):
        # No square brackets: rich console.print eats them as markup tags.
        next_steps = "  # fill in the engine.config connection details first\n"

    print_success(
        f"\nWorkspace initialized successfully!\n\n"
        f"Get started with:\n"
        f"  cd {workspace_path.name if path != '.' else workspace_path}\n"
        f"{next_steps}"
        f"  trilogy unit hello_world.preql\n"
    )
