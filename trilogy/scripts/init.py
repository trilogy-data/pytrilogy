"""Init command for Trilogy CLI - creates a new default workspace."""

from pathlib import Path

from click import argument, pass_context

from trilogy.scripts.display import print_error, print_info, print_success

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

# Default trilogy.toml content
DEFAULT_CONFIG = """# Trilogy Configuration File
# Learn more at: https://github.com/trilmhmogy-data/pytrilogy

[engine]
# Default dialect for execution
# dialect = "duck_db"

# Max parallelism for multi-script execution
# parallelism = 3


[setup]
# Startup scripts to run before execution
# trilogy = []
# sql = []
"""


@argument("path", type=str, required=False, default=".")
@pass_context
def init(ctx, path: str):
    """Create a new default Trilogy workspace.

    Initializes a new workspace with default configuration and structure:
    - trilogy.toml: Configuration file
    - raw/: Directory for raw data models
    - derived/: Directory for derived data models
    - jobs/: Directory for job scripts
    - hello_world.preql: Example script

    Args:
        path: Path where the workspace should be created (default: current directory)
    """
    workspace_path = Path(path).resolve()

    print_info(f"Initializing Trilogy workspace at: {workspace_path}")

    # Check if path already has trilogy files
    if (workspace_path / "trilogy.toml").exists():
        print_error(
            f"Workspace already initialized at {workspace_path} (trilogy.toml exists)"
        )
        raise SystemExit(1)

    # Create base directory if it doesn't exist
    workspace_path.mkdir(parents=True, exist_ok=True)

    # Create subdirectories
    subdirs = ["raw", "derived", "jobs"]
    for subdir in subdirs:
        subdir_path = workspace_path / subdir
        subdir_path.mkdir(exist_ok=True)
        print_info(f"Created directory: {subdir}/")

    # Create trilogy.toml
    config_path = workspace_path / "trilogy.toml"
    config_path.write_text(DEFAULT_CONFIG)
    print_info("Created configuration: trilogy.toml")

    # Create hello_world.preql
    hello_world_path = workspace_path / "hello_world.preql"
    hello_world_path.write_text(HELLO_WORLD_SCRIPT)
    print_info("Created example script: hello_world.preql")

    print_success(
        f"\nWorkspace initialized successfully!\n\n"
        f"Get started with:\n"
        f"  cd {workspace_path.name if path != '.' else workspace_path}\n"
        f"  trilogy unit hello_world.preql\n"
    )
