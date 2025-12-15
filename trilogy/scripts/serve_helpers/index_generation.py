"""Index and model generation utilities for the serve command."""

from pathlib import Path

from trilogy.execution.config import load_config_file
from trilogy.scripts.common import TRILOGY_CONFIG_NAME
from trilogy.scripts.serve_helpers.file_discovery import (
    extract_description_from_file,
    find_all_model_files,
    find_csv_files,
    find_trilogy_files,
    get_relative_model_name,
    get_safe_model_name,
    read_file_content,
)
from trilogy.scripts.serve_helpers.models import (
    ImportFile,
    ModelImport,
    StoreModelIndex,
)


def _get_model_description(directory_path: Path, trilogy_files: list[Path]) -> str:
    """Get model description from README.md, first file, or default.

    Priority order:
    1. README.md file in the directory
    2. First comment from first trilogy file (alphabetically)
    3. Default description based on directory name

    Args:
        directory_path: Root directory of the model
        trilogy_files: List of trilogy files in the directory

    Returns:
        Description string for the model
    """
    # Check for README.md first
    readme_path = directory_path / "README.md"
    if readme_path.exists():
        try:
            with open(readme_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                # Return first non-empty line or first paragraph
                if content:
                    # Get first line or first paragraph (up to first blank line)
                    lines = content.split("\n")
                    first_line = ""
                    for line in lines:
                        stripped = line.strip()
                        # Skip markdown headers
                        if stripped and not stripped.startswith("#"):
                            first_line = stripped
                            break
                        # If it's a header, use it without the hash marks
                        elif stripped.startswith("#"):
                            first_line = stripped.lstrip("#").strip()
                            break
                    if first_line:
                        return first_line
        except Exception:
            pass

    # Fall back to first file's description
    if trilogy_files:
        return extract_description_from_file(trilogy_files[0])

    # Default description
    return f"Trilogy model: {directory_path.name}"


def generate_model_index(
    directory_path: Path, base_url: str, engine: str
) -> list[StoreModelIndex]:
    """Generate model index representing directory as a single model.

    Args:
        directory_path: Root directory containing trilogy files
        base_url: Base URL for the server (e.g., "http://localhost:8100")
        engine: Engine type (e.g., "duckdb", "generic")

    Returns:
        List with a single StoreModelIndex for the directory model
    """
    model_name = directory_path.name
    safe_name = get_safe_model_name(model_name)

    return [StoreModelIndex(name=model_name, url=f"{base_url}/models/{safe_name}.json")]


def find_model_by_name(
    model_name: str, directory_path: Path, base_url: str, engine: str
) -> ModelImport | None:
    """Find and construct a ModelImport representing the directory as a single model.

    Args:
        model_name: The safe model name (directory name with slashes replaced by hyphens)
        directory_path: Root directory containing trilogy files
        base_url: Base URL for the server
        engine: Engine type (e.g., "duckdb", "generic")

    Returns:
        ModelImport object if the model_name matches the directory, None otherwise
    """
    expected_name = get_safe_model_name(directory_path.name)

    if model_name != expected_name:
        return None

    # Check for trilogy.toml config
    config_path = directory_path / TRILOGY_CONFIG_NAME
    setup_scripts = []
    if config_path.exists():
        try:
            config = load_config_file(config_path)
            setup_scripts = config.startup_sql + config.startup_trilogy
        except Exception:
            pass

    # Find all trilogy files (preql and sql)
    trilogy_files = find_trilogy_files(directory_path)

    # Find CSV files separately
    csv_files = find_csv_files(directory_path)

    # Generate description
    description = _get_model_description(directory_path, trilogy_files)

    # Create components for each file
    components = []

    # Add setup scripts first with purpose="setup"
    for setup_file in setup_scripts:
        setup_path = (
            setup_file if setup_file.is_absolute() else directory_path / setup_file
        )
        if setup_path.exists():
            file_model_name = get_relative_model_name(setup_path, directory_path)
            safe_file_name = get_safe_model_name(file_model_name)
            file_ext = setup_path.suffix

            components.append(
                ImportFile(
                    url=f"{base_url}/files/{safe_file_name}{file_ext}",
                    name=file_model_name,
                    alias="",
                    type="sql" if file_ext == ".sql" else "trilogy",
                    purpose="setup",
                )
            )

    # Add all trilogy files (preql and sql) with purpose="source"
    for trilogy_file in trilogy_files:
        # Skip if already added as setup script
        if any(
            trilogy_file.samefile(s) if s.exists() else False for s in setup_scripts
        ):
            continue

        file_model_name = get_relative_model_name(trilogy_file, directory_path)
        safe_file_name = get_safe_model_name(file_model_name)
        file_ext = trilogy_file.suffix

        components.append(
            ImportFile(
                url=f"{base_url}/files/{safe_file_name}{file_ext}",
                name=file_model_name,
                alias="",
                type="sql" if file_ext == ".sql" else "trilogy",
                purpose="source",
            )
        )

    # Add CSV files with purpose="data"
    for csv_file in csv_files:
        file_model_name = get_relative_model_name(csv_file, directory_path)
        safe_file_name = get_safe_model_name(file_model_name)

        components.append(
            ImportFile(
                url=f"{base_url}/files/{safe_file_name}.csv",
                name=file_model_name,
                alias=file_model_name,
                type="csv",
                purpose="data",
            )
        )

    return ModelImport(
        name=directory_path.name,
        description=description,
        engine=engine,
        components=components,
    )


def find_file_content_by_name(file_name: str, directory_path: Path) -> str | None:

    target_parts = Path(file_name.replace("-", "/")).parts

    for model_file in find_all_model_files(directory_path):
        relative_parts = model_file.relative_to(directory_path).parts
        if relative_parts == target_parts:
            return read_file_content(model_file)

    return None
