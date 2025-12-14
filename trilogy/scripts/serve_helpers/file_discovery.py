"""File discovery and processing utilities for the serve command."""

from pathlib import Path


def find_preql_files(directory_path: Path) -> list[Path]:
    """Find all .preql files in the directory recursively.

    Args:
        directory_path: The root directory to search

    Returns:
        List of Path objects for all .preql files found
    """
    return list(directory_path.rglob("*.preql"))


def find_sql_files(directory_path: Path) -> list[Path]:
    """Find all .sql files in the directory recursively.

    Args:
        directory_path: The root directory to search

    Returns:
        List of Path objects for all .sql files found
    """
    return list(directory_path.rglob("*.sql"))


def find_csv_files(directory_path: Path) -> list[Path]:
    """Find all .csv files in the directory recursively.

    Args:
        directory_path: The root directory to search

    Returns:
        List of Path objects for all .csv files found
    """
    return list(directory_path.rglob("*.csv"))


def find_trilogy_files(directory_path: Path) -> list[Path]:
    """Find all .preql and .sql files in the directory recursively.

    Args:
        directory_path: The root directory to search

    Returns:
        List of Path objects for all .preql and .sql files found, sorted by path
    """
    preql_files = find_preql_files(directory_path)
    sql_files = find_sql_files(directory_path)
    return sorted(preql_files + sql_files)


def find_all_model_files(directory_path: Path) -> list[Path]:
    """Find all model files (.preql, .sql, .csv) in the directory recursively.

    Args:
        directory_path: The root directory to search

    Returns:
        List of Path objects for all model files found, sorted by path
    """
    preql_files = find_preql_files(directory_path)
    sql_files = find_sql_files(directory_path)
    csv_files = find_csv_files(directory_path)
    return sorted(preql_files + sql_files + csv_files)


def get_relative_model_name(preql_file: Path, directory_path: Path) -> str:
    """Get the relative model name from a model file path.

    Args:
        preql_file: Path to the .preql, .sql, or .csv file
        directory_path: Root directory path

    Returns:
        Relative model name with forward slashes and no extension
    """
    relative_path = preql_file.relative_to(directory_path)
    return (
        str(relative_path)
        .replace("\\", "/")
        .replace(".preql", "")
        .replace(".sql", "")
        .replace(".csv", "")
    )


def get_safe_model_name(model_name: str) -> str:
    """Convert a model name to a URL-safe format.

    Args:
        model_name: The model name (may contain slashes)

    Returns:
        URL-safe model name with slashes replaced by hyphens
    """
    return model_name.replace("/", "-")


def extract_description_from_file(file_path: Path) -> str:
    """Extract description from a preql or sql file's comments.

    Looks for the first comment line (starting with # or --) in the first 5 lines
    of the file and uses it as the description.

    Args:
        file_path: Path to the .preql or .sql file

    Returns:
        Description extracted from comments or a default description
    """
    with open(file_path, "r") as f:
        content = f.read()

    model_name = file_path.stem
    default_description = f"Trilogy model: {model_name}"

    first_lines = content.split("\n")[:5]
    for line in first_lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
        if stripped.startswith("--"):
            return stripped.lstrip("-").strip()

    return default_description


def read_file_content(file_path: Path) -> str:
    """Read and return the content of a file.

    Args:
        file_path: Path to the file

    Returns:
        File content as string
    """
    with open(file_path, "r") as f:
        return f.read()
