"""Index and model generation utilities for the serve command."""

from pathlib import Path

from trilogy.scripts.serve_helpers.file_discovery import (
    extract_description_from_file,
    find_preql_files,
    get_relative_model_name,
    get_safe_model_name,
    read_file_content,
)
from trilogy.scripts.serve_helpers.models import (
    ImportFile,
    ModelImport,
    StoreModelIndex,
)


def generate_model_index(directory_path: Path, base_url: str) -> list[StoreModelIndex]:
    """Generate model index from preql files in a directory.

    Args:
        directory_path: Root directory containing .preql files
        base_url: Base URL for the server (e.g., "http://localhost:8100")

    Returns:
        List of StoreModelIndex entries for all found models
    """
    preql_files = find_preql_files(directory_path)
    models = []

    for preql_file in preql_files:
        model_name = get_relative_model_name(preql_file, directory_path)
        safe_name = get_safe_model_name(model_name)

        models.append(
            StoreModelIndex(name=model_name, url=f"{base_url}/models/{safe_name}.json")
        )

    return models


def find_model_by_name(
    model_name: str, directory_path: Path, base_url: str
) -> ModelImport | None:
    """Find and construct a ModelImport for a specific model by name.

    Args:
        model_name: The safe model name (with slashes replaced by hyphens)
        directory_path: Root directory containing .preql files
        base_url: Base URL for the server

    Returns:
        ModelImport object if found, None otherwise
    """
    preql_files = find_preql_files(directory_path)

    for preql_file in preql_files:
        file_model_name = get_relative_model_name(preql_file, directory_path)
        safe_name = get_safe_model_name(file_model_name)

        if safe_name == model_name:
            description = extract_description_from_file(preql_file)

            return ModelImport(
                name=file_model_name,
                description=description,
                engine="generic",
                components=[
                    ImportFile(
                        url=f"{base_url}/files/{safe_name}.preql",
                        name=file_model_name,
                        alias="",
                        type="trilogy",
                        purpose="source",
                    )
                ],
            )

    return None


def find_file_content_by_name(file_name: str, directory_path: Path) -> str | None:
    """Find and return the content of a preql file by its safe name.

    Args:
        file_name: The safe file name (with slashes replaced by hyphens)
        directory_path: Root directory containing .preql files

    Returns:
        File content as string if found, None otherwise
    """
    preql_files = find_preql_files(directory_path)

    for preql_file in preql_files:
        file_model_name = get_relative_model_name(preql_file, directory_path)
        safe_name = get_safe_model_name(file_model_name)

        if safe_name == file_name:
            return read_file_content(preql_file)

    return None
