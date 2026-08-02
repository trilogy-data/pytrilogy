"""Helpers for the serve command."""

from trilogy.scripts.serve_helpers.connection_spec import (
    ALLOWED_CONNECTION_OPTIONS,
    build_connection_spec,
    filter_connection_options,
    normalize_connection_type,
)
from trilogy.scripts.serve_helpers.file_discovery import (
    extract_description_from_file,
    find_all_model_files,
    find_csv_files,
    find_preql_files,
    find_python_files,
    find_sql_files,
    find_trilogy_files,
    get_relative_model_name,
    get_safe_model_name,
)
from trilogy.scripts.serve_helpers.index_generation import (
    find_model_by_name,
    generate_model_index,
)
from trilogy.scripts.serve_helpers.jobs import (
    Job,
    cancel_job,
    create_job,
    get_job,
    run_subprocess,
)
from trilogy.scripts.serve_helpers.models import (
    REMOTE_STORE_CONTRACT_VERSION,
    ConnectionSpec,
    DirectoryListing,
    FileCreateRequest,
    FileListResponse,
    FileWriteRequest,
    ImportFile,
    JobRequest,
    JobStatus,
    ModelImport,
    StoreConnectionType,
    StoreIndex,
    StoreModelIndex,
)
from trilogy.scripts.serve_helpers.state_cache import (
    CachedSnapshot,
    StateSnapshotCache,
    fingerprint_directory,
)
from trilogy.scripts.serve_helpers.state_computation import (
    compute_state_snapshot_sync,
    relative_target,
)

__all__ = [
    "ALLOWED_CONNECTION_OPTIONS",
    "REMOTE_STORE_CONTRACT_VERSION",
    "CachedSnapshot",
    "ConnectionSpec",
    "DirectoryListing",
    "FileCreateRequest",
    "FileListResponse",
    "FileWriteRequest",
    "ImportFile",
    "Job",
    "JobRequest",
    "JobStatus",
    "ModelImport",
    "StateSnapshotCache",
    "StoreConnectionType",
    "StoreIndex",
    "StoreModelIndex",
    "build_connection_spec",
    "cancel_job",
    "compute_state_snapshot_sync",
    "create_job",
    "extract_description_from_file",
    "filter_connection_options",
    "find_all_model_files",
    "find_csv_files",
    "find_model_by_name",
    "find_preql_files",
    "find_python_files",
    "find_sql_files",
    "find_trilogy_files",
    "fingerprint_directory",
    "generate_model_index",
    "get_job",
    "get_relative_model_name",
    "get_safe_model_name",
    "normalize_connection_type",
    "relative_target",
    "run_subprocess",
]
