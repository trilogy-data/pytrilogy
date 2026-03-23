"""Pydantic models for the serve command."""

from typing import Literal

from pydantic import BaseModel, Field


class ImportFile(BaseModel):
    """Component file in a model import."""

    url: str
    name: str
    alias: str = ""
    purpose: str
    type: str | None = None


class ModelImport(BaseModel):
    """Model import definition."""

    name: str
    engine: str
    description: str
    link: str = ""
    tags: list[str] = Field(default_factory=list)
    components: list[ImportFile]


class StoreModelIndex(BaseModel):
    """Individual model entry in the store index."""

    name: str
    url: str


class StoreIndex(BaseModel):
    """Store index containing list of available models."""

    name: str
    models: list[StoreModelIndex]
    project_name: str | None = None


class FileWriteRequest(BaseModel):
    """Request body for creating or updating a file."""

    content: str


class FileCreateRequest(BaseModel):
    """Request body for creating a new file."""

    path: str
    content: str


class DirectoryListing(BaseModel):
    """Files grouped under a single directory."""

    directory: str
    files: list[str]


class FileListResponse(BaseModel):
    """All trilogy/sql/csv files organized by directory."""

    directories: list[DirectoryListing]


class JobRequest(BaseModel):
    """Request to run or refresh a target path."""

    target: str


JobStatusLiteral = Literal["running", "success", "error", "cancelled"]


class JobStatus(BaseModel):
    """Status of a background job."""

    job_id: str
    status: JobStatusLiteral
    output: str
    error: str
    return_code: int | None = None


AssetStatusLiteral = Literal["fresh", "stale", "unknown"]


class WatermarkInfo(BaseModel):
    """Serialized watermark for a single key on a datasource."""

    type: str  # "incremental_key", "update_time", "key_hash"
    value: str | None  # always stringified for JSON safety


class AssetState(BaseModel):
    """State of a single datasource asset."""

    id: str
    is_root: bool
    status: AssetStatusLiteral
    stale_reason: str | None = None
    watermarks: dict[str, WatermarkInfo]


class StateSummary(BaseModel):
    total: int
    root: int
    stale: int
    fresh: int
    unknown: int


class StateResponse(BaseModel):
    """Asset-level state for a parsed trilogy file."""

    target: str
    assets: list[AssetState]
    summary: StateSummary
