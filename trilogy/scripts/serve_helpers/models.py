"""Pydantic models for the serve command."""

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
