"""Data models used by display helpers."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ResultSet:
    rows: list[tuple]
    columns: list[str]


@dataclass(frozen=True)
class StaleDataSourceEntry:
    datasource_id: str
    referenced_in: list[Path]
    reason: str | None = None


@dataclass
class ManagedDataGroup:
    data_address: str
    datasources: list[StaleDataSourceEntry]
    common_reason: str | None = None
