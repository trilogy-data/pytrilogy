"""Data models used by display helpers."""

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core.scope_diagnostics import DerivedValueScope


@dataclass
class ResultSet:
    rows: list[tuple]
    columns: list[str]
    # SPIKE (duckdb-only): per-column stats over the FULL result (query re-run
    # with its LIMIT removed), so the stats block reflects true cardinality
    # rather than the limited, ORDER-BY-biased prefix. None when not computed.
    full_column_stats: "list[dict] | None" = None
    full_row_count: "int | None" = None
    # Derived-value scope diagnostics off the ProcessedQuery; None for
    # non-query statements. Always emitted in JSON mode (agent default),
    # rendered in rich mode only behind `run --scope`.
    derived_value_scopes: "list[DerivedValueScope] | None" = None


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
