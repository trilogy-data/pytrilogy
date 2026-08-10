"""Shared vocabulary for ingest-time introspection passes.

Currently consumed by FK inference; intended to govern any future
discovery pass (rich types, key detection, etc.) that has the same
off / cheap / thorough escalation.
"""

from enum import Enum

from trilogy.core.enums import AddressType


class IntrospectionLevel(Enum):
    OFF = "off"
    FAST = "fast"
    FULL = "full"


FILE_ADDRESS_TYPES = frozenset(
    {AddressType.CSV, AddressType.TSV, AddressType.PARQUET}
)


def file_introspection_source(location: str, addr_type: AddressType) -> str:
    """SQL fragment that DuckDB can read this file with."""
    quoted = location.replace("'", "''")
    if addr_type == AddressType.CSV:
        return f"read_csv_auto('{quoted}')"
    if addr_type == AddressType.TSV:
        return f"read_csv_auto('{quoted}', delim='\\t')"
    if addr_type == AddressType.PARQUET:
        return f"read_parquet('{quoted}')"
    raise ValueError(f"Unsupported file address type: {addr_type}")
