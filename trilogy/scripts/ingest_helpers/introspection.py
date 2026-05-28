"""Shared vocabulary for ingest-time introspection passes.

Currently consumed by FK inference; intended to govern any future
discovery pass (rich types, key detection, etc.) that has the same
off / cheap / thorough escalation.
"""

from enum import Enum


class IntrospectionLevel(Enum):
    OFF = "off"
    FAST = "fast"
    FULL = "full"
