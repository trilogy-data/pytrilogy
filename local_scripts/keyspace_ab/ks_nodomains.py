"""pytest plugin: plan as before keyspace phase 4 (no region domain buckets, a
BASIC keyed on its declared keys), so rows can be captured on both sides of it
without a production switch. Inert unless KS_NODOMAINS is set."""

import os

from trilogy.core.processing.v4_helper import group_graph, keyspace


def _no_domains(*args, **kwargs) -> None:
    return None


def _declared_keys(*args, **kwargs) -> frozenset[str]:
    return frozenset()


if os.environ.get("KS_NODOMAINS"):
    group_graph._add_region_domain_buckets = _no_domains
    keyspace._read_addresses = _declared_keys
