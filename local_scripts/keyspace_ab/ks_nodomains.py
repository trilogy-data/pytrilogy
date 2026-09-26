"""pytest plugin: plan as before keyspace phase 4, so rows can be captured on
both sides of it without a production switch. `KS_NODOMAINS` is a comma list of
what to switch off: `domains` (no region domain buckets), `basic_keys` (a BASIC
keyed on its declared keys), or `1` for both. Inert when unset."""

import os

from trilogy.core.processing.v4_helper import group_graph, keyspace

_OFF = set(os.environ.get("KS_NODOMAINS", "").split(",")) - {""}


def _no_domains(*args, **kwargs) -> None:
    return None


def _declared_keys(*args, **kwargs) -> frozenset[str]:
    return frozenset()


if _OFF & {"1", "domains"}:
    group_graph._add_region_domain_buckets = _no_domains
if _OFF & {"1", "basic_keys"}:
    keyspace._read_addresses = _declared_keys
