"""pytest plugin: record every region domain the planner decides, by kind and
note, per test.

    DOMAIN_CENSUS=/tmp/kinds.jsonl PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p domain_kind_plugin tests ...
"""

from __future__ import annotations

import json
import os

import pytest

_current: list[str] = [""]


def pytest_configure(config) -> None:
    out = os.environ.get("DOMAIN_CENSUS")
    if not out:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    path = f"{out}.{worker}.jsonl" if worker else out
    from trilogy.core.processing.v4_helper import region_domains, root_partition

    original = region_domains.decide_region_domains

    def recording(*args, **kwargs):
        domains = original(*args, **kwargs)
        with open(path, "a") as fh:
            fh.writelines(
                json.dumps(
                    {
                        "test": _current[0],
                        "kind": d.kind.value,
                        "note": d.note.split(":")[0],
                        "spans": sorted(d.region.spans),
                    }
                )
                + "\n"
                for d in domains
            )
        return domains

    region_domains.decide_region_domains = recording
    root_partition.decide_region_domains = recording
    _install_election(path)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item) -> None:
    _current[0] = item.nodeid


def _install_election(path: str) -> None:
    from trilogy.core.processing.v4_helper import extent_ownership, group_graph

    original = extent_ownership.elect_extent_owners

    def recording(group_graph_, attrs, keyspace, *args, **kwargs):
        result = original(group_graph_, attrs, keyspace, *args, **kwargs)
        if result.owner_by_span:
            domains = {g for g, a in attrs.items() if a.extent_spans}
            with open(path, "a") as fh:
                fh.write(
                    json.dumps(
                        {
                            "test": _current[0],
                            "election": {
                                span: ("domain" if owner in domains else "ranked")
                                for span, owner in result.owner_by_span.items()
                            },
                            "demanded": sorted(keyspace.output_demanded_spans),
                        }
                    )
                    + "\n"
                )
        return result

    extent_ownership.elect_extent_owners = recording
    group_graph.elect_extent_owners = recording
