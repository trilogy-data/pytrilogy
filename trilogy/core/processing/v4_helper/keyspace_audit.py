"""What the keyspace says the plan still gets wrong (docs/keyspace_phase_plan.md).

Inert unless `TRILOGY_KEYSPACE_AUDIT` names a file; then every plan appends one
JSON line per finding. Nothing here feeds a plan. `owner_pads`: a derivation
evaluated on rows of a region it is absent on. `where`: a region that needed a
domain and got none, because no host delivers the WHERE to the domain's rows.
"""

from __future__ import annotations

import json
import os
from typing import Any

from trilogy.core import graph as nx
from trilogy.core.models.build import BuildConcept

from .constants import FINAL_NODE_ID, ROW_STREAM_DERIVATIONS
from .models import ConceptAttrs, GroupAttrs, Keyspace, Region

AUDIT_PATH = os.environ.get("TRILOGY_KEYSPACE_AUDIT")


def _emit(kind: str, **details: Any) -> None:
    assert AUDIT_PATH
    row = {"kind": kind, "test": os.environ.get("PYTEST_CURRENT_TEST", ""), **details}
    with open(AUDIT_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=sorted) + "\n")


def _audit_owner(
    keyspace: Keyspace,
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_attrs: dict[str, ConceptAttrs],
    outputs: list[str],
) -> None:
    """Derivations the plan evaluates on rows of a region they are ABSENT on:
    they read the span owner's padded row stream."""
    derived = {
        a.address
        for a in concept_attrs.values()
        if a.derivation in ROW_STREAM_DERIVATIONS
    }
    ownership = attrs[FINAL_NODE_ID].extent_ownership
    if ownership is None:
        return
    for span, owner in sorted(ownership.owner_by_span.items()):
        for region in keyspace.live_regions:
            if span not in region.spans:
                continue
            readers = [
                m
                for gid in (owner, *nx.descendants(group_graph, owner))
                if gid != FINAL_NODE_ID
                for m in attrs[gid].members
            ]
            absent = sorted(
                m for m in set(readers) & derived if not keyspace.defined_on(m, region)
            )
            if absent:
                _emit(
                    "owner_pads",
                    outputs=outputs,
                    span=span,
                    owner=owner,
                    absent=absent,
                    region=region.describe(),
                )


def audit_undelivered_where(region: Region, addresses: list[str]) -> None:
    if AUDIT_PATH:
        _emit("where", region=region.describe(), undelivered=addresses)


def audit_plan(
    keyspace: Keyspace,
    concept_attrs: dict[str, ConceptAttrs],
    group_graph: nx.DiGraph,
    group_attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
) -> None:
    if not AUDIT_PATH or FINAL_NODE_ID not in group_attrs:
        return
    outputs = [c.address for c in mandatory_list]
    _audit_owner(keyspace, group_graph, group_attrs, concept_attrs, outputs)
