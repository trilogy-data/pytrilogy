"""Phase 2 of docs/keyspace_phase_plan.md: does the keyspace agree with the
sites that re-derive its answers today?

Inert unless `TRILOGY_KEYSPACE_AUDIT` names a file; then every plan appends one
JSON line per disagreement. Nothing here feeds a plan. Delete with the sites it
audits.
"""

from __future__ import annotations

import json
import os
from typing import Any
from weakref import ReferenceType, ref

from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.join_resolution import licensed_extension_spans

from .constants import FINAL_NODE_ID
from .extent_ownership import demanded_extension_spans
from .keyspace import build_keyspace
from .models import ConceptAttrs, GroupAttrs, Keyspace

AUDIT_PATH = os.environ.get("TRILOGY_KEYSPACE_AUDIT")

# id(environment) -> (live handle, datasources before pin-heal, what it healed)
_PRE_HEAL: dict[
    int, tuple[ReferenceType[BuildEnvironment], list[BuildDatasource], frozenset[str]]
] = {}


def record_heal(
    environment: BuildEnvironment,
    before: list[BuildDatasource],
    healed: frozenset[str],
) -> None:
    if AUDIT_PATH:
        _PRE_HEAL[id(environment)] = (ref(environment), before, healed)


def _emit(kind: str, **details: Any) -> None:
    assert AUDIT_PATH
    row = {"kind": kind, "test": os.environ.get("PYTEST_CURRENT_TEST", ""), **details}
    with open(AUDIT_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=sorted) + "\n")


def _audit_demand(
    keyspace: Keyspace,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    outputs: list[str],
) -> None:
    elected = demanded_extension_spans(
        attrs, licensed_extension_spans(environment), environment
    )
    if elected != keyspace.output_demanded_spans:
        _emit(
            "demand",
            outputs=outputs,
            election=elected,
            keyspace=keyspace.output_demanded_spans,
            regions=keyspace.describe(),
        )


def _audit_owner(
    keyspace: Keyspace, attrs: dict[str, GroupAttrs], outputs: list[str]
) -> None:
    """Members of a span's owner group that are ABSENT on the span's region:
    what the owner evaluates over padded rows today (phase 4's worklist)."""
    ownership = attrs[FINAL_NODE_ID].extent_ownership
    if ownership is None:
        return
    for span, owner in sorted(ownership.owner_by_span.items()):
        for region in keyspace.live_regions:
            if span not in region.spans:
                continue
            absent = [
                m for m in attrs[owner].members if not keyspace.defined_on(m, region)
            ]
            if absent:
                _emit(
                    "owner_pads",
                    outputs=outputs,
                    span=span,
                    owner=owner,
                    absent=absent,
                    region=region.describe(),
                )


def _audit_heal(
    concept_attrs: dict[str, ConceptAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> None:
    recorded = _PRE_HEAL.get(id(environment))
    if recorded is None or recorded[0]() is not environment:
        return
    _, before, healed = recorded
    replay = build_keyspace(
        concept_attrs, mandatory_list, environment, conditions, datasources=before
    )
    emptied: frozenset[str] = frozenset().union(
        *(r.spans for r in replay.regions if r.is_empty)
    )
    in_play: frozenset[str] = frozenset().union(*(r.spans for r in replay.regions))
    # pin-heal also heals `~` keys the statement never asks extension rows of
    if healed & in_play != emptied:
        _emit(
            "heal",
            outputs=[c.address for c in mandatory_list],
            pin_heal=healed & in_play,
            keyspace=emptied,
            regions=replay.describe(),
        )


def audit_plan(
    keyspace: Keyspace,
    concept_attrs: dict[str, ConceptAttrs],
    group_attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> None:
    if not AUDIT_PATH or FINAL_NODE_ID not in group_attrs:
        return
    outputs = [c.address for c in mandatory_list]
    _audit_demand(keyspace, group_attrs, environment, outputs)
    _audit_owner(keyspace, group_attrs, outputs)
    _audit_heal(concept_attrs, mandatory_list, environment, conditions)
