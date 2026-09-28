"""Heal audit: does the plan keyspace, built over the datasources pin-heal
REWROTE, say what the keyspace over the bindings AS AUTHORED says?

Pin-heal proves per binding that the rows a `~` source lacks are dead under the
plan's WHERE, then strips the `~`. The rewrite tells `_model_facts` something
stronger: the source is a complete table, so keyed lookups from other sources
may enter it. The two coincide only if the anchor guard is exactly that claim.
This audit (`TRILOGY_KEYSPACE_HEAL_AUDIT=<file>`) builds the keyspace both ways
at every plan and appends one JSON line per plan whose reader-visible facts
differ (`KeyspaceFacts`). `in_play` differs by design (the authored side keeps
the emptied regions' spans) and is reported apart.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, fields

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace

from .keyspace import RowsetWitness, build_keyspace
from .models import ConceptAttrs

AUDIT_ENV = "TRILOGY_KEYSPACE_HEAL_AUDIT"


@dataclass(frozen=True)
class KeyspaceFacts:
    """What a keyspace's readers see, in a comparable, JSON-ready form."""

    # (present, spans, live completes) per live region
    live_regions: list[tuple[list[str], list[str], list[str]]]
    demanded_spans: list[str]
    output_demanded_spans: list[str]
    keys_by_address: dict[str, list[str]]
    span_reach: dict[str, list[str]]
    # (output, present) for every live region the output is carried on
    carried_on: list[tuple[str, list[str]]]


def keyspace_facts(keyspace: Keyspace, in_play: frozenset[str]) -> KeyspaceFacts:
    """`in_play` restricts the reach to the spans both sides have in play: the
    authored side keeps the emptied regions' spans there by design."""
    live = keyspace.live_regions
    return KeyspaceFacts(
        live_regions=sorted(
            (sorted(r.present), sorted(r.spans), sorted(r.live_completes)) for r in live
        ),
        demanded_spans=sorted(keyspace.demanded_spans),
        output_demanded_spans=sorted(keyspace.output_demanded_spans),
        keys_by_address={
            a: sorted(k) for a, k in sorted(keyspace.keys_by_address.items())
        },
        span_reach={
            s: sorted(r) for s, r in sorted(keyspace.span_reach.items()) if s in in_play
        },
        carried_on=sorted(
            (o, sorted(r.present))
            for o in keyspace.outputs
            for r in live
            if keyspace.carried_on(o, r)
        ),
    )


def audit_heal_keyspace(
    planned: Keyspace,
    concept_attrs: dict[str, ConceptAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    witnesses: tuple[RowsetWitness, ...] = (),
) -> None:
    path = os.environ.get(AUDIT_ENV)
    if not path or environment.authored_datasources is None:
        return
    authored = build_keyspace(
        concept_attrs,
        mandatory_list,
        environment,
        conditions,
        datasources=environment.authored_datasources,
        rowset_witnesses=witnesses,
    )
    common = authored.in_play_spans & planned.in_play_spans
    left, right = asdict(keyspace_facts(authored, common)), asdict(
        keyspace_facts(planned, common)
    )
    diff = {
        f.name: {"authored": left[f.name], "rewritten": right[f.name]}
        for f in fields(KeyspaceFacts)
        if left[f.name] != right[f.name]
    }
    if not diff:
        return
    record = {
        "test": os.environ.get("PYTEST_CURRENT_TEST", ""),
        "outputs": [c.address for c in mandatory_list],
        "conditions": [str(c.conditional) for c in conditions],
        "in_play": {
            "authored": sorted(authored.in_play_spans),
            "rewritten": sorted(planned.in_play_spans),
        },
        "diff": diff,
    }
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record) + "\n")
