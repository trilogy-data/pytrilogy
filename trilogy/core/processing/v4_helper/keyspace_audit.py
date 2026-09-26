"""Heal audit: does the plan keyspace, built over the datasources pin-heal
REWROTE, say what the keyspace over the bindings AS AUTHORED says?

Pin-heal proves per binding that the rows a `~` source lacks are dead under the
plan's WHERE, then strips the `~`. The rewrite tells `_model_facts` something
stronger: the source is a complete table, so keyed lookups from other sources
may enter it. The two coincide only if the anchor guard is exactly that claim.
This audit (`TRILOGY_KEYSPACE_HEAL_AUDIT=<file>`) builds the keyspace both ways
at every plan and appends one JSON line per plan whose reader-visible facts
differ: live regions, demanded spans, entity keys, span reach, what an output is
carried on. `in_play` differs by design (the authored side keeps the emptied
regions' spans) and is reported apart.
"""

from __future__ import annotations

import json
import os
from typing import Any

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment

from .keyspace import RowsetWitness, build_keyspace
from .models import ConceptAttrs, Keyspace

AUDIT_ENV = "TRILOGY_KEYSPACE_HEAL_AUDIT"


def _sorted(values) -> list[str]:
    return sorted(values)


def _facts(keyspace: Keyspace, in_play: frozenset[str]) -> dict[str, Any]:
    live = keyspace.live_regions
    return {
        "live_regions": sorted(
            (_sorted(r.present), _sorted(r.spans), _sorted(r.live_completes))
            for r in live
        ),
        "demanded_spans": _sorted(keyspace.demanded_spans),
        "output_demanded_spans": _sorted(keyspace.output_demanded_spans),
        "keys_by_address": {
            a: _sorted(k) for a, k in sorted(keyspace.keys_by_address.items())
        },
        # only over the spans both sides have in play: the authored side keeps
        # the emptied regions' spans there by design
        "span_reach": {
            s: _sorted(r)
            for s, r in sorted(keyspace.span_reach.items())
            if s in in_play
        },
        "carried_on": sorted(
            (o, _sorted(r.present))
            for o in keyspace.outputs
            for r in live
            if keyspace.carried_on(o, r)
        ),
    }


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
    left, right = _facts(authored, common), _facts(planned, common)
    diff = {
        k: {"authored": left[k], "rewritten": right[k]}
        for k in left
        if left[k] != right[k]
    }
    if not diff:
        return
    record = {
        "test": os.environ.get("PYTEST_CURRENT_TEST", ""),
        "outputs": [c.address for c in mandatory_list],
        "conditions": [str(c.conditional) for c in conditions],
        "in_play": {
            "authored": _sorted(authored.in_play_spans),
            "rewritten": _sorted(planned.in_play_spans),
        },
        "diff": diff,
    }
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record) + "\n")
