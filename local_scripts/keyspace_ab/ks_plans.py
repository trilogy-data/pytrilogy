"""pytest plugin: print each plan's keyspace as it is built (run with `-s`).
Sub-plans (rowset bodies, condition sources) print too, in build order."""

import trilogy.core.processing.concept_strategies_v4 as v4

_original = v4.build_keyspace


def _showing(concept_attrs, mandatory_list, environment, conditions, **kwargs):
    keyspace = _original(
        concept_attrs, mandatory_list, environment, conditions, **kwargs
    )
    print(
        "KEYSPACE outputs=",
        [c.address for c in mandatory_list][:6],
        "in_play=",
        sorted(keyspace.in_play_spans),
        "regions=",
        keyspace.describe(),
    )
    return keyspace


v4.build_keyspace = _showing
