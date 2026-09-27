"""pytest plugin: print each plan's keyspace as it is built (run with `-s`).
Sub-plans (rowset bodies, condition sources) print too, in build order.
Patched where `_build_from_graph` imports it at call time."""

import trilogy.core.processing.v4_node_generators.rowset_witness as rw

_original = rw.statement_keyspace


def _showing(concept_attrs, mandatory_list, environment, conditions, *args, **kwargs):
    keyspace = _original(
        concept_attrs, mandatory_list, environment, conditions, *args, **kwargs
    )
    print(
        "KEYSPACE outputs=",
        [c.address for c in mandatory_list][:6],
        "in_play=",
        sorted(keyspace.in_play_spans),
        "demanded=",
        sorted(keyspace.output_demanded_spans),
        "witnessed=",
        keyspace.witnessed,
        "regions=",
        keyspace.describe(),
    )
    return keyspace


rw.statement_keyspace = _showing
