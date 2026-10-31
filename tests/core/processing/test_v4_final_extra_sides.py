from trilogy import Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.models import DepthLabel, GroupAttrs
from trilogy.core.processing.v4_helper.strategy_builder import _final_extra_sides

MODEL = """
key tree_id string;
property tree_id.raw_species string;
auto genus <- substring(raw_species, 1, 3);
auto x_genus <- count(tree_id) by genus;
datasource trees (tree_id: tree_id, sp: raw_species)
grain (tree_id)
address trees;
"""


def _attrs(derivation: Derivation | None, *outputs: str) -> GroupAttrs:
    return GroupAttrs(
        depth_label=DepthLabel.D0, derivation=derivation, output_concepts=outputs
    )


def test_gate_key_no_side_renders_is_joined_from_its_row_stream():
    env = Environment()
    env.parse(MODEL)
    build = env.materialize_for_select()
    graph = nx.DiGraph()
    for gid in ("rows", "agg", "basic", FINAL_NODE_ID):
        graph.add_node(gid)
    attrs = {
        "rows": _attrs(Derivation.ROOT, "local.tree_id"),
        "agg": _attrs(Derivation.AGGREGATE, "local.x_genus", "local.genus"),
        "basic": _attrs(Derivation.BASIC, "local.genus", "local.tree_id"),
        FINAL_NODE_ID: _attrs(None),
    }

    extras = _final_extra_sides(
        graph,
        attrs,
        ["agg", "basic"],
        {"rows": [build.concepts["local.tree_id"]]},
        [],
        {"local.x_genus"},
        build,
    )

    assert extras.arg_producers == {"local.x_genus": "agg"}
    assert extras.gate_hosts == {"local.genus": "basic"}
    assert extras.groups == ["agg", "basic"]
