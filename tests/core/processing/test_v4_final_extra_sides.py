from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from trilogy import Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import UnbuiltGroupException
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.models import DepthLabel, GroupAttrs
from trilogy.core.processing.v4_helper.strategy_builder import (
    _final_extra_sides,
    _FinalExtras,
    _join_gate_hosts,
    _planned_side,
)

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


def test_root_side_carries_an_argument_it_outputs():
    env = Environment()
    env.parse(MODEL)
    build = env.materialize_for_select()
    graph = nx.DiGraph()
    for gid in ("rows", "basic", FINAL_NODE_ID):
        graph.add_node(gid)
    attrs = {
        "rows": _attrs(Derivation.ROOT, "local.tree_id", "local.raw_species"),
        "basic": _attrs(Derivation.BASIC, "local.genus", "local.tree_id"),
        FINAL_NODE_ID: _attrs(None),
    }

    extras = _final_extra_sides(
        graph,
        attrs,
        ["rows", "basic"],
        {"rows": [build.concepts["local.tree_id"]]},
        [],
        {"local.raw_species"},
        build,
    )

    assert extras.carried == {"local.raw_species": "rows"}
    assert extras.carried_by("rows") == ["local.raw_species"]
    assert extras.arg_producers == {}
    assert extras.groups == []


@dataclass
class _Node:
    output_concepts: list[SimpleNamespace]


def _node(*addresses: str) -> _Node:
    return _Node([SimpleNamespace(address=a) for a in addresses])


def test_gate_key_no_parent_carries_joins_its_planned_host():
    rows, basic = _node("local.tree_id"), _node("local.genus", "local.tree_id")
    extras = _FinalExtras({"local.x_genus": "agg"}, {"local.genus": "basic"})
    keys = frozenset({"local.genus"})

    assert _join_gate_hosts([rows], keys, extras, {"basic": basic}) == [rows, basic]
    assert _join_gate_hosts([basic], keys, extras, {"basic": basic}) == [basic]
    with pytest.raises(UnbuiltGroupException, match="built nothing"):
        _join_gate_hosts([rows], keys, extras, {})


def test_planned_side_that_built_nothing_raises():
    with pytest.raises(UnbuiltGroupException, match="built nothing"):
        _planned_side({}, "basic")
