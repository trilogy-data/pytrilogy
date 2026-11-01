from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from trilogy import Environment
from trilogy.core import graph as nx
from trilogy.core.enums import AggregateGroupingMode, ComparisonOperator, Derivation
from trilogy.core.exceptions import UnbuiltGroupException
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.models import DepthLabel, GroupAttrs
from trilogy.core.processing.v4_helper.strategy_builder import (
    _bridge_candidates,
    _final_extra_sides,
    _FinalCover,
    _FinalExtras,
    _group_applies_conditions,
    _join_gate_hosts,
    _planned_side,
    _row_condition_before_aggregate,
    _uncovered_final_side,
)

MODEL = """
key tree_id string;
property tree_id.raw_species string;
auto genus <- substring(raw_species, 1, 3);
auto x_genus <- count(tree_id) by genus;
auto is_oak <- case when raw_species = 'oak' then 1 else 0 end;
auto oaks <- sum(is_oak);
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


def test_uncovered_final_is_the_first_planned_group():
    first, second = _node("local.a"), _node("local.b")
    assert _uncovered_final_side({"b": second, "a": first}, ["a", "b"]) is first
    with pytest.raises(UnbuiltGroupException, match="built nothing"):
        _uncovered_final_side({"b": second}, ["a", "b"])


def test_bridge_candidates_are_planned_groups_decided_before_the_consumer():
    graph = nx.DiGraph()
    graph.add_edges_from([("root", "basic"), ("basic", "agg"), ("agg", FINAL_NODE_ID)])
    planned = ["root", "basic", "agg"]
    assert _bridge_candidates(graph, planned, "agg") == ["basic", "root"]
    assert _bridge_candidates(graph, planned) == ["agg", "basic", "root"]
    assert _bridge_candidates(graph, ["root", "agg"], "agg") == ["root"]


def test_cover_holds_what_a_contributor_renders_and_is_owed():
    env = Environment().parse(MODEL)[0].materialize_for_select()
    genus = env.concepts["local.genus"]
    cover = _FinalCover(
        ["basic"],
        {},
        {"basic": [genus]},
        {},
        {"basic": frozenset({"local.tree_id"})},
    )
    assert cover.holds("basic") == {"local.tree_id", "local.genus"}
    assert cover.holds("other") == set()


def _placed_atom_graph(
    host: str, mode: AggregateGroupingMode
) -> tuple[nx.DiGraph, dict[str, GroupAttrs], BuildWhereClause]:
    env = Environment().parse(MODEL)[0].materialize_for_select()
    atom = BuildComparison(
        left=env.concepts["local.raw_species"],
        right="oak",
        operator=ComparisonOperator.EQ,
    )
    graph = nx.DiGraph()
    graph.add_edges_from(
        [
            ("root", "agg"),
            ("agg", "basic"),
            ("basic", FINAL_NODE_ID),
            ("side", FINAL_NODE_ID),
        ]
    )
    attrs = {
        "root": _attrs(Derivation.ROOT, "local.tree_id", "local.raw_species"),
        "agg": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.AGGREGATE,
            primary_members=("local.x_genus",),
            grain_components=frozenset({"local.genus"}),
            grouping_mode=mode,
        ),
        "basic": _attrs(Derivation.BASIC, "local.genus"),
        "side": _attrs(Derivation.ROOT, "local.tree_id", "local.raw_species"),
        FINAL_NODE_ID: _attrs(None),
    }
    attrs[host].condition_atoms.append(atom)
    return graph, attrs, BuildWhereClause(conditional=atom)


@pytest.mark.parametrize(
    "host,mode,applied",
    [
        ("basic", AggregateGroupingMode.STANDARD, True),
        ("root", AggregateGroupingMode.ROLLUP, True),
        ("agg", AggregateGroupingMode.ROLLUP, True),
        ("root", AggregateGroupingMode.STANDARD, True),
        ("side", AggregateGroupingMode.STANDARD, False),
        ("agg", AggregateGroupingMode.STANDARD, False),
    ],
)
def test_sole_contributor_reads_where_condition_placement_put_the_atom(
    host: str, mode: AggregateGroupingMode, applied: bool
):
    graph, attrs, where = _placed_atom_graph(host, mode)
    assert _group_applies_conditions(graph, attrs, "basic", where) is applied


@pytest.mark.parametrize(
    "derivation,member,column,applied",
    [
        (Derivation.AGGREGATE, "local.x_genus", "local.raw_species", True),
        (Derivation.AGGREGATE, "local.x_genus", "local.oaks", False),
        (Derivation.AGGREGATE, "local.oaks", "local.raw_species", False),
        (Derivation.AGGREGATE, "local.oaks", "local.tree_id", True),
        (Derivation.WINDOW, "local.x_genus", "local.raw_species", False),
    ],
)
def test_row_condition_applies_before_an_aggregate_whose_input_rows_hold_it(
    derivation: Derivation, member: str, column: str, applied: bool
):
    env = Environment().parse(MODEL)[0].materialize_for_select()
    graph = nx.DiGraph()
    graph.add_edges_from([("root", "agg"), ("agg", FINAL_NODE_ID)])
    attrs = {
        "root": _attrs(Derivation.ROOT, "local.tree_id", "local.raw_species"),
        "agg": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=derivation,
            primary_members=(member,),
            output_concepts=(member, "local.genus"),
        ),
        FINAL_NODE_ID: _attrs(None),
    }
    where = BuildWhereClause(
        conditional=BuildComparison(
            left=env.concepts[column], right="oak", operator=ComparisonOperator.EQ
        )
    )
    assert _row_condition_before_aggregate(graph, attrs, "agg", where, env) is applied
