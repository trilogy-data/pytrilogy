"""Integration tests for the per-group concept-set computation pass.

These build synthetic group graphs and call the private orchestrator
directly. The point is to exercise the two-pass capability/demand wiring
end-to-end on shapes mirroring the TPC-DS regressions, without standing
up a BuildEnvironment.

Concept-node state lives in a side dict (``dict[str, ConceptAttrs]``) keyed
by node id, mirroring production; the concept graph carries only topology +
lineage edges.
"""

import networkx as nx

from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.processing.v4_helper.constants import (
    FINAL_NODE_ID,
    DepthLabel,
    EdgeKind,
)
from trilogy.core.processing.v4_helper.edges import EdgeMap, add_edge
from trilogy.core.processing.v4_helper.group_graph import _compute_concept_sets
from trilogy.core.processing.v4_helper.models import (
    ConceptAttrs,
    GroupAttrs,
    GroupBucket,
)


# A minimal stand-in for BuildConcept — _compute_concept_sets only reads `.address`.
class _FakeConcept:
    def __init__(self, address: str):
        self.address = address


def _gg_node(
    graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    derivation: Derivation,
    primary: list[str],
    secondary: list[str] | None = None,
    grain: set[str] | None = None,
    condition_atoms: list | None = None,
):
    graph.add_node(gid)
    attrs[gid] = GroupAttrs(
        depth_label=DepthLabel.STAR,
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=tuple(primary),
        secondary_members=tuple(secondary or ()),
        condition_atoms=condition_atoms or [],
    )


def _final_node(graph: nx.DiGraph, attrs: dict[str, GroupAttrs]):
    graph.add_node(FINAL_NODE_ID)
    attrs[FINAL_NODE_ID] = GroupAttrs(depth_label=DepthLabel.FINAL)


def _make_bucket(
    derivation: Derivation, primary: list[str], grain: set[str] | None = None
) -> GroupBucket:
    return GroupBucket(
        depth_label=DepthLabel.STAR,
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=list(primary),
    )


def _add_concept(
    cg: nx.DiGraph,
    cattrs: dict[str, ConceptAttrs],
    address: str,
    grain: set[str] | None = None,
    derivation: Derivation = Derivation.ROOT,
):
    cg.add_node(address)
    cattrs[address] = ConceptAttrs(
        address=address,
        label="",
        derivation=derivation,
        purpose=Purpose.KEY,
        granularity=Granularity.MULTI_ROW,
        depth_label=DepthLabel.STAR,
        grain_components=frozenset(grain or ()),
    )


def _add_lineage(cg: nx.DiGraph, cedges: EdgeMap, parent: str, child: str):
    add_edge(cg, cedges, parent, child, EdgeKind.LINEAGE)


# ----------------------------------------------------------------------
# q02-shape: ROOT -> FILTER -> AGGREGATE -> WINDOW -> BASIC -> FINAL
# Verifies the regression is captured: BASIC must expose week_seq even
# though its declared grain is the source row grain.
# ----------------------------------------------------------------------


def test_q02_shape_basic_exposes_inherited_grain_key():
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "week_seq", grain={"date.id"})
    _add_concept(cg, cattrs, "ext_price", grain={"item.id", "order_id"})
    _add_concept(
        cg, cattrs, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE
    )
    _add_concept(
        cg, cattrs, "win_lead", grain={"week_seq"}, derivation=Derivation.WINDOW
    )
    _add_concept(
        cg,
        cattrs,
        "round_result",
        grain={"item.id", "order_id"},
        derivation=Derivation.BASIC,
    )
    _add_lineage(cg, cedges, "ext_price", "agg_sum")
    _add_lineage(cg, cedges, "week_seq", "agg_sum")
    _add_lineage(cg, cedges, "agg_sum", "win_lead")
    _add_lineage(cg, cedges, "week_seq", "win_lead")
    _add_lineage(cg, cedges, "agg_sum", "round_result")
    _add_lineage(cg, cedges, "win_lead", "round_result")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(gg, attrs, "root", Derivation.ROOT, primary=["week_seq", "ext_price"])
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "win",
        Derivation.WINDOW,
        primary=["win_lead"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC,
        primary=["round_result"],
        grain={"item.id", "order_id"},  # the misleading source row grain
    )
    _final_node(gg, attrs)
    add_edge(gg, gedges, "root", "agg", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "root", "win", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "agg", "win", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "agg", "basic", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "win", "basic", EdgeKind.LINEAGE)
    for gid in ("root", "agg", "win", "basic"):
        add_edge(gg, gedges, gid, FINAL_NODE_ID, EdgeKind.MERGE)

    buckets = {
        "root": _make_bucket(Derivation.ROOT, ["week_seq", "ext_price"]),
        "agg": _make_bucket(Derivation.AGGREGATE, ["agg_sum"], grain={"week_seq"}),
        "win": _make_bucket(Derivation.WINDOW, ["win_lead"], grain={"week_seq"}),
        "basic": _make_bucket(
            Derivation.BASIC,
            ["round_result"],
            grain={"item.id", "order_id"},
        ),
    }
    mandatory = [_FakeConcept("round_result"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    basic_out = set(attrs["basic"].output_concepts)
    assert "round_result" in basic_out
    assert (
        "week_seq" in basic_out
    ), "BASIC must expose its inherited-grain key for FINAL to read"


def test_q02_shape_root_does_not_leak_finer_columns_to_aggregate():
    """ROOT's `ext_price` (row grain) must NOT show up in AGGREGATE's
    capability — the grain check at the aggregate boundary blocks it."""
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "week_seq", grain={"date.id"})
    _add_concept(cg, cattrs, "ext_price", grain={"item.id"})
    _add_concept(
        cg, cattrs, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE
    )
    _add_lineage(cg, cedges, "ext_price", "agg_sum")
    _add_lineage(cg, cedges, "week_seq", "agg_sum")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(gg, attrs, "root", Derivation.ROOT, primary=["week_seq", "ext_price"])
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    add_edge(gg, gedges, "root", "agg", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "root", FINAL_NODE_ID, EdgeKind.MERGE)
    add_edge(gg, gedges, "agg", FINAL_NODE_ID, EdgeKind.MERGE)
    buckets = {
        "root": _make_bucket(Derivation.ROOT, ["week_seq", "ext_price"]),
        "agg": _make_bucket(Derivation.AGGREGATE, ["agg_sum"], grain={"week_seq"}),
    }
    mandatory = [_FakeConcept("agg_sum"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    agg_out = set(attrs["agg"].output_concepts)
    assert (
        "ext_price" not in agg_out
    ), "row-grain column must not ride through an aggregate at narrower grain"
    assert {"agg_sum", "week_seq"} <= agg_out


# ----------------------------------------------------------------------
# q04-shape: BASIC at customer grain, condition-input AGGREGATE at d1
# Verifies the BASIC's GROUP BY can't be inflated by unrelated roots.
# ----------------------------------------------------------------------


def test_q04_shape_basic_at_customer_grain_does_not_pull_row_grain():
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "customer.id", grain={"customer.id"})
    _add_concept(cg, cattrs, "text_id", grain={"customer.id"})
    _add_concept(cg, cattrs, "first_name", grain={"customer.id"})
    _add_concept(cg, cattrs, "ext_price", grain={"item.id"})  # row grain, irrelevant
    _add_concept(cg, cattrs, "year", grain={"date.id"})
    _add_concept(
        cg, cattrs, "local_id", grain={"customer.id"}, derivation=Derivation.BASIC
    )
    _add_concept(
        cg,
        cattrs,
        "local_name",
        grain={"customer.id"},
        derivation=Derivation.BASIC,
    )
    _add_lineage(cg, cedges, "text_id", "local_id")
    _add_lineage(cg, cedges, "first_name", "local_name")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(
        gg,
        attrs,
        "root",
        Derivation.ROOT,
        primary=["customer.id", "text_id", "first_name", "ext_price", "year"],
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC,
        primary=["local_id", "local_name"],
        grain={"customer.id"},
    )
    _final_node(gg, attrs)
    add_edge(gg, gedges, "root", "basic", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "root", FINAL_NODE_ID, EdgeKind.MERGE)
    add_edge(gg, gedges, "basic", FINAL_NODE_ID, EdgeKind.MERGE)
    buckets = {
        "root": _make_bucket(
            Derivation.ROOT,
            ["customer.id", "text_id", "first_name", "ext_price", "year"],
        ),
        "basic": _make_bucket(
            Derivation.BASIC,
            ["local_id", "local_name"],
            grain={"customer.id"},
        ),
    }
    mandatory = [_FakeConcept("local_id"), _FakeConcept("local_name")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    basic_out = set(attrs["basic"].output_concepts)
    assert "local_id" in basic_out and "local_name" in basic_out
    assert (
        "ext_price" not in basic_out
    ), "row-grain leak would inflate the BASIC GROUP BY"
    assert "year" not in basic_out


# ----------------------------------------------------------------------
# input_concepts: lineage of primaries + passthrough outputs
# ----------------------------------------------------------------------


def test_aggregate_inputs_include_primary_lineage_args():
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "week_seq", grain={"date.id"})
    _add_concept(cg, cattrs, "ext_price", grain={"item.id", "order_id"})
    _add_concept(
        cg, cattrs, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE
    )
    _add_lineage(cg, cedges, "ext_price", "agg_sum")
    _add_lineage(cg, cedges, "week_seq", "agg_sum")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(gg, attrs, "root", Derivation.ROOT, primary=["week_seq", "ext_price"])
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    add_edge(gg, gedges, "root", "agg", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "agg", FINAL_NODE_ID, EdgeKind.MERGE)
    buckets = {
        "root": _make_bucket(Derivation.ROOT, ["week_seq", "ext_price"]),
        "agg": _make_bucket(Derivation.AGGREGATE, ["agg_sum"], grain={"week_seq"}),
    }
    mandatory = [_FakeConcept("agg_sum"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    agg_in = set(attrs["agg"].input_concepts)
    # Inputs for the SUM: ext_price (lineage arg) and week_seq (passthrough output).
    assert {"ext_price", "week_seq"} <= agg_in


def test_basic_inputs_drop_primaries_that_are_computed_locally():
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "agg_sum", grain={"week_seq"})
    _add_concept(
        cg,
        cattrs,
        "round_result",
        grain={"week_seq"},
        derivation=Derivation.BASIC,
    )
    _add_lineage(cg, cedges, "agg_sum", "round_result")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE,
        primary=["agg_sum"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC,
        primary=["round_result"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    add_edge(gg, gedges, "agg", "basic", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "basic", FINAL_NODE_ID, EdgeKind.MERGE)
    buckets = {
        "agg": _make_bucket(Derivation.AGGREGATE, ["agg_sum"], grain={"week_seq"}),
        "basic": _make_bucket(Derivation.BASIC, ["round_result"], grain={"week_seq"}),
    }
    mandatory = [_FakeConcept("round_result")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    basic_in = set(attrs["basic"].input_concepts)
    # The lineage arg agg_sum must come from upstream.
    assert "agg_sum" in basic_in
    # The primary (round_result) is computed locally; it isn't sourced.
    assert "round_result" not in basic_in


# ----------------------------------------------------------------------
# hidden_concepts stays empty at intermediate groups
# ----------------------------------------------------------------------


def test_intermediate_groups_have_empty_hidden_concepts():
    cg = nx.DiGraph()
    cattrs: dict[str, ConceptAttrs] = {}
    cedges: EdgeMap = {}
    _add_concept(cg, cattrs, "x", grain={"x"})
    _add_concept(cg, cattrs, "y", grain={"x"}, derivation=Derivation.BASIC)
    _add_lineage(cg, cedges, "x", "y")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    gedges: EdgeMap = {}
    _gg_node(gg, attrs, "root", Derivation.ROOT, primary=["x"])
    _gg_node(gg, attrs, "basic", Derivation.BASIC, primary=["y"], grain={"x"})
    _final_node(gg, attrs)
    add_edge(gg, gedges, "root", "basic", EdgeKind.LINEAGE)
    add_edge(gg, gedges, "basic", FINAL_NODE_ID, EdgeKind.MERGE)
    buckets = {
        "root": _make_bucket(Derivation.ROOT, ["x"]),
        "basic": _make_bucket(Derivation.BASIC, ["y"], grain={"x"}),
    }
    mandatory = [_FakeConcept("y")]

    _compute_concept_sets(gg, gedges, attrs, cg, cedges, cattrs, buckets, mandatory)

    assert attrs["root"].hidden_concepts == ()
    assert attrs["basic"].hidden_concepts == ()
