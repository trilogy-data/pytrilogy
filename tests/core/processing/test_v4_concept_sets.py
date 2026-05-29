"""Integration tests for the per-group concept-set computation pass.

These build synthetic group graphs and call the private orchestrator
directly. The point is to exercise the two-pass capability/demand wiring
end-to-end on shapes mirroring the TPC-DS regressions, without standing
up a BuildEnvironment.
"""

import networkx as nx

from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.group_graph import _compute_concept_sets
from trilogy.core.processing.v4_helper.models import GroupAttrs, GroupBucket


# A minimal stand-in for BuildConcept — _compute_concept_sets only reads `.address`.
class _FakeConcept:
    def __init__(self, address: str):
        self.address = address


def _gg_node(
    graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    derivation: str,
    primary: list[str],
    secondary: list[str] | None = None,
    grain: set[str] | None = None,
    condition_atoms: list | None = None,
):
    graph.add_node(gid)
    attrs[gid] = GroupAttrs(
        depth_label="d*",
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=tuple(primary),
        secondary_members=tuple(secondary or ()),
        condition_atoms=condition_atoms or [],
    )


def _final_node(graph: nx.DiGraph, attrs: dict[str, GroupAttrs]):
    graph.add_node(FINAL_NODE_ID)
    attrs[FINAL_NODE_ID] = GroupAttrs(depth_label="", derivation="final")


def _make_bucket(
    derivation: str, primary: list[str], grain: set[str] | None = None
) -> GroupBucket:
    return GroupBucket(
        depth_label="d*",
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=list(primary),
    )


def _add_concept(
    cg: nx.DiGraph,
    address: str,
    grain: set[str] | None = None,
    derivation: str = Derivation.ROOT.value,
):
    cg.add_node(
        address,
        grain_components=frozenset(grain or ()),
        derivation=derivation,
        depth_label="d*",
    )


def _add_lineage(cg: nx.DiGraph, parent: str, child: str):
    cg.add_edge(parent, child, kind="lineage")


# ----------------------------------------------------------------------
# q02-shape: ROOT -> FILTER -> AGGREGATE -> WINDOW -> BASIC -> FINAL
# Verifies the regression is captured: BASIC must expose week_seq even
# though its declared grain is the source row grain.
# ----------------------------------------------------------------------


def test_q02_shape_basic_exposes_inherited_grain_key():
    cg = nx.DiGraph()
    _add_concept(cg, "week_seq", grain={"date.id"})
    _add_concept(cg, "ext_price", grain={"item.id", "order_id"})
    _add_concept(
        cg, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE.value
    )
    _add_concept(cg, "win_lead", grain={"week_seq"}, derivation=Derivation.WINDOW.value)
    _add_concept(
        cg,
        "round_result",
        grain={"item.id", "order_id"},
        derivation=Derivation.BASIC.value,
    )
    _add_lineage(cg, "ext_price", "agg_sum")
    _add_lineage(cg, "week_seq", "agg_sum")
    _add_lineage(cg, "agg_sum", "win_lead")
    _add_lineage(cg, "week_seq", "win_lead")
    _add_lineage(cg, "agg_sum", "round_result")
    _add_lineage(cg, "win_lead", "round_result")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(
        gg, attrs, "root", Derivation.ROOT.value, primary=["week_seq", "ext_price"]
    )
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE.value,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "win",
        Derivation.WINDOW.value,
        primary=["win_lead"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC.value,
        primary=["round_result"],
        grain={"item.id", "order_id"},  # the misleading source row grain
    )
    _final_node(gg, attrs)
    gg.add_edge("root", "agg", kind="lineage")
    gg.add_edge("root", "win", kind="lineage")
    gg.add_edge("agg", "win", kind="lineage")
    gg.add_edge("agg", "basic", kind="lineage")
    gg.add_edge("win", "basic", kind="lineage")
    for gid in ("root", "agg", "win", "basic"):
        gg.add_edge(gid, FINAL_NODE_ID, kind="merge")

    buckets = {
        "root": _make_bucket(Derivation.ROOT.value, ["week_seq", "ext_price"]),
        "agg": _make_bucket(
            Derivation.AGGREGATE.value, ["agg_sum"], grain={"week_seq"}
        ),
        "win": _make_bucket(Derivation.WINDOW.value, ["win_lead"], grain={"week_seq"}),
        "basic": _make_bucket(
            Derivation.BASIC.value,
            ["round_result"],
            grain={"item.id", "order_id"},
        ),
    }
    mandatory = [_FakeConcept("round_result"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

    basic_out = set(attrs["basic"].output_concepts)
    assert "round_result" in basic_out
    assert (
        "week_seq" in basic_out
    ), "BASIC must expose its inherited-grain key for FINAL to read"


def test_q02_shape_root_does_not_leak_finer_columns_to_aggregate():
    """ROOT's `ext_price` (row grain) must NOT show up in AGGREGATE's
    capability — the grain check at the aggregate boundary blocks it."""
    cg = nx.DiGraph()
    _add_concept(cg, "week_seq", grain={"date.id"})
    _add_concept(cg, "ext_price", grain={"item.id"})
    _add_concept(
        cg, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE.value
    )
    _add_lineage(cg, "ext_price", "agg_sum")
    _add_lineage(cg, "week_seq", "agg_sum")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(
        gg, attrs, "root", Derivation.ROOT.value, primary=["week_seq", "ext_price"]
    )
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE.value,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    gg.add_edge("root", "agg", kind="lineage")
    gg.add_edge("root", FINAL_NODE_ID, kind="merge")
    gg.add_edge("agg", FINAL_NODE_ID, kind="merge")
    buckets = {
        "root": _make_bucket(Derivation.ROOT.value, ["week_seq", "ext_price"]),
        "agg": _make_bucket(
            Derivation.AGGREGATE.value, ["agg_sum"], grain={"week_seq"}
        ),
    }
    mandatory = [_FakeConcept("agg_sum"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

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
    _add_concept(cg, "customer.id", grain={"customer.id"})
    _add_concept(cg, "text_id", grain={"customer.id"})
    _add_concept(cg, "first_name", grain={"customer.id"})
    _add_concept(cg, "ext_price", grain={"item.id"})  # row grain, irrelevant
    _add_concept(cg, "year", grain={"date.id"})
    _add_concept(
        cg, "local_id", grain={"customer.id"}, derivation=Derivation.BASIC.value
    )
    _add_concept(
        cg, "local_name", grain={"customer.id"}, derivation=Derivation.BASIC.value
    )
    _add_lineage(cg, "text_id", "local_id")
    _add_lineage(cg, "first_name", "local_name")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(
        gg,
        attrs,
        "root",
        Derivation.ROOT.value,
        primary=["customer.id", "text_id", "first_name", "ext_price", "year"],
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC.value,
        primary=["local_id", "local_name"],
        grain={"customer.id"},
    )
    _final_node(gg, attrs)
    gg.add_edge("root", "basic", kind="lineage")
    gg.add_edge("root", FINAL_NODE_ID, kind="merge")
    gg.add_edge("basic", FINAL_NODE_ID, kind="merge")
    buckets = {
        "root": _make_bucket(
            Derivation.ROOT.value,
            ["customer.id", "text_id", "first_name", "ext_price", "year"],
        ),
        "basic": _make_bucket(
            Derivation.BASIC.value,
            ["local_id", "local_name"],
            grain={"customer.id"},
        ),
    }
    mandatory = [_FakeConcept("local_id"), _FakeConcept("local_name")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

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
    _add_concept(cg, "week_seq", grain={"date.id"})
    _add_concept(cg, "ext_price", grain={"item.id", "order_id"})
    _add_concept(
        cg, "agg_sum", grain={"week_seq"}, derivation=Derivation.AGGREGATE.value
    )
    _add_lineage(cg, "ext_price", "agg_sum")
    _add_lineage(cg, "week_seq", "agg_sum")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(
        gg, attrs, "root", Derivation.ROOT.value, primary=["week_seq", "ext_price"]
    )
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE.value,
        primary=["agg_sum"],
        secondary=["week_seq"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    gg.add_edge("root", "agg", kind="lineage")
    gg.add_edge("agg", FINAL_NODE_ID, kind="merge")
    buckets = {
        "root": _make_bucket(Derivation.ROOT.value, ["week_seq", "ext_price"]),
        "agg": _make_bucket(
            Derivation.AGGREGATE.value, ["agg_sum"], grain={"week_seq"}
        ),
    }
    mandatory = [_FakeConcept("agg_sum"), _FakeConcept("week_seq")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

    agg_in = set(attrs["agg"].input_concepts)
    # Inputs for the SUM: ext_price (lineage arg) and week_seq (passthrough output).
    assert {"ext_price", "week_seq"} <= agg_in


def test_basic_inputs_drop_primaries_that_are_computed_locally():
    cg = nx.DiGraph()
    _add_concept(cg, "agg_sum", grain={"week_seq"})
    _add_concept(
        cg, "round_result", grain={"week_seq"}, derivation=Derivation.BASIC.value
    )
    _add_lineage(cg, "agg_sum", "round_result")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(
        gg,
        attrs,
        "agg",
        Derivation.AGGREGATE.value,
        primary=["agg_sum"],
        grain={"week_seq"},
    )
    _gg_node(
        gg,
        attrs,
        "basic",
        Derivation.BASIC.value,
        primary=["round_result"],
        grain={"week_seq"},
    )
    _final_node(gg, attrs)
    gg.add_edge("agg", "basic", kind="lineage")
    gg.add_edge("basic", FINAL_NODE_ID, kind="merge")
    buckets = {
        "agg": _make_bucket(
            Derivation.AGGREGATE.value, ["agg_sum"], grain={"week_seq"}
        ),
        "basic": _make_bucket(
            Derivation.BASIC.value, ["round_result"], grain={"week_seq"}
        ),
    }
    mandatory = [_FakeConcept("round_result")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

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
    _add_concept(cg, "x", grain={"x"})
    _add_concept(cg, "y", grain={"x"}, derivation=Derivation.BASIC.value)
    _add_lineage(cg, "x", "y")

    gg = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    _gg_node(gg, attrs, "root", Derivation.ROOT.value, primary=["x"])
    _gg_node(gg, attrs, "basic", Derivation.BASIC.value, primary=["y"], grain={"x"})
    _final_node(gg, attrs)
    gg.add_edge("root", "basic", kind="lineage")
    gg.add_edge("basic", FINAL_NODE_ID, kind="merge")
    buckets = {
        "root": _make_bucket(Derivation.ROOT.value, ["x"]),
        "basic": _make_bucket(Derivation.BASIC.value, ["y"], grain={"x"}),
    }
    mandatory = [_FakeConcept("y")]

    _compute_concept_sets(gg, attrs, cg, buckets, mandatory)

    assert attrs["root"].hidden_concepts == ()
    assert attrs["basic"].hidden_concepts == ()
