"""A concept-level `union(...)` plans as a stacked source: one arm scope per
row population, one parent per arm, and a WHERE that lands where its column
lives (in the arm, or in every arm for a stacked column)."""

from dataclasses import dataclass, field

import pytest

import trilogy.core.processing.concept_strategies_v4 as cs
import trilogy.core.processing.v4_helper.strategy_builder as sb
from tests.engine.test_duckdb_union_arm_cast import MODEL
from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.processing.nodes import UnionNode
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.models import GroupAttrs
from trilogy.core.processing.v4_helper.union_arms import (
    union_arm_identities,
    union_arms,
)

SWAPPED_MODEL = MODEL + "\nauto swapped <- union(pad, amt);"
CONSTANT_MODEL = (
    MODEL
    + "\nauto zero <- cast(0.0 as numeric(15,2));\nauto all_v <- union(amt, zero);"
)
UNRELATED_MODEL = MODEL + """
key j1 int;
key j2 int;
auto all_j <- union(j1, j2);
datasource j (a: j1) grain (j1) query '''select 9 as a''';
datasource jj (a: j2) grain (j2) query '''select 8 as a''';
"""


def _materialize(text: str):
    env = Environment()
    env.parse(text)
    return env.materialize_for_select()


@dataclass
class Capture:
    graphs: list = field(default_factory=list)
    attrs: list[dict[str, GroupAttrs]] = field(default_factory=list)
    builds: list[tuple[Derivation, int]] = field(default_factory=list)
    union_nodes: list[UnionNode] = field(default_factory=list)


@pytest.fixture
def capture(monkeypatch) -> Capture:
    captured = Capture()
    graph_builder = cs.build_group_graph
    node_builder = sb.build_node

    def spy_graph(*args, **kwargs):
        result = graph_builder(*args, **kwargs)
        captured.graphs.append(result[0])
        captured.attrs.append(result[2])
        return result

    def spy_node(**kwargs):
        captured.builds.append((kwargs["derivation"], len(kwargs["parents"])))
        node = node_builder(**kwargs)
        if isinstance(node, UnionNode):
            captured.union_nodes.append(node)
        return node

    monkeypatch.setattr(cs, "build_group_graph", spy_graph)
    monkeypatch.setattr(sb, "build_node", spy_node)
    return captured


def _plan(model: str, query: str) -> None:
    Dialects.DUCK_DB.default_executor().generate_sql(model + "\n" + query)


def test_property_arms_take_their_keys_identity():
    benv = _materialize(MODEL)
    for name in ("all_k", "all_amt"):
        assert union_arm_identities(benv.concepts[name], benv) == [
            "local.k1",
            "local.k2",
        ]


def test_arms_align_by_key_not_position():
    benv = _materialize(SWAPPED_MODEL)
    arms = union_arms([benv.concepts["all_k"], benv.concepts["swapped"]], benv)
    assert arms is not None
    assert sorted(tuple(c.address for c in arm) for arm in arms) == [
        ("local.k1", "local.amt"),
        ("local.k2", "local.pad"),
    ]


def test_keyless_arm_takes_the_unclaimed_family_identity():
    benv = _materialize(CONSTANT_MODEL)
    assert union_arm_identities(benv.concepts["all_v"], benv) == [
        "local.k1",
        "local.k2",
    ]


def test_unrelated_union_keeps_its_own_family():
    benv = _materialize(UNRELATED_MODEL)
    assert union_arm_identities(benv.concepts["all_j"], benv) == [
        "local.j1",
        "local.j2",
    ]
    assert union_arms([benv.concepts["all_k"], benv.concepts["all_j"]], benv) is None


def test_union_group_is_fed_one_root_per_arm(capture: Capture):
    _plan(MODEL, "select all_k, all_amt;")
    graph, attrs = capture.graphs[0], capture.attrs[0]
    union_gid = next(
        gid for gid, a in attrs.items() if a.derivation == Derivation.UNION
    )
    arm_labels = sorted(
        attrs[pred].label
        for pred in graph.predecessors(union_gid)
        if pred != FINAL_NODE_ID
    )
    assert arm_labels == ["arm:local.k1", "arm:local.k2"]
    assert sorted(capture.builds, key=lambda b: (b[0].value, b[1])) == [
        (Derivation.ROOT, 0),
        (Derivation.ROOT, 0),
        (Derivation.UNION, 2),
    ]
    assert len(capture.union_nodes[0].parents) == 2


def test_arm_private_filter_hosts_on_its_arm(capture: Capture):
    _plan(MODEL, "select all_k, all_amt where amt > 0.15;")
    hosts = {a.label for a in capture.attrs[0].values() if a.condition_atoms}
    assert hosts == {"arm:local.k1"}


def test_stacked_column_filter_hosts_on_the_union(capture: Capture):
    _plan(MODEL, "select all_k where all_amt > 0.15;")
    hosts = {
        (a.derivation, a.label) for a in capture.attrs[0].values() if a.condition_atoms
    }
    assert hosts == {(Derivation.UNION, "")}
    assert all(arm.conditions is not None for arm in capture.union_nodes[0].parents)
