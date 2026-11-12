"""The rehome and condition injection read the sets the demand pass states as
a value: no group holds its sets until the pass after injection writes them."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_window_input_carry import _MODEL
from tests.test_partition_family_strict_grain_dedup import MODEL, build
from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.constants import DepthLabel
from trilogy.core.processing.v4_helper.models import ConceptSets, GroupBucket

_QUERY = "select vehicle, gap_class where gap_class is not null;"


def _held(attrs) -> set[str]:
    return {
        gid
        for gid, a in attrs.items()
        if gid != gg.FINAL_NODE_ID and (a.output_concepts or a.input_concepts)
    }


class _SetsLog:
    def __init__(self) -> None:
        self.held: dict[str, set[str]] = {}
        self.sets: list[ConceptSets] = []
        self.steps: list[str] = []
        self.reads: dict = {}
        self.written: list[ConceptSets] = []
        self.attrs: dict = {}

    def watch(self, name, fn):
        def watched(group_graph, group_edges, attrs, sets, *rest):
            self.held[name] = _held(attrs)
            self.sets.append(sets)
            self.steps.append(name)
            self.attrs = attrs
            out = fn(group_graph, group_edges, attrs, sets, *rest)
            if name == "reads":
                self.reads = out
            return out

        return watched

    def place(self, *args):
        self.held["injection"] = _held(self.attrs)
        self.sets.append(args[-1])
        self.steps.append("injection")
        return _PLACE(*args)

    def write(self, attrs, sets):
        self.steps.append("write")
        self.written.append(sets)
        _WRITE(attrs, sets)


_FOLDS = gg._elect_folds
_READS = gg._elect_reads
_PLACE = gg.plan_condition_placements
_WRITE = gg._write_concept_sets


@pytest.fixture
def log(monkeypatch) -> _SetsLog:
    log = _SetsLog()
    monkeypatch.setattr(gg, "_elect_folds", log.watch("folds", _FOLDS))
    monkeypatch.setattr(gg, "_elect_reads", log.watch("reads", _READS))
    monkeypatch.setattr(gg, "plan_condition_placements", log.place)
    monkeypatch.setattr(gg, "_write_concept_sets", log.write)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_no_group_holds_its_sets_until_injection_is_planned(log):
    assert log.reads
    assert log.held == {"folds": set(), "reads": set(), "injection": set()}
    assert log.sets[0] is log.sets[1] is log.sets[2]
    assert log.sets[0].inputs


def test_the_sets_are_written_once_after_injection(log):
    assert log.steps == ["folds", "reads", "injection", "write"]
    assert log.written[0] is not log.sets[0]


def _bucket(members: list[str], grain: set[str]) -> GroupBucket:
    bucket = GroupBucket(DepthLabel.D0, Derivation.AGGREGATE, frozenset(grain))
    for member in members:
        bucket.add_member(member, member, DepthLabel.D0)
    return bucket


def test_a_group_carries_what_it_is_stated_to_emit():
    sets = ConceptSets(outputs={"g": frozenset({"k", "total", "hidden"})})
    assert sets.carried_by("g", _bucket(["total"], {"k"})) == {"k", "hidden"}


def test_a_group_minted_since_the_statement_carries_its_keys():
    assert ConceptSets().carried_by("g", _bucket(["total"], {"k"})) == {"k"}


def test_a_folded_group_takes_no_sets():
    window = SimpleNamespace(output_concepts=(), input_concepts=())
    sets = ConceptSets(
        outputs={"g": frozenset({"y"}), "w": frozenset({"x", "k"})},
        inputs={"g": frozenset({"x"}), "w": frozenset({"k"})},
        pin_anchors={"g": frozenset({"k"})},
    )
    gg._write_concept_sets({"w": window}, sets)
    assert window.output_concepts == ("k", "x")
    assert window.input_concepts == ("k",)


def test_the_final_spine_reads_the_previous_statement(monkeypatch):
    stated: list[ConceptSets] = []
    spines: list[tuple[ConceptSets, ConceptSets | None]] = []
    concept_sets, merge_axes = gg._concept_sets, gg.merge_axes

    def record_sets(*args, **kwargs):
        stated.append(concept_sets(*args, **kwargs))
        return stated[-1]

    def record_spine(cover, attrs, mandatory, authored, row_columns=None):
        if row_columns is not None:
            spines.append((row_columns.__self__, stated[-1] if stated else None))
        return merge_axes(cover, attrs, mandatory, authored, row_columns)

    monkeypatch.setattr(gg, "_concept_sets", record_sets)
    monkeypatch.setattr(gg, "merge_axes", record_spine)
    model = MODEL.replace(
        "key src enum<string>['MUNI_A', 'OSM_A'];",
        "property tree_id.src enum<string>['MUNI_A', 'OSM_A'];",
    )
    assert build(model) == build(MODEL)
    assert any(statement.outputs for statement, _ in spines)
    assert all(s is last for s, last in spines if s.outputs)


def test_both_passes_read_one_demand(monkeypatch):
    demands: list[gg.Demand] = []
    concept_sets = gg._concept_sets

    def record(group_graph, group_edges, attrs, buckets, demand, stated=None):
        demands.append(demand)
        return concept_sets(group_graph, group_edges, attrs, buckets, demand, stated)

    monkeypatch.setattr(gg, "_concept_sets", record)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    assert len(demands) == 2
    assert demands[0] is demands[1]
