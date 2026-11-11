"""The rehome reads the sets the demand pass states as a value: no group holds
its sets until every fold, placement and read is made."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_window_input_carry import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import group_graph as gg

_QUERY = "select vehicle, gap_class;"


def _held(attrs) -> set[str]:
    return {
        gid
        for gid, a in attrs.items()
        if gid != gg.FINAL_NODE_ID and (a.output_concepts or a.input_concepts)
    }


class _SetsLog:
    def __init__(self) -> None:
        self.held: dict[str, set[str]] = {}
        self.sets: list[gg.ConceptSets] = []
        self.steps: list[str] = []
        self.reads: dict = {}
        self.written: list[tuple[dict[str, tuple], gg.ConceptSets]] = []

    def watch(self, name, fn):
        def watched(group_graph, group_edges, attrs, sets, *rest):
            self.held[name] = _held(attrs)
            self.sets.append(sets)
            self.steps.append(name)
            out = fn(group_graph, group_edges, attrs, sets, *rest)
            if name == "reads":
                self.reads = out
            return out

        return watched

    def write(self, attrs, buckets, sets):
        if not self.written:
            self.steps.append("write")
        _WRITE(attrs, buckets, sets)
        outputs = {gid: a.output_concepts for gid, a in attrs.items()}
        self.written.append((outputs, sets))


_FOLDS = gg._elect_folds
_READS = gg._elect_reads
_WRITE = gg._write_concept_sets


@pytest.fixture
def log(monkeypatch) -> _SetsLog:
    log = _SetsLog()
    monkeypatch.setattr(gg, "_elect_folds", log.watch("folds", _FOLDS))
    monkeypatch.setattr(gg, "_elect_reads", log.watch("reads", _READS))
    monkeypatch.setattr(gg, "_write_concept_sets", log.write)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_no_group_holds_its_sets_while_the_rehome_elects(log):
    assert log.reads
    assert log.held == {"folds": set(), "reads": set()}
    assert log.sets[0] is log.sets[1]
    assert log.sets[0].inputs


def test_the_stated_sets_are_written_once_the_reads_are_applied(log):
    assert log.steps[:3] == ["folds", "reads", "write"]
    outputs, sets = log.written[0]
    assert sets is log.sets[0]
    assert {gid: o for gid, o in outputs.items() if gid in sets.outputs} == {
        gid: tuple(sorted(o)) for gid, o in sets.outputs.items() if gid in outputs
    }


def test_a_folded_group_takes_no_sets():
    window = SimpleNamespace(output_concepts=(), input_concepts=())
    bucket = SimpleNamespace(output_concepts=())
    sets = gg.ConceptSets(
        outputs={"g": frozenset({"y"}), "w": frozenset({"x", "k"})},
        inputs={"g": frozenset({"x"}), "w": frozenset({"k"})},
        pin_anchors={"g": frozenset({"k"})},
    )
    gg._write_concept_sets({"w": window}, {"w": bucket}, sets)
    assert window.output_concepts == bucket.output_concepts == ("k", "x")
    assert window.input_concepts == ("k",)
