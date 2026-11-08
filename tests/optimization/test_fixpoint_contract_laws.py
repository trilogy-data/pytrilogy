"""The optimizer's WHERE-atom contract, as modeled in formal/Trilogy/Fixpoint.lean."""

import logging

import pytest

import trilogy.core.processing.concept_strategies_v4 as v4
from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core.exceptions import OptimizationNonConvergence
from trilogy.core.models import build
from trilogy.core.optimization import build_optimization_rule_plan
from trilogy.core.optimizations import predicate_pushdown
from trilogy.core.optimizations.atom_contract import FACT_REMOVERS, REMOVERS

# A complete scan and two partial ones, each read in its own CTE: pushdown
# offers `tree_id is not null` to every scan, and the strip drops it from the
# ones whose tree_id cannot be NULL (issue #830).
PARTIAL_SCANS = """
key tree_id string;
key species string;
key genus string;
property tree_id.height_m float;
root datasource trees (tree_id: tree_id, species: species)
grain (tree_id) file `https://example.com/trees.parquet`;
root datasource enrichment (species: ~species, genus: ?genus)
grain (species) file `https://example.com/enrichment.parquet`;
root datasource heights (tree_id: ~tree_id, height_m: ?height_m)
grain (tree_id) file `https://example.com/heights.parquet`;
"""
QUERY = "select tree_id, height_m, genus where tree_id is not null;"

# A NULL key on the heights side keeps its join FULL, so the WHERE reads the
# coalesced tree_id and survives at the final CTE for pushdown to re-offer.
NULL_KEYED_SCANS = PARTIAL_SCANS.replace("tree_id: ~tree_id", "tree_id: ~?tree_id")


def _sql(model: str = PARTIAL_SCANS) -> str:
    env = Environment()
    env.parse(model)
    return Dialects.DUCK_DB.default_executor(environment=env).generate_sql(QUERY)[-1]


def _ungate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(predicate_pushdown, "removable", lambda cte, atom: None)


def test_fact_removers_are_driver_phases():
    phases = {phase.name for phase in build_optimization_rule_plan()}
    assert set(REMOVERS) <= phases
    assert set(FACT_REMOVERS) == {"strip_redundant_not_null"}


def test_contract_holds():
    assert CONFIG.optimizations.enforce_fixpoint_contract
    sql = _sql()
    assert sql.count("is not null") == 1


def test_ungated_pushdown_breaks_contract(monkeypatch: pytest.MonkeyPatch):
    """`Fixpoint.ungated_breaks_contract`, caught at the phase that adds."""
    _ungate(monkeypatch)
    with pytest.raises(OptimizationNonConvergence) as err:
        _sql()
    assert "predicate_pushdown" in str(err.value)
    assert "strip_redundant_not_null drops it" in str(err.value)


def test_ungated_pushdown_cycles(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """`Fixpoint.ungated_cycles`: unchecked, the two rules run to the pass cap."""
    _ungate(monkeypatch)
    monkeypatch.setattr(CONFIG.optimizations, "enforce_fixpoint_contract", False)
    with caplog.at_level(logging.WARNING):
        _sql(NULL_KEYED_SCANS)
    assert "MAX_OPTIMIZATION_PASSES" in caplog.text


def test_nested_row_grains_once_per_concept(monkeypatch: pytest.MonkeyPatch):
    """Each x_i reads x_{i-1} and x_{i-2}: the lineage has exponentially many
    paths but one row set per concept, so one grain each."""
    calls: list[int] = []
    keyed = v4._keyed_grain
    monkeypatch.setattr(
        v4, "_keyed_grain", lambda *a, **k: calls.append(1) or keyed(*a, **k)
    )
    n = 16
    lines = [
        "key id int;",
        "property id.x0 float;",
        "property id.x1 float;",
        "datasource t (id: id, x0: x0, x1: x1) grain (id) address t;",
        *(f"auto x{i} <- x{i - 1} + x{i - 2};" for i in range(2, n)),
    ]
    env = Environment()
    env.parse("\n".join(lines))
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
        f"select id, x{n - 1};"
    )
    assert len(calls) <= n


def test_grain_reads_determinants_once(monkeypatch: pytest.MonkeyPatch):
    env = Environment()
    env.parse(
        "key id int;\n"
        + "\n".join(f"property id.p{i} int;" for i in range(40))
        + "\ndatasource t (id: id, "
        + ", ".join(f"p{i}: p{i}" for i in range(40))
        + ") grain (id) address t;"
    )
    built = env.materialize_for_select()
    concepts = [built.concepts["local.id"]] + [
        built.concepts[f"local.p{i}"] for i in range(40)
    ]
    calls: list[int] = []
    collect = build.concept_collection_equivalent_addresses
    monkeypatch.setattr(
        build,
        "concept_collection_equivalent_addresses",
        lambda c: calls.append(1) or collect(c),
    )
    assert build.BuildGrain.from_concepts(concepts).components == {"local.id"}
    assert len(calls) == 1
