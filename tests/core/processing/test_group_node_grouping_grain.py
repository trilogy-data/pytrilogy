"""Unit coverage for the non-standard-grouping (ROLLUP/CUBE/GROUPING SETS)
helpers in ``node_generators.group_node``.

A coalescing scoped join can canonicalize an aggregate's grain components to
the join-mate's addresses while the wrapper's ``by`` list keeps the authored
ones; the grouped CTE renders GROUP BY from ``by`` verbatim, so any grain dim
emitted under a different address becomes a bare, ungrouped column (illegal
SQL — the q14 BinderException). These paths depend on which side the
pseudonym collapse picks as canonical, so they can't be forced from a small
end-to-end model; pin them at the function level instead.
"""

from trilogy.core.enums import AggregateGroupingMode, Derivation, FunctionType, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFunction,
    BuildGrain,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.processing.node_generators.group_node import (
    _matching_by_concept,
    _pseudonym_linked,
    _resolve_grain_components,
)


def _concept(
    name: str,
    *,
    purpose: Purpose = Purpose.KEY,
    pseudonyms: set[str] | None = None,
    grain: BuildGrain | None = None,
    lineage=None,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=purpose,
        build_is_aggregate=False,
        derivation=Derivation.ROOT,
        namespace="test",
        grain=grain or BuildGrain(),
        keys=None,
        pseudonyms=pseudonyms or set(),
        lineage=lineage,
    )


def _rollup_aggregate(
    by: list[BuildConcept], grain_addresses: list[str]
) -> BuildConcept:
    wrapper = BuildAggregateWrapper(
        function=BuildFunction(
            operator=FunctionType.SUM,
            arguments=[_concept("value", purpose=Purpose.PROPERTY)],
            output_data_type=DataType.FLOAT,
            output_purpose=Purpose.METRIC,
            arg_count=1,
        ),
        by=by,
        grouping=AggregateGroupingMode.ROLLUP,
    )
    concept = _concept(
        "total",
        purpose=Purpose.METRIC,
        grain=BuildGrain(components=set(grain_addresses)),
        lineage=wrapper,
    )
    return concept


def _env(*concepts: BuildConcept) -> BuildEnvironment:
    env = BuildEnvironment()
    for c in concepts:
        env.concepts[c.address] = c
    return env


def test_rollup_grain_component_pinned_to_by_address():
    authored = _concept("nov.k", pseudonyms={"test.mate.k"})
    mate = _concept("mate.k", pseudonyms={"test.nov.k"})
    env = _env(authored, mate)
    # grain canonicalized to the mate's address; by keeps the authored one
    concept = _rollup_aggregate(by=[authored], grain_addresses=[mate.address])
    resolved = _resolve_grain_components(concept, [mate], env)
    assert [c.address for c in resolved] == [authored.address]


def test_rollup_grain_component_matching_by_address_kept():
    authored = _concept("nov.k")
    env = _env(authored)
    concept = _rollup_aggregate(by=[authored], grain_addresses=[authored.address])
    resolved = _resolve_grain_components(concept, [], env)
    assert [c.address for c in resolved] == [authored.address]


def test_standard_aggregate_keeps_equivalent_swap():
    authored = _concept("nov.k", pseudonyms={"test.mate.k"})
    mate = _concept("mate.k", pseudonyms={"test.nov.k"})
    env = _env(authored, mate)
    concept = _rollup_aggregate(by=[authored], grain_addresses=[mate.address])
    assert isinstance(concept.lineage, BuildAggregateWrapper)
    concept.lineage.grouping = AggregateGroupingMode.STANDARD
    resolved = _resolve_grain_components(concept, [mate], env)
    # no pinning for standard grouping: the local_optional equivalent wins
    assert [c.address for c in resolved] == [mate.address]


def test_matching_by_concept_via_reverse_pseudonym():
    # the grain address knows the by member as a pseudonym, not vice versa
    authored = _concept("nov.k")
    mate = _concept("mate.k", pseudonyms={"test.nov.k"})
    env = _env(authored, mate)
    assert _matching_by_concept(mate.address, [authored], env) is authored
    assert _matching_by_concept("test.unrelated", [authored], env) is None


def test_pseudonym_linked_both_directions():
    authored = _concept("nov.k", pseudonyms={"test.mate.k"})
    mate_forward = _concept("mate.k")
    assert _pseudonym_linked(mate_forward, [authored])
    unrelated = _concept("other.k")
    assert not _pseudonym_linked(unrelated, [authored])
