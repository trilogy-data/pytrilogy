from trilogy.core.enums import JoinType, Purpose
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import BaseJoin, ConceptPair
from trilogy.core.processing.grain_utility import _left_join_sources
from trilogy.core.processing.utility import join_left_sources, left_deep_joins

KEY = "local.k"


def _concept(address: str) -> BuildConcept:
    namespace, name = address.split(".")
    return BuildConcept(
        name=name,
        canonical_name=name,
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(components=set()),
    )


def _scan(name: str) -> BuildDatasource:
    return BuildDatasource(
        name=name,
        address=name,
        columns=[BuildColumnAssignment(alias="k", concept=_concept(KEY))],
        grain=BuildGrain(),
    )


def _join(left: BuildDatasource, paired: BuildDatasource, right: BuildDatasource):
    return BaseJoin(
        left_datasource=left,
        right_datasource=right,
        join_type=JoinType.FULL,
        concept_pairs=[
            ConceptPair(
                left=_concept(KEY), right=_concept(KEY), existing_datasource=paired
            )
        ],
    )


def test_left_sources_are_the_declared_side_and_every_pair_source():
    a, b, c = _scan("a"), _scan("b"), _scan("c")
    join = _join(a, b, c)
    expected = [a.identifier, b.identifier]
    assert [s.identifier for s in join_left_sources(join)] == expected
    assert [s.identifier for s in _left_join_sources(join, [a, b, c])] == expected
    assert left_deep_joins([join])[0][1] == frozenset(expected)


def test_keyless_join_without_a_declared_side_reads_every_other_source():
    a, b, c = _scan("a"), _scan("b"), _scan("c")
    join = BaseJoin(right_datasource=c, join_type=JoinType.FULL, concepts=[])
    assert join_left_sources(join) == []
    assert [s.identifier for s in _left_join_sources(join, [a, b, c])] == [
        a.identifier,
        b.identifier,
    ]
