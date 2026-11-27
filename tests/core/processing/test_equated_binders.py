from trilogy.core.enums import JoinType, Purpose, SourceType
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import ConceptPair, Join, QueryDatasource
from trilogy.core.processing.join_resolution import _one_value_set

CITY, SPECIES = "local.city", "local.species"


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


def _table(name: str, addresses: list[str]) -> BuildDatasource:
    return BuildDatasource(
        name=name,
        columns=[
            BuildColumnAssignment(alias=a.split(".")[1], concept=_concept(a))
            for a in addresses
        ],
        address=name,
        grain=BuildGrain(),
    )


def _merge(join_type: JoinType, **matched: bool) -> QueryDatasource:
    trees = _table("trees", [CITY, SPECIES])
    enrichment = _table("enrichment", [SPECIES])
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[_concept(CITY), _concept(SPECIES)],
        datasources=[trees, enrichment],
        source_map={CITY: {trees}, SPECIES: {trees, enrichment}},
        grain=BuildGrain(),
        joins=[
            Join(
                left=trees,
                right=enrichment,
                join_type=join_type,
                pairs=[
                    ConceptPair(
                        left=_concept(SPECIES), right=_concept(SPECIES), node=trees
                    )
                ],
                **matched,
            )
        ],
        source_type=SourceType.SELECT,
    )


def test_an_inner_join_with_both_sides_matched_equates_its_key():
    merge = _merge(JoinType.INNER, matched_left=True, matched_right=True)
    assert _one_value_set(merge, _concept(SPECIES)) == {"trees", "enrichment"}
    assert _one_value_set(merge, _concept(CITY)) == {"trees"}


def test_an_inner_join_that_may_drop_rows_proves_nothing():
    merge = _merge(JoinType.INNER, matched_left=True)
    assert _one_value_set(merge, _concept(SPECIES)) == frozenset()
    assert _one_value_set(merge, _concept(CITY)) == {"trees"}
    assert _one_value_set(_merge(JoinType.INNER), _concept(CITY)) == frozenset()


def test_an_outer_join_keeps_rows_but_equates_nothing():
    assert _one_value_set(_merge(JoinType.FULL), _concept(SPECIES)) == frozenset()
    assert _one_value_set(_merge(JoinType.FULL), _concept(CITY)) == {"trees"}
    assert _one_value_set(_merge(JoinType.LEFT_OUTER), _concept(SPECIES)) == frozenset()
