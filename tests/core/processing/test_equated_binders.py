from trilogy.core.enums import JoinType, Purpose, SourceType
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import ConceptPair, Join, QueryDatasource
from trilogy.core.processing.join_resolution import (
    _one_value_set,
    _row_tables,
    _total_input,
    key_providers,
)

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


def test_a_merge_dropping_every_binder_is_the_computed_provider():
    merge = _merge(JoinType.INNER)
    assert key_providers(merge, CITY) == frozenset()
    assert key_providers(merge, CITY, computed=True) == {merge.identifier}
    kept = _merge(JoinType.INNER, matched_left=True)
    assert key_providers(kept, CITY, computed=True) == {"trees"}


def test_a_derived_key_is_one_value_set_at_its_source():
    merge = _merge(JoinType.INNER)
    merge.output_concepts.append(_concept("local.v0"))
    assert key_providers(merge, "local.v0", computed=True) == {merge.identifier}
    assert _one_value_set(merge, _concept("local.v0")) == {merge.identifier}


def _beside_set(existence: bool) -> QueryDatasource:
    trees = _table("trees", [CITY, SPECIES])
    gate = _table("gate", [CITY])
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[_concept(CITY)],
        datasources=[trees, gate],
        source_map={CITY: {trees} | (set() if existence else {gate})},
        existence_source_map={"local.member": {gate}} if existence else {},
        grain=BuildGrain(),
        joins=[],
        source_type=SourceType.SELECT,
    )


def test_a_set_only_a_subselect_reads_binds_no_value():
    assert _one_value_set(_beside_set(True), _concept(CITY)) == {"trees"}
    assert _row_tables(_beside_set(True)) == {"trees"}
    assert _one_value_set(_beside_set(False), _concept(CITY)) == frozenset()
    assert _row_tables(_beside_set(False)) == {"trees", "gate"}


def test_rows_of_a_padded_side_hold_no_row_of_it():
    assert _row_tables(_merge(JoinType.INNER)) == {"trees", "enrichment"}
    assert _row_tables(_merge(JoinType.LEFT_OUTER)) == {"trees"}
    assert _row_tables(_merge(JoinType.FULL)) == frozenset()


def test_a_total_reads_its_table_bare_or_not_at_all():
    trees = _table("trees", [CITY])
    total = QueryDatasource(
        input_concepts=[],
        output_concepts=[_concept(CITY)],
        datasources=[trees],
        source_map={CITY: {trees}},
        grain=BuildGrain(),
        joins=[],
        source_type=SourceType.GROUP,
    )
    assert _total_input(total) == "trees"
    assert _total_input(_merge(JoinType.INNER)) is None
