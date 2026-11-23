from trilogy import Environment
from trilogy.core.enums import SourceType
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource


def _count() -> BuildConcept:
    env = Environment()
    env.parse("""
key order_id int;
key customer_id int;
auto n <- count(order_id) by customer_id;
""")
    return env.materialize_for_select().concepts["local.n"]


def _source(
    n: BuildConcept, group: bool, rolled: bool = False, region: bool = False
) -> QueryDatasource:
    return QueryDatasource(
        input_concepts=[n],
        output_concepts=[n],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={n.address: set()},
        nullable_concepts=[n],
        rollup_concepts=[n] if rolled else [],
        source_type=SourceType.GROUP if group else SourceType.SELECT,
        region_zero_filled=frozenset({n.address}) if region else frozenset(),
    )


def _cte(source: QueryDatasource) -> CTE:
    return CTE(
        name="c",
        source=source,
        output_columns=list(source.output_concepts),
        source_map={},
        grain=BuildGrain(),
        group_to_grain=source.group_required,
        nullable_concepts=list(source.nullable_concepts),
        rollup_concepts=list(source.rollup_concepts),
        region_zero_filled=source.region_zero_filled,
        zero_filled=source.zero_filled,
    )


def test_source_decides_a_padded_count_is_zero_filled():
    n = _count()
    assert n.zero_on_empty
    assert _source(n, group=False).zero_filled == {n.address}
    assert _source(n, group=True).zero_filled == frozenset()
    assert _source(n, group=True, rolled=True).zero_filled == {n.address}


def test_cte_reads_the_source_decision():
    n = _count()
    cte = _cte(_source(n, group=False))
    assert cte.zero_fills_count(n)
    cte.nullable_concepts = []
    assert cte.zero_fills_count(n)


def test_regroup_redecides_zero_fill():
    n = _count()
    cte = _cte(_source(n, group=False))
    cte.regroup(True)
    assert not cte.zero_fills_count(n)
    cte.regroup(False)
    assert cte.zero_fills_count(n)


def test_region_padding_survives_regrouping():
    n = _count()
    source = _source(n, group=True, region=True)
    assert source.zero_filled == {n.address}
    cte = _cte(_source(n, group=False, region=True))
    cte.regroup(True)
    assert cte.zero_fills_count(n)
