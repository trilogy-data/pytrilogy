"""One truth for partial and nullable bindings: a scan stamps its projected
outputs from the datasource columns minus its proofs, every node inherits from
its resolved parents, and a merge derives its stamp from its sides and the
resolved join types."""

from trilogy import Dialects
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import BaseJoin, QueryDatasource
from trilogy.core.processing.join_resolution import (
    merge_partial_addresses,
    preserved_sources,
)
from trilogy.core.query_processor import get_query_datasources
from trilogy.parser import parse_text

MODEL = """
key product_id int;
property product_id.brand string;
property product_id.color string?;

key sale_id int;
property sale_id.amount float;
property sale_id.note string?;

datasource products (
    id: product_id,
    brand: brand,
    color: color,
)
grain (product_id)
query '''select 1 as id, 'a' as brand, null as color union all select 2, 'b', 'red' ''';

datasource sales (
    id: sale_id,
    product_id: ~product_id,
    amount: amount,
    note: note,
)
grain (sale_id)
query '''select 1 as id, 1 as product_id, 10.0 as amount, null as note''';
"""


def _final(executor, query: str) -> QueryDatasource:
    _, statements = parse_text(query, executor.environment)
    return get_query_datasources(
        environment=executor.environment, statement=statements[-1]
    )


def _scan(qds: QueryDatasource, table: str) -> QueryDatasource:
    stack = [qds]
    while stack:
        current = stack.pop()
        if isinstance(current, QueryDatasource):
            base = current.base_datasource
            if (
                current.source_type.value == "direct_select"
                and base is not None
                and base.name == table
            ):
                return current
            stack.extend(
                d for d in current.datasources if isinstance(d, QueryDatasource)
            )
    raise AssertionError(f"no scan of {table}")


def test_scan_stamps_only_projected_columns():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    scan = _scan(_final(executor, "select sale_id, amount;"), "sales")
    assert {c.address for c in scan.partial_concepts} == set()
    assert {c.address for c in scan.nullable_concepts} == set()


def test_scan_stamps_projected_partial_and_nullable():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    scan = _scan(_final(executor, "select sale_id, product_id, note;"), "sales")
    assert {c.address for c in scan.partial_concepts} == {"local.product_id"}
    assert {c.address for c in scan.nullable_concepts} == {"local.note"}


def test_scan_condition_proof_strips_nullable():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    scan = _scan(
        _final(executor, "where note is not null select sale_id, note;"), "sales"
    )
    assert {c.address for c in scan.nullable_concepts} == set()


class _Side:
    def __init__(self, identifier: str, outputs: set[str], partial: set[str]):
        self.identifier = identifier
        self.output_concepts = [_Concept(a) for a in outputs]
        self.partial_concepts = [_Concept(a) for a in partial]


class _Concept:
    def __init__(self, address: str):
        self.address = address


def _join(left: _Side, right: _Side, join_type: JoinType) -> BaseJoin:
    return BaseJoin(left_datasource=left, right_datasource=right, join_type=join_type)


def test_merge_partials_follow_join_types():
    fact = _Side("fact", {"k", "m"}, {"k"})
    dim = _Side("dim", {"k", "d"}, set())
    outputs = [_Concept("k"), _Concept("m"), _Concept("d")]
    sides = [fact, dim]
    assert merge_partial_addresses(
        sides, [_join(fact, dim, JoinType.INNER)], outputs
    ) == {"k"}
    assert merge_partial_addresses(
        sides, [_join(fact, dim, JoinType.LEFT_OUTER)], outputs
    ) == {"k"}
    assert (
        merge_partial_addresses(sides, [_join(fact, dim, JoinType.FULL)], outputs)
        == set()
    )
    assert (
        merge_partial_addresses(
            sides, [_join(fact, dim, JoinType.RIGHT_OUTER)], outputs
        )
        == set()
    )


def test_preserved_sources_reset_by_row_dropping_joins():
    a = _Side("a", {"k"}, set())
    b = _Side("b", {"k"}, set())
    c = _Side("c", {"k"}, set())
    joins = [_join(a, b, JoinType.FULL), _join(b, c, JoinType.INNER)]
    assert preserved_sources([a, b, c], joins) == set()
    joins = [_join(a, b, JoinType.INNER), _join(b, c, JoinType.FULL)]
    assert preserved_sources([a, b, c], joins) == {"c"}
