"""Guard for the q64 token sink — `Could not find CTE for datasource ...`.

`QueryDatasource._compute_identifier` composed the `_join_` prefix from
`self.datasources` in list order. `__post_init__` sorts datasources only at
construction, but optimization passes reassign `datasources` afterward without
re-sorting — so the same logical join could present its members in two orders,
yielding two identifiers (`A_join_B_...` vs `B_join_A_...`) with an identical
grain/filter suffix. `get_datasource_cte` matches by identifier string, so the
built CTE became unfindable and `base_join_to_join` raised a bare ValueError
that escaped `generate_sql`.

Repro: evals/tpcds_agent/bug_q64_join_datasource_identifier_order.md
"""

from dataclasses import dataclass

from trilogy.core.enums import SetOperator, SourceType
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import QueryDatasource


@dataclass
class _DS:
    identifier: str


def _qds(
    order: list[str],
    set_operator: SetOperator = SetOperator.UNION_ALL,
    source_type: SourceType = SourceType.SELECT,
):
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[],
        datasources=[_DS(x) for x in order],
        source_map={},
        grain=BuildGrain(components=set()),
        joins=[],
        set_operator=set_operator,
        source_type=source_type,
    )


def test_join_identifier_order_independent_after_mutation():
    ab = _qds(["z_alpha", "a_beta"])
    ba = _qds(["a_beta", "z_alpha"])
    # Optimization passes reassign datasources post-construction (no
    # __post_init__ re-sort) — this is what desyncs the two orders in q64.
    ab.datasources = [_DS("z_alpha"), _DS("a_beta")]
    ba.datasources = [_DS("a_beta"), _DS("z_alpha")]
    assert ab.identifier == ba.identifier
    assert ab.identifier.startswith("a_beta_join_z_alpha")


def test_union_arm_order_preserved_for_except():
    # EXCEPT is non-commutative: arm order stays semantic, must NOT be sorted.
    ab = _qds(
        ["z_alpha", "a_beta"],
        set_operator=SetOperator.EXCEPT,
        source_type=SourceType.UNION,
    )
    ab.datasources = [_DS("z_alpha"), _DS("a_beta")]
    assert ab.identifier.startswith("z_alpha_union_a_beta")
