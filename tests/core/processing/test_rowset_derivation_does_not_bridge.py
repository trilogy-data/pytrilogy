"""A derivation over two rowsets, or over a rowset and a base concept, is not a
join path: `a.n + b.m` beside the handles once welded both islands into one
component and the plan cross-joined them (`FULL JOIN ... on 1=1`). The gate
judges a derivation by what it reads, and names the reads it cannot relate.
"""

import pytest

from tests.helpers.models import CUSTOMERS_MATERIALIZED
from tests.helpers.rows import executor_for
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.executor import Executor

A = "with a as where status = 'delivered' select customer_id, count(order_id) as n;\n"
B = "with b as where status = 'in-transit' select customer_id, count(order_id) as m;\n"
S = "with s as select status, count(order_id) as m;\n"


@pytest.fixture(scope="module")
def ex() -> Executor:
    return executor_for(CUSTOMERS_MATERIALIZED)


def _rows(ex: Executor, query: str) -> list[tuple]:
    return sorted(ex.execute_text(query)[-1].fetchall(), key=str)


def _subgraphs(ex: Executor, query: str) -> list[list[str]]:
    with pytest.raises(DisconnectedConceptsException) as exc:
        ex.generate_sql(query)
    return exc.value.subgraphs


@pytest.mark.parametrize(
    "query",
    [
        A + S + "select a.customer_id, s.status, a.n + s.m as x;",
        A + S + "select a.n + s.m as x;",
        A + S + "select a.customer_id, s.status, a.n - coalesce(s.m, 0) as x;",
    ],
)
def test_a_derivation_over_two_rowsets_names_both_reads(
    ex: Executor, query: str
) -> None:
    subgraphs = _subgraphs(ex, query)
    assert len(subgraphs) == 2
    assert any("a.n" in group for group in subgraphs)
    assert any("s.m" in group for group in subgraphs)


def test_a_derivation_over_a_rowset_and_a_base_concept_names_the_join(
    ex: Executor,
) -> None:
    with pytest.raises(DisconnectedConceptsException) as exc:
        ex.generate_sql(A + "select customer_id, a.n + order_id as x;")
    assert "`subset join a.customer_id = customer_id`" in str(exc.value)


def test_a_derivation_downstream_of_a_rowset_derivation_still_does_not_bridge(
    ex: Executor,
) -> None:
    subgraphs = _subgraphs(
        ex, A + "auto y <- a.n * 2;\nselect customer_id, y + order_id as z;"
    )
    assert len(subgraphs) == 2


def test_a_derivation_read_off_one_rowset_alone_plans(ex: Executor) -> None:
    assert _rows(ex, A + "select a.customer_id, a.n * 2 as x;") == [(1, 2), (2, 2)]


def test_a_derivation_across_a_declared_join_plans(ex: Executor) -> None:
    assert _rows(
        ex,
        A + B + "select a.customer_id, a.n - b.m as d "
        "subset join b.customer_id = a.customer_id;",
    ) == [(1, 0), (2, None)]
    assert _rows(
        ex,
        A + "select customer_id, a.n + order_id as x "
        "subset join a.customer_id = customer_id;",
    ) == [(1, 101), (1, 102), (2, 103), (3, None)]
