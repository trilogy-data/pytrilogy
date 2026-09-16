"""A sub-request that drops its parent's WHERE must still carry, for labeling,
both that clause and whatever the parent was already deferring: a summary may
not roll up rows a grandparent will filter."""

from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.core import DataType
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _deferred_conditions,
)


def _where(name: str, value: str) -> BuildWhereClause:
    concept = BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )
    return BuildWhereClause(
        conditional=BuildComparison(
            left=concept, right=value, operator=ComparisonOperator.EQ
        )
    )


def _request(conditions, deferred) -> SourceRequest:
    return SourceRequest(
        outputs=[],
        environment=None,  # type: ignore[arg-type]
        graph=None,  # type: ignore[arg-type]
        history=None,  # type: ignore[arg-type]
        conditions=conditions,
        deferred_conditions=deferred,
    )


def test_nothing_to_defer():
    assert _deferred_conditions(_request(None, None)) is None


def test_own_clause_is_deferred():
    own = _where("a", "1")
    assert _deferred_conditions(_request(own, None)) is own


def test_already_deferred_clause_survives_when_there_is_no_own_clause():
    deferred = _where("b", "2")
    assert _deferred_conditions(_request(None, deferred)) is deferred


def test_own_clause_is_conjoined_onto_the_already_deferred_one():
    own, deferred = _where("a", "1"), _where("b", "2")
    merged = _deferred_conditions(_request(own, deferred))
    assert merged is not None
    assert isinstance(merged.conditional, BuildConditional)
    assert merged.conditional.operator == BooleanOperator.AND
    assert merged.conditional.left is deferred.conditional
    assert merged.conditional.right is own.conditional
