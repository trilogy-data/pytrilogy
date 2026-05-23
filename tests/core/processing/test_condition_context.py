import pytest

from trilogy.core.enums import ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditionContext,
    BuildConditionLayer,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, source_query_concepts


def _concept(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _eq(concept: BuildConcept, value: str) -> BuildComparison:
    return BuildComparison(left=concept, right=value, operator=ComparisonOperator.EQ)


def test_where_clause_exposes_and_atoms():
    city = _concept("city")
    state = _concept("state")
    city_cond = _eq(city, "Boston")
    state_cond = _eq(state, "MA")
    where = BuildWhereClause(conditional=city_cond + state_cond)

    assert [atom.conditional for atom in where.condition_atoms] == [
        city_cond,
        state_cond,
    ]


def test_single_layer_context_preserves_original_condition_tree():
    city = _concept("city")
    state = _concept("state")
    where = BuildWhereClause(conditional=_eq(city, "Boston") + _eq(state, "MA"))

    context = BuildConditionContext.from_where_clause(where)

    assert context is not None
    assert context.as_single_where_clause() is not None
    assert context.as_single_where_clause().conditional is where.conditional


def test_multi_layer_context_errors_at_discovery_boundary():
    city = _concept("city")
    state = _concept("state")
    context = BuildConditionContext(
        layers=(
            BuildConditionLayer.from_where_clause(
                BuildWhereClause(conditional=_eq(city, "Boston"))
            ),
            BuildConditionLayer.from_where_clause(
                BuildWhereClause(conditional=_eq(state, "MA"))
            ),
        )
    )

    with pytest.raises(NotImplementedError, match="Nested condition discovery"):
        source_query_concepts(
            output_concepts=[city],
            history=History(base_environment=Environment()),
            environment=BuildEnvironment(),
            conditions=context,
        )
