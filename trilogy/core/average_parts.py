"""`avg(x)` answered from a stored `sum(x)` and `count(x)`
(`formal/Trilogy/Rollup.lean`, `Rollup.avg_of_parts_exact`).

AVG does not re-aggregate, but SUM and COUNT do, and `/` is true division, so a
datasource storing both parts answers AVG at any grain its own rollup reaches.
"""

from collections import defaultdict
from dataclasses import replace

from trilogy.constants import MagicConstants
from trilogy.core.enums import AggregateGroupingMode, ComparisonOperator, FunctionType
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Function,
)
from trilogy.core.models.environment import Environment

PARTS = (FunctionType.SUM, FunctionType.COUNT)


def _arguments(wrapper: AggregateWrapper) -> tuple[str, ...] | None:
    """The aggregate's arguments when each is a bare concept: `avg(x * 2)`
    matches no stored part of `x`."""
    addresses = []
    for argument in wrapper.function.arguments:
        if not isinstance(argument, (Concept, ConceptRef)):
            return None
        addresses.append(argument.address)
    return tuple(addresses)


def stored_average_arguments(environment: Environment) -> set[tuple[str, ...]]:
    """The arguments of every `avg` one datasource stores both parts of."""
    found: set[tuple[str, ...]] = set()
    for datasource in environment.datasources.values():
        parts: dict[FunctionType, set[tuple[str, ...]]] = defaultdict(set)
        for column in datasource.columns:
            concept = environment.concepts.get(column.concept.address)
            lineage = concept.lineage if concept is not None else None
            if (
                isinstance(lineage, AggregateWrapper)
                and lineage.function.operator in PARTS
            ):
                arguments = _arguments(lineage)
                if arguments is not None:
                    parts[lineage.function.operator].add(arguments)
        found |= parts[FunctionType.SUM] & parts[FunctionType.COUNT]
    return found


def is_stored_average(wrapper: AggregateWrapper, stored: set[tuple[str, ...]]) -> bool:
    return (
        wrapper.function.operator == FunctionType.AVG
        and wrapper.grouping == AggregateGroupingMode.STANDARD
        and (arguments := _arguments(wrapper)) is not None
        and arguments in stored
    )


def average_of_parts(wrapper: AggregateWrapper, environment: Environment) -> Function:
    """`case when count(x) = 0 then null else sum(x) / count(x) end`, each part
    with `wrapper`'s own `by`: a bare `avg` keeps bare parts, which follow the
    select's WHERE as it does. The guard keeps AVG NULL on a group of only
    NULL inputs, where some dialects' `/` gives 0
    (`Rollup.unguarded_quotient_zero_fills`)."""
    factory = FunctionFactory(environment)
    total, count = (
        replace(
            wrapper,
            function=factory.create_function(list(wrapper.function.arguments), op),
        )
        for op in PARTS
    )
    quotient = factory.create_function([total, count], FunctionType.DIVIDE)
    return factory.create_function(
        [
            CaseWhen(
                comparison=Comparison(
                    left=count, right=0, operator=ComparisonOperator.EQ
                ),
                expr=MagicConstants.NULL,
            ),
            CaseElse(expr=quotient),
        ],
        FunctionType.CASE,
    )
