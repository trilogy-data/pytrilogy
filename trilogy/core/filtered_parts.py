"""`agg(x ? c)` answered from a stored `agg(x)` (`formal/Trilogy/Cover.lean`,
`Cover.filtered_partials_exact`).

A row-level condition is a function of its own columns, so grouping the rows
by the aggregate's grain plus those columns puts each row in exactly one group
where `c` is constant. The filtered aggregate is then the re-aggregate of the
groups `c` keeps: `sum(case when c then count(x) by G, C end)`, `0` where no
group is kept for a count. The inner aggregate is a plain rollable one, so a
summary storing `agg(x)` at a grain that reaches `G` and `C` serves it.
"""

from collections.abc import Iterable

from trilogy.core.enums import Derivation, FunctionType, Purpose
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    Parenthetical,
    SubselectComparison,
)
from trilogy.core.models.environment import Environment

REAGGREGATES = {
    FunctionType.COUNT: FunctionType.SUM,
    FunctionType.SUM: FunctionType.SUM,
    FunctionType.MIN: FunctionType.MIN,
    FunctionType.MAX: FunctionType.MAX,
}


def _filtered_argument(
    wrapper: AggregateWrapper,
) -> tuple[ConceptRef, FilterItem] | None:
    arguments = wrapper.function.arguments
    if len(arguments) != 1 or not isinstance(arguments[0], FilterItem):
        return None
    content = arguments[0].content
    if not isinstance(content, (Concept, ConceptRef)):
        return None
    return content.reference if isinstance(content, Concept) else content, arguments[0]


def _case_condition(
    condition: object,
) -> Conditional | SubselectComparison | Comparison | Between | None:
    while isinstance(condition, Parenthetical):
        condition = condition.content
    if isinstance(condition, (Conditional, SubselectComparison, Comparison, Between)):
        return condition
    return None


def _stored_operand(column: Concept) -> tuple[FunctionType, str] | None:
    lineage = column.lineage
    if not isinstance(lineage, AggregateWrapper):
        return None
    arguments = lineage.function.arguments
    if len(arguments) != 1 or not isinstance(arguments[0], (Concept, ConceptRef)):
        return None
    return lineage.function.operator, arguments[0].address


def _reachable(
    address: str, bound: set[str], environment: Environment, seen: frozenset[str]
) -> bool:
    """A function of a datasource's row: bound by it, a property of keys that
    are, or a scalar of such columns. Nothing that fans a row out."""
    if address in bound:
        return True
    concept = environment.concepts.get(address)
    if concept is None or address in seen:
        return False
    seen = seen | {address}
    if concept.derivation == Derivation.CONSTANT:
        return True
    if concept.derivation == Derivation.BASIC and isinstance(concept.lineage, Function):
        arguments = concept.lineage.concept_arguments
        return bool(arguments) and all(
            _reachable(a.address, bound, environment, seen) for a in arguments
        )
    return (
        concept.derivation == Derivation.ROOT
        and concept.purpose == Purpose.PROPERTY
        and bool(concept.keys)
        and all(_reachable(k, bound, environment, seen) for k in concept.keys or ())
    )


def _served_by(
    environment: Environment,
    operator: FunctionType,
    operand: str,
    columns: Iterable[str],
) -> bool:
    """Some datasource stores `operator(operand)` and reaches every column."""
    columns = list(columns)
    for datasource in environment.datasources.values():
        bound = {c.concept.address for c in datasource.columns}
        stored = {
            _stored_operand(concept)
            for c in datasource.columns
            if (concept := environment.concepts.get(c.concept.address)) is not None
        }
        if (operator, operand) in stored and all(
            _reachable(c, bound, environment, frozenset()) for c in columns
        ):
            return True
    return False


def filtered_of_parts(
    wrapper: AggregateWrapper,
    grain: list[str],
    filtering: list[str],
    environment: Environment,
) -> Function | AggregateWrapper | None:
    """`wrapper` as the re-aggregate of its unfiltered partials at its grain
    plus the condition's columns, when a datasource stores those partials and
    reaches the select's `filtering` columns too: under a WHERE no summary
    can apply, the rewrite would only scan the rows twice."""
    if wrapper.function.operator not in REAGGREGATES or wrapper.grouping_sets:
        return None
    found = _filtered_argument(wrapper)
    if found is None:
        return None
    operand, item = found
    condition = _case_condition(item.where.conditional)
    if condition is None:
        return None
    columns = [c.address for c in item.where.conditional.concept_arguments]
    if not columns:
        return None
    by = [b.address for b in wrapper.by] if wrapper.by else grain
    keys = list(dict.fromkeys([*by, *columns]))
    if not _served_by(
        environment, wrapper.function.operator, operand.address, keys + filtering
    ):
        return None
    factory = FunctionFactory(environment)
    partial = AggregateWrapper(
        function=factory.create_function([operand], wrapper.function.operator),
        by=[environment.concepts[k].reference for k in keys],
    )
    kept = factory.create_function(
        [CaseWhen(comparison=condition, expr=partial)], FunctionType.CASE
    )
    reaggregated = AggregateWrapper(
        function=factory.create_function(
            [kept], REAGGREGATES[wrapper.function.operator]
        ),
        by=wrapper.by,
        grouping=wrapper.grouping,
        grain_inherited=wrapper.grain_inherited,
    )
    if wrapper.function.operator != FunctionType.COUNT:
        return reaggregated
    return factory.create_function([reaggregated, 0], FunctionType.COALESCE)
