from datetime import date, datetime
from typing import Any, Iterable, List, Mapping, Tuple

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX, MagicConstants
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    Modifier,
    Purpose,
    WindowType,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.functions import function_args_to_output_purpose
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    Between,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptArgs,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    HavingClause,
    ListWrapper,
    MapWrapper,
    Metadata,
    MultiSelectLineage,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    SubselectComparison,
    SubselectItem,
    TupleWrapper,
    UndefinedConcept,
    UnionSelectLineage,
    WhereClause,
    WindowItem,
    get_concept_arguments,
)
from trilogy.core.models.core import (
    DataType,
    TraitDataType,
    arg_to_datatype,
    is_compatible_datatype,
    merge_datatypes,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import SelectStatement
from trilogy.parsing.helpers import Meta
from trilogy.utility import string_to_hash, unique

ARBITRARY_INPUTS = (
    AggregateWrapper
    | FunctionCallWrapper
    | WindowItem
    | FilterItem
    | SubselectItem
    | Function
    | Parenthetical
    | ListWrapper
    | MapWrapper
    | Comparison
    | int
    | float
    | str
    | date
    | bool
)

Expr = (
    Function
    | FilterItem
    | WindowItem
    | AggregateWrapper
    | FunctionCallWrapper
    | Parenthetical
    | SubselectItem
    | ConceptRef
)


def unwrap_transformation(
    input: Expr,
    environment: Environment,
) -> (
    Function
    | FilterItem
    | WindowItem
    | AggregateWrapper
    | FunctionCallWrapper
    | Parenthetical
    | SubselectItem
):
    if isinstance(input, Function):
        return input
    elif isinstance(input, AggregateWrapper):
        return input
    elif isinstance(input, ConceptRef):
        concept = environment.concepts[input.address]
        return Function(
            operator=FunctionType.ALIAS,
            output_datatype=concept.datatype,
            output_purpose=concept.purpose,
            arguments=[input],
        )
    elif isinstance(input, FilterItem):
        return input
    elif isinstance(input, WindowItem):
        return input
    elif isinstance(input, FunctionCallWrapper):
        return input
    elif isinstance(input, Parenthetical):
        return input
    elif isinstance(input, SubselectItem):
        return input
    elif isinstance(input, Comparison):
        # A comparison / membership (`x > 5`, `x in other`) references real
        # concepts, so it must not fall through to the CONSTANT wrapper below —
        # that mislabels the derived column as a literal, and a downstream
        # `flag = true` then constant-folds to `1 = 0`. Route it through the
        # parenthetical path (the form a user would write with explicit
        # parens), which the build pipeline handles correctly. Covers
        # SubselectComparison (a Comparison subclass).
        return Parenthetical(content=input)
    else:
        return Function(
            operator=FunctionType.CONSTANT,
            output_datatype=arg_to_datatype(input),
            output_purpose=Purpose.CONSTANT,
            arguments=[input],
        )


def process_function_arg(
    arg,
    meta: Meta | None,
    environment: Environment,
):
    # if a function has an anonymous function argument
    # create an implicit concept
    if isinstance(arg, Parenthetical):
        processed = process_function_args([arg.content], meta, environment)
        return Function(
            operator=FunctionType.PARENTHETICAL,
            arguments=processed,
            output_datatype=arg_to_datatype(processed[0]),
            output_purpose=function_args_to_output_purpose(processed, environment),
        )
    elif isinstance(arg, Function):
        # if it's not an aggregate function, we can skip the virtual concepts
        # to simplify anonymous function handling
        if (
            arg.operator not in FunctionClass.AGGREGATE_FUNCTIONS.value
            and arg.operator not in FunctionClass.ONE_TO_MANY.value
        ):
            return arg
        id_hash = string_to_hash(str(arg))
        name = f"{VIRTUAL_CONCEPT_PREFIX}_{arg.operator.value}_{id_hash}"
        if f"{environment.namespace}.{name}" in environment.concepts:
            return environment.concepts[f"{environment.namespace}.{name}"]
        concept = function_to_concept(
            arg,
            name=name,
            environment=environment,
        )
        # to satisfy mypy, concept will always have metadata
        if concept.metadata and meta:
            concept.metadata.line_number = meta.line
        environment.add_concept(concept, meta=meta)
        return concept.reference
    elif isinstance(
        arg,
        (ListWrapper, MapWrapper),
    ):
        id_hash = string_to_hash(str(arg))
        name = f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}"
        if f"{environment.namespace}.{name}" in environment.concepts:
            return environment.concepts[f"{environment.namespace}.{name}"]
        concept = arbitrary_to_concept(
            arg,
            name=name,
            environment=environment,
        )
        if concept.metadata and meta:
            concept.metadata.line_number = meta.line
        environment.add_concept(concept, meta=meta)
        return concept.reference
    elif isinstance(arg, SubselectItem):
        id_hash = string_to_hash(str(arg))
        name = f"{VIRTUAL_CONCEPT_PREFIX}_subselect_{arg.content.name}_{id_hash}"
        if f"{environment.namespace}.{name}" in environment.concepts:
            return environment.concepts[f"{environment.namespace}.{name}"]
        concept = subselect_to_concept(arg, name, environment.namespace, environment)
        if concept.metadata and meta:
            concept.metadata.line_number = meta.line
        environment.add_concept(concept, meta=meta)
        return concept.reference
    elif isinstance(arg, Concept):
        return arg.reference
    elif isinstance(arg, ConceptRef):
        return environment.concepts[arg.address].reference
    return arg


def process_function_args(
    args,
    meta: Meta | None,
    environment: Environment,
) -> List[ConceptRef | Function | str | int | float | date | datetime]:
    final: List[ConceptRef | Function | str | int | float | date | datetime] = []
    for arg in args:
        final.append(process_function_arg(arg, meta, environment))
    return final


# Function operators whose result is never NULL, regardless of how nullable
# their arguments are (count of an empty group is 0, grouping flags are 0/1,
# the clock functions and random() always return a value).
NEVER_NULL_FUNCTIONS: frozenset[FunctionType] = frozenset(
    {
        FunctionType.COUNT,
        FunctionType.COUNT_DISTINCT,
        FunctionType.IS_NULL,
        FunctionType.GROUPING,
        FunctionType.GROUPING_ID,
        FunctionType.CURRENT_DATE,
        FunctionType.CURRENT_DATETIME,
        FunctionType.CURRENT_TIMESTAMP,
        FunctionType.RANDOM,
    }
)


def _value_is_null_literal(value: Any) -> bool:
    return value is None or value is MagicConstants.NULL


# Sentinel: a comparison operand that is not a usable scalar literal.
_NO_LITERAL = object()


def _condition_literal(value: Any) -> Any:
    """The scalar literal carried by a comparison operand, or ``_NO_LITERAL``."""
    while isinstance(value, Parenthetical):
        value = value.content
    if isinstance(value, Function) and value.operator == FunctionType.CONSTANT:
        if len(value.arguments) != 1:
            return _NO_LITERAL
        value = value.arguments[0]
    if isinstance(value, (bool, int, float, str, date, datetime)):
        return value
    return _NO_LITERAL


def _resolve_concept(ref: Any, environment: Environment) -> Concept | None:
    if isinstance(ref, ConceptRef):
        resolved: Concept | None = environment.concepts.get(ref.address)
    elif isinstance(ref, Concept):
        resolved = ref
    else:
        return None
    if resolved is None or isinstance(resolved, UndefinedConcept):
        return None
    return resolved


def _case_when_atoms(
    operator: FunctionType, arguments: list[Any], environment: Environment
) -> dict[str, tuple[Any, list[tuple[ComparisonOperator, Any]]]] | None:
    """Per-concept ``(datatype, atoms)`` for a CASE's WHEN tests.

    Returns None when any test isn't a simple ``concept <op> literal`` against a
    *non-nullable* concept — a nullable concept can itself be NULL (a value no
    WHEN represents), so coverage can never be proven in that case.
    """
    pairs: list[tuple[Any, ComparisonOperator, Any]] = []
    if operator == FunctionType.SIMPLE_CASE:
        if not arguments:
            return None
        switch = arguments[0]
        for branch in arguments[1:]:
            if not isinstance(branch, CaseSimpleWhen):
                return None
            pairs.append((switch, ComparisonOperator.EQ, branch.value_expr))
    else:
        for branch in arguments:
            if not isinstance(branch, CaseWhen):
                return None
            cond = branch.comparison
            if not isinstance(cond, Comparison) or isinstance(
                cond, SubselectComparison
            ):
                return None
            left_is_concept = isinstance(cond.left, (Concept, ConceptRef))
            right_is_concept = isinstance(cond.right, (Concept, ConceptRef))
            if left_is_concept and not right_is_concept:
                pairs.append((cond.left, cond.operator, cond.right))
            elif right_is_concept and not left_is_concept:
                pairs.append((cond.right, cond.operator, cond.left))
            else:
                return None

    grouped: dict[str, tuple[Any, list[tuple[ComparisonOperator, Any]]]] = {}
    for ref, op, raw in pairs:
        concept = _resolve_concept(ref, environment)
        if concept is None or Modifier.NULLABLE in concept.modifiers:
            return None
        value = _condition_literal(raw)
        if value is _NO_LITERAL:
            return None
        grouped.setdefault(concept.address, (concept.datatype, []))[1].append(
            (op, value)
        )
    return grouped


def _case_is_exhaustive(
    operator: FunctionType, arguments: list[Any], environment: Environment
) -> bool:
    """True when a CASE's WHEN conditions provably cover the whole input
    domain, so no row falls through to the (absent) ELSE.

    Reuses the domain-coverage proof from datasource injection
    (``conditions_cover_domain``)."""
    from trilogy.core.processing.condition_utility import conditions_cover_domain

    atoms = _case_when_atoms(operator, arguments, environment)
    return atoms is not None and conditions_cover_domain(atoms)


def _function_is_nullable(fn: Function, environment: Environment) -> bool:
    op = fn.operator
    arguments = list(fn.arguments)
    if op in (FunctionType.CASE, FunctionType.SIMPLE_CASE):
        branches = [
            a for a in arguments if isinstance(a, (CaseWhen, CaseSimpleWhen, CaseElse))
        ]
        # A nullable branch *value* makes the whole expression nullable.
        if any(_expr_is_nullable(b.expr, environment) for b in branches):
            return True
        if any(isinstance(a, CaseElse) for a in branches):
            return False
        # No ELSE: unmatched rows fall through to NULL — unless the WHEN
        # conditions provably cover the domain (then no row is unmatched).
        return not _case_is_exhaustive(op, arguments, environment)
    if op == FunctionType.COALESCE:
        # NULL only when every fallback is itself nullable.
        return all(_expr_is_nullable(a, environment) for a in arguments)
    if op == FunctionType.NULLIF:
        # nullif(a, b) is NULL whenever a == b.
        return True
    if op in (FunctionType.CONSTANT, FunctionType.TYPED_CONSTANT):
        return any(_value_is_null_literal(a) for a in arguments)
    if op in NEVER_NULL_FUNCTIONS:
        return False
    if op == FunctionType.GROUP:
        # group(expr, *grain): nullability tracks the wrapped expression.
        return bool(arguments) and _expr_is_nullable(arguments[0], environment)
    # Scalar functions and remaining aggregates propagate NULL from any input.
    return any(_expr_is_nullable(a, environment) for a in arguments)


def _expr_is_nullable(expr: Any, environment: Environment) -> bool:
    """True when ``expr`` can evaluate to NULL.

    Walks an authored expression tree. Concept leaves stop the recursion —
    a referenced concept's own ``modifiers`` already encode (transitively)
    whether it is nullable, so we never descend into its lineage.
    """
    if _value_is_null_literal(expr):
        return True
    if isinstance(
        expr,
        (int, float, str, bool, date, datetime, ListWrapper, TupleWrapper, MapWrapper),
    ):
        return False
    if isinstance(expr, (ConceptRef, Concept)):
        concept: Concept | None
        if isinstance(expr, ConceptRef):
            concept = environment.concepts.get(expr.address)
        else:
            concept = expr
        if concept is None or isinstance(concept, UndefinedConcept):
            return False
        return Modifier.NULLABLE in concept.modifiers
    if isinstance(expr, Parenthetical):
        return _expr_is_nullable(expr.content, environment)
    if isinstance(expr, FunctionCallWrapper):
        return _expr_is_nullable(expr.content, environment)
    if isinstance(expr, AggregateWrapper):
        return _expr_is_nullable(expr.function, environment)
    if isinstance(expr, FilterItem):
        # A filtered value is NULL exactly when its source value is.
        return _expr_is_nullable(expr.content, environment)
    if isinstance(expr, NumberingWindowItem):
        # row_number / rank / dense_rank always produce a value.
        return False
    if isinstance(expr, NavigationWindowItem):
        if expr.type in (WindowType.COUNT, WindowType.COUNT_DISTINCT):
            return False
        # lag / lead yield NULL past the partition edge.
        if expr.type in (WindowType.LAG, WindowType.LEAD):
            return True
        # sum / min / max / avg over a window follow their input.
        return _expr_is_nullable(expr.content, environment)
    if isinstance(expr, (CaseWhen, CaseSimpleWhen, CaseElse)):
        return _expr_is_nullable(expr.expr, environment)
    if isinstance(expr, Conditional):
        return _expr_is_nullable(expr.left, environment) or _expr_is_nullable(
            expr.right, environment
        )
    if isinstance(expr, Comparison):
        # IS [NOT] NULL always yields a non-null boolean.
        if expr.operator in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
            return False
        return _expr_is_nullable(expr.left, environment) or _expr_is_nullable(
            expr.right, environment
        )
    if isinstance(expr, Function):
        return _function_is_nullable(expr, environment)
    # Catch-all: any other concept-bearing node (Between, SubselectItem, ...)
    # is nullable if any concept it references is.
    if isinstance(expr, ConceptArgs):
        return any(
            _expr_is_nullable(arg, environment) for arg in expr.concept_arguments
        )
    return False


def is_nullable_lineage(lineage: Any, environment: Environment) -> bool:
    """Whether a derived concept with this lineage can produce NULL values."""
    return _expr_is_nullable(lineage, environment)


def get_lineage_modifiers(lineage: Any, environment: Environment) -> list[Modifier]:
    """Modifiers a derived concept inherits from its ``lineage``.

    Currently only nullability: a derivation is nullable when its expression
    can yield NULL — a CASE with no ELSE, ``nullif``, an arithmetic op over a
    nullable input, etc. See ``is_nullable_lineage``.
    """
    if is_nullable_lineage(lineage, environment):
        return [Modifier.NULLABLE]
    return []


def get_purpose_and_keys(
    purpose: Purpose | None,
    args: Tuple[ConceptRef | Concept, ...] | None,
    environment: Environment,
) -> Tuple[Purpose, set[str] | None]:
    local_purpose = purpose or function_args_to_output_purpose(args, environment)
    if local_purpose in (Purpose.PROPERTY, Purpose.METRIC) and args:
        keys = concept_list_to_keys(args, environment)
    else:
        keys = None
    return local_purpose, keys


def concept_list_to_keys(
    concepts: Tuple[Concept | ConceptRef, ...], environment: Environment
) -> set[str]:
    final_keys: List[str] = []
    for concept in concepts:

        if isinstance(concept, ConceptRef):
            concept = environment.concepts[concept.address]
        if isinstance(concept, UndefinedConcept):
            continue
        if concept.keys:
            final_keys += list(concept.keys)
        elif concept.purpose != Purpose.PROPERTY:
            final_keys.append(concept.address)
    return set(final_keys)


def constant_to_concept(
    parent: (
        ListWrapper | TupleWrapper | MapWrapper | int | float | str | date | datetime
    ),
    name: str,
    namespace: str,
    metadata: Metadata | None = None,
) -> Concept:
    const_function: Function = Function(
        operator=FunctionType.CONSTANT,
        output_datatype=arg_to_datatype(parent),
        output_purpose=Purpose.CONSTANT,
        arguments=[parent],
    )
    # assert const_function.arguments[0] == parent, f'{const_function.arguments[0]} != {parent}, {type(const_function.arguments[0])} != {type(parent)}'
    fmetadata = metadata or Metadata()
    return Concept(
        name=name,
        datatype=const_function.output_datatype,
        purpose=Purpose.CONSTANT,
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
        lineage=const_function,
        grain=Grain(),
        namespace=namespace,
        metadata=fmetadata,
    )


def atom_is_relevant(
    atom,
    others: list[Concept | ConceptRef],
    environment: Environment | None = None,
):

    if isinstance(atom, (ConceptRef, Concept)):
        # when we are looking at atoms, if there is a concept that is in others
        # return directly
        if atom.address in others:
            return False
        return concept_is_relevant(atom, others, environment)

    if isinstance(atom, AggregateWrapper) and not atom.by:
        return False
    elif isinstance(atom, AggregateWrapper):
        return any(atom_is_relevant(x, others, environment) for x in atom.by)

    elif isinstance(atom, Function):
        relevant = False
        for arg in atom.arguments:

            relevant = relevant or atom_is_relevant(arg, others, environment)
        return relevant
    elif isinstance(atom, FunctionCallWrapper):
        return any(
            [atom_is_relevant(atom.content, others, environment)]
            + [atom_is_relevant(x, others, environment) for x in atom.args]
        )
    elif isinstance(atom, CaseWhen):
        rval = atom_is_relevant(atom.expr, others, environment) or atom_is_relevant(
            atom.comparison, others, environment
        )
        return rval
    elif isinstance(atom, CaseElse):

        rval = atom_is_relevant(atom.expr, others, environment)
        return rval
    elif isinstance(atom, SubselectComparison):
        return atom_is_relevant(atom.left, others, environment)
    elif isinstance(atom, Comparison):
        return atom_is_relevant(atom.left, others, environment) or atom_is_relevant(
            atom.right, others, environment
        )
    elif isinstance(atom, Conditional):
        return atom_is_relevant(atom.left, others, environment) or atom_is_relevant(
            atom.right, others, environment
        )
    elif isinstance(atom, Parenthetical):
        return atom_is_relevant(atom.content, others, environment)
    elif isinstance(atom, ConceptArgs):
        # use atom is relevant here to trigger the early exit behavior for concepts in set
        return any(
            [atom_is_relevant(x, others, environment) for x in atom.concept_arguments]
        )

    return False


def concept_is_relevant(
    concept: Concept | ConceptRef,
    others: list[Concept | ConceptRef],
    environment: Environment | None = None,
) -> bool:
    if isinstance(concept, UndefinedConcept):
        return False
    if concept.datatype == DataType.UNKNOWN:
        return False

    if isinstance(concept, ConceptRef):
        if environment:
            concept = environment.concepts[concept.address]
        else:
            raise SyntaxError(
                "Require environment to determine relevance of ConceptRef"
            )
    other_addresses = {
        address
        for other in others
        for address in (
            environment.concepts[other.address].equivalent_addresses
            if isinstance(other, ConceptRef) and environment
            else (
                other.equivalent_addresses
                if isinstance(other, Concept)
                else {other.address}
            )
        )
    }
    if concept.derivation == Derivation.CONSTANT:
        return False
    if concept.is_aggregate and not (
        isinstance(concept.lineage, AggregateWrapper) and concept.lineage.by
    ):

        return False
    if concept.purpose in (Purpose.PROPERTY, Purpose.METRIC) and concept.keys:
        if all([c in other_addresses for c in concept.keys]):
            return False
    if (
        concept.purpose == Purpose.KEY
        and concept.keys
        and all([c in other_addresses for c in concept.keys])
    ):
        return False
    if concept.purpose in (Purpose.METRIC,):
        if all([c in other_addresses for c in concept.grain.components]):
            return False
    if (
        concept.derivation in (Derivation.BASIC,)
        and isinstance(concept.lineage, Function)
        and concept.lineage.operator == FunctionType.DATE_SPINE
    ):
        return True
    if concept.derivation in (Derivation.BASIC,) and isinstance(
        concept.lineage, (Function, CaseWhen)
    ):
        relevant = False
        for arg in concept.lineage.arguments:
            relevant = atom_is_relevant(arg, others, environment) or relevant
        return relevant
    if concept.derivation in (Derivation.BASIC,) and isinstance(
        concept.lineage, Parenthetical
    ):
        return atom_is_relevant(concept.lineage.content, others, environment)

    if concept.granularity == Granularity.SINGLE_ROW:
        return False
    return True


def concepts_to_grain_concepts_ordered(
    concepts: Iterable[Concept | ConceptRef | str],
    environment: Environment | None,
    local_concepts: Mapping[str, Concept] | None = None,
) -> list[str]:
    raw: list[Concept] = []
    for c in concepts:
        if isinstance(c, Concept):
            raw.append(c)

        elif isinstance(c, ConceptRef) and environment:
            if local_concepts and c.address in local_concepts:
                raw.append(local_concepts[c.address])
            else:
                raw.append(environment.concepts[c.address])
        elif isinstance(c, str) and environment:
            if local_concepts and c in local_concepts:
                raw.append(local_concepts[c])
            else:
                raw.append(environment.concepts[c])
        else:
            raise ValueError(
                f"Unable to resolve input {c} without environment provided to concepts_to_grain call"
            )

    def _lookup(addr: str) -> Concept | None:
        if local_concepts and addr in local_concepts:
            return local_concepts[addr]  # type: ignore
        if environment:
            return environment.concepts.get(addr)
        return None

    preconcepts: list[Concept] = []
    for x in raw:
        if (
            x.lineage
            and isinstance(x.lineage, Function)
            and x.lineage.operator == FunctionType.ALIAS
            and environment
        ):
            # alias is a renamed view of the source — use the source for grain
            source_addr = x.lineage.arguments[0].address  # type: ignore
            source = _lookup(source_addr)
            preconcepts.append(source if source is not None else x)
        elif x.derivation == Derivation.WINDOW and x.keys and environment:
            # A window output (rank/row_number/…) is row-distinct, so it would
            # otherwise enter the grain as itself. But its value is determined by
            # its keys (partition + anchor), and its order_by may re-derive an
            # aggregate that would then be grouped by this very output — a build
            # recursion. Contribute the window's keys to the grain instead.
            for kaddr in x.keys:
                key_concept = _lookup(kaddr)
                if key_concept is not None:
                    preconcepts.append(key_concept)
        else:
            preconcepts.append(x)
    seen: set[str] = set()
    output: list[str] = []
    for sub in preconcepts:
        if seen & sub.equivalent_addresses:
            continue
        if not concept_is_relevant(sub, preconcepts, environment):  # type: ignore

            continue
        seen.add(sub.address)
        output.append(sub.address)

    return output


def concepts_to_grain_concepts(
    concepts: Iterable[Concept | ConceptRef | str],
    environment: Environment | None,
    local_concepts: Mapping[str, Concept] | None = None,
) -> set[str]:
    return set(
        concepts_to_grain_concepts_ordered(
            concepts, environment=environment, local_concepts=local_concepts
        )
    )


def _get_relevant_parent_concepts(arg) -> tuple[list[ConceptRef], bool]:
    is_metric = False
    if isinstance(arg, Function):
        all = []
        for y in arg.arguments:
            refs, local_flag = get_relevant_parent_concepts(y)
            all += refs
            is_metric = is_metric or local_flag
        return all, is_metric
    elif isinstance(arg, AggregateWrapper) and not arg.by:
        return [], True
    elif isinstance(arg, AggregateWrapper) and arg.by:
        return [x.reference for x in arg.by], True
    elif isinstance(arg, FunctionCallWrapper):
        return get_relevant_parent_concepts(arg.content)
    elif isinstance(arg, (Comparison, Conditional)):
        left, lflag = get_relevant_parent_concepts(arg.left)
        right, rflag = get_relevant_parent_concepts(arg.right)
        return left + right, lflag or rflag
    elif isinstance(arg, Between):
        all = []
        flag = False
        for y in (arg.left, arg.low, arg.high):
            refs, local_flag = get_relevant_parent_concepts(y)
            all += refs
            flag = flag or local_flag
        return all, flag
    return get_concept_arguments(arg), False


def get_relevant_parent_concepts(arg) -> tuple[list[ConceptRef], bool]:
    concepts, status = _get_relevant_parent_concepts(arg)
    return unique(concepts, "address"), status


def group_function_to_concept(
    parent: Function,
    name: str,
    environment: Environment,
    namespace: str | None = None,
    metadata: Metadata | None = None,
):
    pkeys: List[Concept] = []
    namespace = namespace or environment.namespace
    is_metric = False
    ref_args, is_metric = get_relevant_parent_concepts(parent)
    concrete_args = [environment.concepts[c.address] for c in ref_args]
    pkeys += [x for x in concrete_args if not x.derivation == Derivation.CONSTANT]
    modifiers = get_lineage_modifiers(parent, environment)
    key_grain: list[str] = []
    for x in pkeys:
        # for a group to, if we have a dynamic metric, ignore it
        # it will end up with the group target grain
        if x.purpose == Purpose.METRIC and not x.keys:
            continue
        # metrics will group to keys, so do no do key traversal
        elif is_metric:
            key_grain.append(x.address)
        else:
            key_grain.append(x.address)
    keys = set(key_grain)

    grain = Grain.from_concepts(keys, environment)
    if is_metric:
        purpose = Purpose.METRIC
    elif not pkeys:
        purpose = Purpose.CONSTANT
    else:
        purpose = parent.output_purpose
    fmetadata = metadata or Metadata()
    granularity = Granularity.MULTI_ROW

    if grain is not None:
        # deduplicte
        grain = Grain.from_concepts(grain.components, environment)

        r = Concept(
            name=name,
            datatype=parent.output_datatype,
            purpose=purpose,
            lineage=parent,
            namespace=namespace,
            keys=keys,
            modifiers=modifiers,
            grain=grain,
            metadata=fmetadata,
            derivation=Derivation.GROUP_TO,
            granularity=granularity,
        )
        return r

    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        metadata=fmetadata,
        derivation=Derivation.BASIC,
        granularity=granularity,
    )


def function_to_concept(
    parent: Function,
    name: str,
    environment: Environment,
    namespace: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:

    pkeys: List[Concept] = []
    namespace = namespace or environment.namespace
    is_metric = False
    ref_args, is_metric = get_relevant_parent_concepts(parent)
    concrete_args = [environment.concepts[c.address] for c in ref_args]
    pkeys += [
        x
        for x in concrete_args
        if not x.derivation == Derivation.CONSTANT
        and not (x.derivation == Derivation.AGGREGATE and not x.grain.components)
    ]
    grain: Grain | None = Grain()
    for x in pkeys:
        grain += x.grain
    if parent.operator in FunctionClass.ONE_TO_MANY.value:
        # if the function will create more rows, we don't know what grain this is at
        grain = None
    modifiers = get_lineage_modifiers(parent, environment)
    key_grain: list[str] = []
    for x in pkeys:
        # metrics will group to keys, so do not do key traversal
        if is_metric:
            key_grain.append(x.address)
        # otherwse, for row ops, assume keys are transitive
        elif x.keys:
            key_grain += [*x.keys]
        else:
            key_grain.append(x.address)
    keys = Grain.from_concepts(set(key_grain), environment).components
    # A metric's grain is the grouping grain it's aggregated to (its keys),
    # not the union of those keys' own grains — summing x.grain descends a
    # property grouping key like week_seq down to its key (date.id), finer
    # than the metric actually lives at. keys already holds the grouping
    # addresses.
    if is_metric and grain is not None:
        grain = Grain.from_concepts(keys, environment)
    if is_metric:
        purpose = Purpose.METRIC
    elif not pkeys:
        purpose = Purpose.CONSTANT
    else:
        purpose = parent.output_purpose
    fmetadata = metadata or Metadata()
    if parent.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
        derivation = Derivation.AGGREGATE
        if (
            grain
            and grain.components
            and all(x.endswith(ALL_ROWS_CONCEPT) for x in grain.components)
        ):
            granularity = Granularity.SINGLE_ROW
        else:
            granularity = Granularity.MULTI_ROW
    elif parent.operator == FunctionType.UNION:
        derivation = Derivation.UNION
        granularity = Granularity.MULTI_ROW
    elif parent.operator in FunctionClass.ONE_TO_MANY.value:
        derivation = Derivation.UNNEST
        granularity = Granularity.MULTI_ROW
    elif parent.operator == FunctionType.RECURSE_EDGE:
        derivation = Derivation.RECURSIVE
        granularity = Granularity.MULTI_ROW
    elif parent.operator in FunctionClass.SINGLE_ROW.value:
        derivation = Derivation.CONSTANT
        granularity = Granularity.SINGLE_ROW
    elif concrete_args and all(
        x.derivation == Derivation.CONSTANT for x in concrete_args
    ):
        derivation = Derivation.CONSTANT
        granularity = Granularity.SINGLE_ROW
    else:
        derivation = Derivation.BASIC
        granularity = Granularity.MULTI_ROW
    if grain is not None:
        r = Concept(
            name=name,
            datatype=parent.output_datatype,
            purpose=purpose,
            lineage=parent,
            namespace=namespace,
            keys=keys,
            modifiers=modifiers,
            grain=grain,
            metadata=fmetadata,
            derivation=derivation,
            granularity=granularity,
        )
        return r

    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        metadata=fmetadata,
        derivation=derivation,
        granularity=granularity,
    )


def filter_item_to_concept(
    parent: FilterItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    fallback_keys = set()
    if isinstance(parent.content, ConceptRef):
        cparent = environment.concepts[parent.content.address]
        fallback_keys = set([cparent.address])
    elif isinstance(
        parent.content,
        (
            FilterItem,
            AggregateWrapper,
            FunctionCallWrapper,
            WindowItem,
            Function,
            ListWrapper,
            MapWrapper,
            int,
            str,
            float,
        ),
    ):
        cparent = arbitrary_to_concept(parent.content, environment, namespace=namespace)

    else:
        raise NotImplementedError(
            f"Filter item with non ref content {parent.content} ({type(parent.content)}) not yet supported"
        )
    modifiers = get_lineage_modifiers(parent, environment)
    grain = cparent.grain if cparent.purpose == Purpose.PROPERTY else Grain()
    granularity = cparent.granularity
    return Concept(
        name=name,
        datatype=cparent.datatype,
        purpose=Purpose.PROPERTY,
        lineage=parent,
        metadata=fmetadata,
        namespace=namespace,
        # filtered copies cannot inherit keys
        keys=(cparent.keys if cparent.purpose == Purpose.PROPERTY else fallback_keys),
        grain=grain,
        modifiers=modifiers,
        derivation=Derivation.FILTER,
        granularity=granularity,
    )


def _window_over_refs(over: list) -> list[ConceptRef | Concept]:
    """Flatten a window's `over` list to concept references for grain
    purposes. Expression items (e.g. `partition by upper(country)`) are
    replaced with their dependent concept refs since they carry no address
    until they're materialized at build time."""
    refs: list[ConceptRef | Concept] = []
    for item in over:
        if isinstance(item, (ConceptRef, Concept)):
            refs.append(item)
        else:
            refs.extend(get_concept_arguments(item))
    return refs


def _numbering_window_to_concept(
    parent: NumberingWindowItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata,
) -> Concept:
    anchor: Concept | None = None
    if parent.arguments:
        anchor = environment.concepts[parent.arguments[0].address]
        if isinstance(anchor, UndefinedConcept):
            return UndefinedConcept(address=f"{namespace}.{name}", metadata=metadata)
    arg_addresses = [a.address for a in parent.arguments]
    over_refs = _window_over_refs(list(parent.over))
    over_addresses = [y.address for y in over_refs]
    if arg_addresses:
        keys = (
            Grain.from_concepts(arg_addresses + over_addresses, environment).components
            or set()
        )
        keys = set(keys) | set(arg_addresses)
    else:
        keys = {f"{namespace}.{name}"}

    grain_components: list = list(over_refs) + list(parent.arguments)
    if parent.order_by:
        for item in parent.order_by:
            relevant, _ = get_relevant_parent_concepts(item.expr)
            grain_components += relevant
    final_grain = (
        Grain.from_concepts(grain_components, environment) if arg_addresses else Grain()
    )

    modifiers = get_lineage_modifiers(parent, environment)
    if parent.type in (WindowType.RANK, WindowType.DENSE_RANK):
        datatype: TraitDataType | DataType = TraitDataType(
            type=DataType.INTEGER, traits=["rank"]
        )
    else:
        datatype = DataType.INTEGER
    return Concept(
        name=name,
        datatype=datatype,
        purpose=Purpose.PROPERTY,
        lineage=parent,
        metadata=metadata,
        grain=final_grain,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        derivation=Derivation.WINDOW,
        granularity=anchor.granularity if anchor else Granularity.MULTI_ROW,
    )


def _navigation_window_to_concept(
    parent: NavigationWindowItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata,
) -> Concept:
    if isinstance(
        parent.content,
        (
            FilterItem,
            AggregateWrapper,
            FunctionCallWrapper,
            WindowItem,
            Function,
            ListWrapper,
            MapWrapper,
            int,
            str,
            float,
        ),
    ):
        bcontent = arbitrary_to_concept(
            parent.content, environment, namespace=namespace
        )
    elif isinstance(parent.content, ConceptRef):
        bcontent = environment.concepts[parent.content.address]
    else:
        raise NotImplementedError(
            f"Navigation window function with content type {type(parent.content)} not yet supported"
        )
    if isinstance(bcontent, UndefinedConcept):
        return UndefinedConcept(address=f"{namespace}.{name}", metadata=metadata)
    over_refs = _window_over_refs(list(parent.over))
    if bcontent.purpose == Purpose.METRIC:
        local_purpose, keys = get_purpose_and_keys(None, (bcontent,), environment)
    else:
        local_purpose = Purpose.PROPERTY
        keys = Grain.from_concepts(
            [bcontent.address] + [y.address for y in over_refs], environment
        ).components

    # A navigation window (lead/lag) emits one value per input row, so its
    # grain is the operand's grain — not the operand itself. Embedding the
    # operand (e.g. an aggregate metric) would give every window over the
    # same rowset a distinct grain (operand differs per window) and block
    # them from sharing a scan. Recurse to the operand's actual grain,
    # falling back to the operand when it has none (a bare key/property).
    if bcontent.grain and bcontent.grain.components:
        operand_grain: list = list(bcontent.grain.components)
    else:
        operand_grain = [bcontent.output]
    grain_components: list = list(over_refs) + operand_grain
    if parent.order_by:
        for item in parent.order_by:
            relevant, _ = get_relevant_parent_concepts(item.expr)
            grain_components += relevant
    final_grain = Grain.from_concepts(grain_components, environment)

    modifiers = get_lineage_modifiers(parent, environment)
    if parent.type in (WindowType.COUNT, WindowType.COUNT_DISTINCT):
        datatype: Any = DataType.INTEGER
    else:
        datatype = bcontent.datatype
    return Concept(
        name=name,
        datatype=datatype,
        purpose=local_purpose,
        lineage=parent,
        metadata=metadata,
        grain=final_grain,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        derivation=Derivation.WINDOW,
        granularity=bcontent.granularity,
    )


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    if isinstance(parent, NumberingWindowItem):
        return _numbering_window_to_concept(
            parent, name, namespace, environment, fmetadata
        )
    if isinstance(parent, NavigationWindowItem):
        return _navigation_window_to_concept(
            parent, name, namespace, environment, fmetadata
        )
    raise NotImplementedError(f"Unknown window item type {type(parent)}")


def agg_wrapper_to_concept(
    parent: AggregateWrapper,
    namespace: str,
    name: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    # `by` items must be concept refs by the time we get here — the
    # ``aggregate_paren_by`` rule materializes expressions to virtual concepts
    # so that grain/keys can be computed against a stable address. Window
    # ``partition by`` keeps raw expressions because windows don't redefine
    # grain; aggregates do.
    by_refs: list[ConceptRef | Concept] = list(parent.by) if parent.by else []
    _, keys = get_purpose_and_keys(
        Purpose.METRIC, tuple(by_refs) if by_refs else None, environment=environment
    )
    # anything grouped to a grain should be a property
    # at that grain
    fmetadata = metadata or Metadata()
    aggfunction = parent.function
    modifiers = get_lineage_modifiers(parent, environment)
    grain = Grain.from_concepts(by_refs, environment) if by_refs else Grain()
    granularity = Concept.calculate_granularity(Derivation.AGGREGATE, grain, parent)

    out = Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        metadata=fmetadata,
        lineage=parent,
        grain=grain,
        namespace=namespace,
        keys=set([x.address for x in by_refs]) if by_refs else keys,
        modifiers=modifiers,
        derivation=Derivation.AGGREGATE,
        granularity=granularity,
    )
    for x in parent.function.concept_arguments:
        if x.address == out.address:
            raise InvalidSyntaxException(
                f"Aggregate concept {out.address} cannot reference itself. If defining a new concept in a select, use a new name."
            )
    return out


def align_item_to_concept(
    parent: AlignItem,
    align_clause: AlignClause,
    selects: list[SelectStatement],
    environment: Environment,
    where: WhereClause | None = None,
    having: HavingClause | None = None,
    limit: int | None = None,
) -> Concept:
    align = parent
    # Align tolerates any datatypes the engine can coerce together: traits are
    # pure annotations (``numeric(15,2)::usd`` aligns with bare ``numeric(15,2)``)
    # and the whole integer/bigint/float/numeric family is mutually compatible
    # (so ``date_dim.moy`` bigint aligns with ``month(date)`` int::month). Use the
    # structural compatibility check rather than exact-inner-type grouping.
    raw_datatypes = [c.datatype for c in align.concepts]
    if any(
        not is_compatible_datatype(a, b)
        for i, a in enumerate(raw_datatypes)
        for b in raw_datatypes[i + 1 :]
    ):
        raise InvalidSyntaxException(
            f"Datatypes do not align for merged statements {align.alias}, have {set(raw_datatypes)}"
        )
    # Prefer a trait-bearing representative so the merged concept keeps the richer
    # type information; otherwise widen across the compatible set.
    merged_datatype = next(
        (dt for dt in raw_datatypes if isinstance(dt, TraitDataType)),
        merge_datatypes(raw_datatypes),
    )

    new_selects = [x.as_lineage(environment) for x in selects]
    multi_lineage = MultiSelectLineage(
        selects=new_selects,
        align=align_clause,
        namespace=align.namespace,
        where_clause=where,
        having_clause=having,
        limit=limit,
        hidden_components=set(y for x in new_selects for y in x.hidden_components),
    )
    grain = Grain()
    new = Concept(
        name=align.alias,
        datatype=merged_datatype,
        purpose=Purpose.PROPERTY,
        lineage=multi_lineage,
        grain=grain,
        namespace=align.namespace,
        granularity=Granularity.MULTI_ROW,
        derivation=Derivation.MULTISELECT,
        keys=set(x.address for x in align.concepts),
    )
    return new


def union_item_to_concept(
    parent: AlignItem,
    align_clause: AlignClause,
    selects: list[SelectStatement],
    environment: Environment,
    purpose: Purpose | None = None,
    datatype: DataType | TraitDataType | None = None,
    nullable: bool = False,
) -> Concept:
    """Build one positional output concept of a relational ``union(...)`` TVF.

    Mirrors :func:`align_item_to_concept` (the arm/positional binding shape is
    shared) but the concept's lineage is a ``UnionSelectLineage`` and its
    derivation is ``TVF_UNION`` so it lowers to a column-stack UNION rather than
    a FULL-JOIN merge. ``purpose``/``datatype``/``nullable`` override the
    inferred values when the output signature is explicit.
    """
    align = parent
    raw_datatypes = [c.datatype for c in align.concepts]
    if any(
        not is_compatible_datatype(a, b)
        for i, a in enumerate(raw_datatypes)
        for b in raw_datatypes[i + 1 :]
    ):
        raise InvalidSyntaxException(
            f"Datatypes do not align for union output {align.alias}, "
            f"have {set(raw_datatypes)}"
        )
    inferred = next(
        (dt for dt in raw_datatypes if isinstance(dt, TraitDataType)),
        merge_datatypes(raw_datatypes),
    )
    if datatype is not None and not all(
        is_compatible_datatype(datatype, dt) for dt in raw_datatypes
    ):
        raise InvalidSyntaxException(
            f"Output column '{align.alias}' declared as {datatype} but arms "
            f"produce incompatible types {set(raw_datatypes)}"
        )
    merged_datatype = datatype if datatype is not None else inferred

    # The grain of stacking two selects (UNION ALL) is unknowable in general, so
    # every union output is its own grain component: a KEY, never a metric. (A
    # metric output would be dropped from downstream grain and break consumers
    # that re-aggregate the stacked result; an explicit grain clause could refine
    # this later.) An explicit signature purpose still wins.
    new_selects = [x.as_lineage(environment) for x in selects]
    union_lineage = UnionSelectLineage(
        selects=new_selects,
        align=align_clause,
        namespace=align.namespace,
        hidden_components=set(y for x in new_selects for y in x.hidden_components),
    )
    new = Concept(
        name=align.alias,
        datatype=merged_datatype,
        purpose=purpose or Purpose.KEY,
        lineage=union_lineage,
        grain=Grain(),
        namespace=align.namespace,
        granularity=Granularity.MULTI_ROW,
        derivation=Derivation.TVF_UNION,
        keys=None,
        modifiers=[Modifier.NULLABLE] if nullable else [],
    )
    return new


def derive_item_to_concept(
    parent: ARBITRARY_INPUTS,
    name: str,
    lineage: MultiSelectLineage,
    namespace: str | None = None,
) -> Concept:
    datatype = arg_to_datatype(parent)
    grain = Grain()
    new = Concept(
        name=name,
        datatype=datatype,
        purpose=Purpose.PROPERTY,
        lineage=lineage,
        grain=grain,
        namespace=namespace or DEFAULT_NAMESPACE,
        granularity=Granularity.MULTI_ROW,
        derivation=Derivation.MULTISELECT,
        # A derive output is computed at the merge grain, so its keys are the
        # aligned concepts. Without this it has no keys and gets treated as a
        # grain component itself, forcing a spurious top-level GROUP BY over the
        # derived metric columns.
        keys=set(item.aligned_concept for item in lineage.align.items),
    )
    return new


def generate_concept_name(
    parent: ARBITRARY_INPUTS,
) -> str:
    """Generate a name for a concept based on its parent type and content."""
    if isinstance(parent, AggregateWrapper):
        return f"{VIRTUAL_CONCEPT_PREFIX}_agg_{parent.function.operator.value}_{string_to_hash(str(parent))}"
    elif isinstance(parent, WindowItem):
        return f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(str(parent))}"
    elif isinstance(parent, FilterItem):
        if isinstance(parent.content, ConceptRef):
            return f"{VIRTUAL_CONCEPT_PREFIX}_filter_{parent.content.name}_{string_to_hash(str(parent))}"
        else:
            return f"{VIRTUAL_CONCEPT_PREFIX}_filter_{string_to_hash(str(parent))}"
    elif isinstance(parent, Function):
        if parent.operator == FunctionType.GROUP:
            return f"{VIRTUAL_CONCEPT_PREFIX}_group_to_{string_to_hash(str(parent))}"
        else:
            return f"{VIRTUAL_CONCEPT_PREFIX}_func_{parent.operator.value}_{string_to_hash(str(parent))}"
    elif isinstance(parent, Parenthetical):
        return f"{VIRTUAL_CONCEPT_PREFIX}_paren_{string_to_hash(str(parent))}"
    elif isinstance(parent, FunctionCallWrapper):
        return f"{VIRTUAL_CONCEPT_PREFIX}_{parent.name}_{string_to_hash(str(parent))}"
    elif isinstance(parent, SubselectItem):
        return f"{VIRTUAL_CONCEPT_PREFIX}_subselect_{parent.content.name}_{string_to_hash(str(parent))}"
    elif isinstance(parent, Comparison):
        return f"{VIRTUAL_CONCEPT_PREFIX}_comp_{string_to_hash(str(parent))}"
    else:  # ListWrapper, MapWrapper, or primitive types
        return f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"


def subselect_to_concept(
    parent: SubselectItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    namespace = namespace or environment.namespace
    concrete_args = [environment.concepts[c.address] for c in parent.concept_arguments]
    pkeys: list[Concept] = [
        x
        for x in concrete_args
        if x.derivation != Derivation.CONSTANT
        and not (x.derivation == Derivation.AGGREGATE and not x.grain.components)
    ]
    grain: Grain = Grain()
    for x in pkeys:
        grain += x.grain
    modifiers = get_lineage_modifiers(parent, environment)
    key_grain: list[str] = []
    for x in pkeys:
        if x.keys:
            key_grain += [*x.keys]
        else:
            key_grain.append(x.address)
    keys = Grain.from_concepts(set(key_grain), environment).components
    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=Purpose.PROPERTY,
        derivation=Derivation.SUBSELECT,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        grain=grain,
        metadata=fmetadata,
        granularity=Granularity.MULTI_ROW,
    )


def parenthetical_to_concept(
    parent: Parenthetical,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    if isinstance(parent.content, ConceptRef):
        # a parenthetical wrapping a bare concept reference is a no-op; treat it
        # exactly like the reference itself (aliased to `name` if one was given)
        return arbitrary_to_concept(
            unwrap_transformation(parent.content, environment),
            environment,
            namespace,
            name,
            metadata,
        )
    if isinstance(
        parent.content,
        ARBITRARY_INPUTS,
    ):

        return arbitrary_to_concept(
            parent.content, environment, namespace, name, metadata
        )
    raise NotImplementedError(
        f"Parenthetical with non-supported content {parent.content} ({type(parent.content)}) not yet supported"
    )


def comparison_to_concept(
    parent: Comparison | Conditional | Between,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
):
    fmetadata = metadata or Metadata()

    pkeys: List[Concept] = []
    namespace = namespace or environment.namespace
    is_metric = False
    ref_args, is_metric = get_relevant_parent_concepts(parent)
    concrete_args = [environment.concepts[c.address] for c in ref_args]
    pkeys += [
        x
        for x in concrete_args
        if not x.derivation == Derivation.CONSTANT
        and not (x.derivation == Derivation.AGGREGATE and not x.grain.components)
    ]
    grain: Grain | None = Grain()
    for x in pkeys:
        grain += x.grain
    # Conditional/Between have no comparison operator; only a Comparison can
    # carry a row-expanding one.
    if (
        isinstance(parent, Comparison)
        and parent.operator in FunctionClass.ONE_TO_MANY.value
    ):
        # if the function will create more rows, we don't know what grain this is at
        grain = None
    modifiers = get_lineage_modifiers(parent, environment)
    key_grain: list[str] = []
    for x in pkeys:
        # metrics will group to keys, so do not do key traversal
        if is_metric:
            key_grain.append(x.address)
        # otherwse, for row ops, assume keys are transitive
        elif x.keys:
            key_grain += [*x.keys]
        else:
            key_grain.append(x.address)
    keys = set(key_grain)
    if is_metric:
        purpose = Purpose.METRIC
    elif not pkeys:
        purpose = Purpose.CONSTANT
    else:
        purpose = Purpose.PROPERTY
    fmetadata = metadata or Metadata()

    if grain is not None:
        r = Concept(
            name=name,
            datatype=parent.output_datatype,
            purpose=purpose,
            lineage=parent,
            namespace=namespace,
            keys=keys,
            modifiers=modifiers,
            grain=grain,
            metadata=fmetadata,
            derivation=Derivation.BASIC,
            granularity=(
                Granularity.MULTI_ROW if concrete_args else Granularity.SINGLE_ROW
            ),
        )
        return r

    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        metadata=fmetadata,
        derivation=Derivation.BASIC,
        granularity=Granularity.MULTI_ROW,
    )


def arbitrary_to_concept(
    parent: ARBITRARY_INPUTS,
    environment: Environment,
    namespace: str | None = None,
    name: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    namespace = namespace or environment.namespace

    # Keep the wrapper as the concept lineage so the author-time syntax
    # `@fn(...)` round-trips through the renderer. The build phase strips it
    # via `_build_function_call_wrapper`, so semantics are unchanged. Author-
    # layer code that pattern-matches on lineage must look through the
    # wrapper (e.g. `unwrap_function_call_wrapper`).
    if isinstance(parent, FunctionCallWrapper):
        inner = arbitrary_to_concept(
            parent.content, environment, namespace, name or parent.name, metadata  # type: ignore
        )
        inner.lineage = FunctionCallWrapper(
            content=inner.lineage,  # type: ignore[arg-type]
            name=parent.name,
            args=parent.args,
        )
        return inner

    # Generate name if not provided
    if not name:
        name = generate_concept_name(parent)

    if isinstance(parent, AggregateWrapper):
        return agg_wrapper_to_concept(
            parent, namespace, name, metadata=metadata, environment=environment
        )
    elif isinstance(parent, WindowItem):
        return window_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            metadata=metadata,
        )
    elif isinstance(parent, FilterItem):
        return filter_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            metadata=metadata,
        )
    elif isinstance(parent, Function):
        if parent.operator == FunctionType.GROUP:
            return group_function_to_concept(
                parent,
                name,
                environment=environment,
                namespace=namespace,
                metadata=metadata,
            )
        return function_to_concept(
            parent,
            name,
            metadata=metadata,
            environment=environment,
            namespace=namespace,
        )
    elif isinstance(parent, SubselectItem):
        return subselect_to_concept(parent, name, namespace, environment, metadata)
    elif isinstance(parent, ListWrapper):
        return constant_to_concept(parent, name, namespace, metadata)
    elif isinstance(parent, Parenthetical):
        return parenthetical_to_concept(parent, name, namespace, environment, metadata)
    elif isinstance(parent, (Comparison, Conditional, Between)):
        return comparison_to_concept(parent, name, namespace, environment, metadata)
    else:
        return constant_to_concept(parent, name, namespace, metadata)
