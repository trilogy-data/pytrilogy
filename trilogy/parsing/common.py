from datetime import date, datetime
from typing import List, Tuple

from trilogy.constants import (
    VIRTUAL_CONCEPT_PREFIX,
)
from trilogy.core.enums import (
    FunctionType,
    Granularity,
    Modifier,
    Derivation,
    WindowType,
)
from trilogy.core.common_models import arg_to_datatype, args_to_output_purpose
from trilogy.core.execute_models import (
    DataType,
    BoundFunction,
    FunctionClass,
    ListWrapper,
    MapWrapper,
    Meta,
    Metadata,
    Purpose,
)
from trilogy.core.author_models import (
    ConceptRef,
    Grain,
    Function,
    AggregateWrapper,
    FilterItem,
    WindowItem,
    Parenthetical,
    Concept,
    Environment,
)
from trilogy.utility import string_to_hash, unique


def get_upstream_modifiers(
    keys: List[Concept], environment: Environment
) -> list[Modifier]:
    modifiers = set()
    for pkey in keys:
        if pkey.modifiers:
            modifiers.update(pkey.modifiers)
    return list(modifiers)


def process_function_args(
    args,
    meta: Meta | None,
    environment: Environment,
) -> List[ConceptRef | Function | str | int | float | date | datetime]:
    final: List[ConceptRef | Function | str | int | float | date | datetime] = []
    for arg in args:
        # if a function has an anonymous function argument
        # create an implicit concept
        if isinstance(arg, ConceptRef):
            final.append(environment.concepts[arg.address])
        elif isinstance(arg, Parenthetical):
            processed = process_function_args([arg.content], meta, environment)
            final.append(
                Function(
                    operator=FunctionType.PARENTHETICAL,
                    arguments=processed,
                    output_datatype=arg_to_datatype(processed[0]),
                    output_purpose=args_to_output_purpose(processed),
                )
            )
        elif isinstance(arg, Function):
            # if it's not an aggregate function, we can skip the virtual concepts
            # to simplify anonymous function handling
            if (
                arg.operator not in FunctionClass.AGGREGATE_FUNCTIONS.value
                and arg.operator != FunctionType.UNNEST
            ):
                final.append(arg)
                continue
            id_hash = string_to_hash(str(arg))
            concept = function_to_concept(
                arg,
                name=f"{VIRTUAL_CONCEPT_PREFIX}_{arg.operator.value}_{id_hash}",
                environment=environment,
            )
            # to satisfy mypy, concept will always have metadata
            if concept.metadata and meta:
                concept.metadata.line_number = meta.line
            environment.add_concept(concept, meta=meta)
            final.append(concept)
        elif isinstance(
            arg, (FilterItem, WindowItem, AggregateWrapper, ListWrapper, MapWrapper)
        ):
            id_hash = string_to_hash(str(arg))
            concept = arbitrary_to_concept(
                arg,
                name=f"{VIRTUAL_CONCEPT_PREFIX}_{id_hash}",
                environment=environment,
            )
            if concept.metadata and meta:
                concept.metadata.line_number = meta.line
            environment.add_concept(concept, meta=meta)
            final.append(concept)
        elif isinstance(arg, ConceptRef):
            final.append(arg)
        else:
            final.append(arg)
    return final


def get_purpose_and_keys(
    environment: Environment, purpose: Purpose | None, args: Tuple[Concept, ...] | None
) -> Tuple[Purpose, set[str] | None]:
    local_purpose = purpose or args_to_output_purpose(args)
    if local_purpose in (Purpose.PROPERTY, Purpose.METRIC) and args:
        keys = concept_list_to_keys(args)
    else:
        keys = None
    return local_purpose, keys


def concept_list_to_keys(concepts: Tuple[Concept, ...]) -> set[str]:
    final_keys: List[str] = []
    for concept in concepts:
        if concept.keys:
            final_keys += list(concept.keys)
        elif concept.purpose != Purpose.PROPERTY:
            final_keys.append(concept.address)
    return set(final_keys)


def constant_to_concept(
    parent: ListWrapper | MapWrapper | list | int | float | str,
    name: str,
    namespace: str,
    environment: Environment,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    const_function: Function = Function(
        operator=FunctionType.CONSTANT,
        output_datatype=arg_to_datatype(parent),
        output_purpose=Purpose.CONSTANT,
        arguments=[parent],
    )
    assert const_function.arguments[0] == parent, const_function.arguments[0]
    fmetadata = metadata or Metadata()
    return Concept(
        name=name,
        datatype=const_function.output_datatype,
        purpose=Purpose.CONSTANT,
        lineage=const_function,
        derivation=Derivation.CONSTANT,
        grain=Grain(),
        namespace=namespace,
        metadata=fmetadata,
        granularity=Granularity.SINGLE_ROW,
    )


def concepts_to_grain_concepts(
    concepts: List[Concept] | List[str] | set[str], environment: Environment
) -> list[Concept]:
    pconcepts: list[Concept] = []
    for x in concepts:
        if isinstance(x, ConceptRef):
            pconcepts.append(environment.concepts[x.address])
        elif isinstance(x, str):
            pconcepts.append(environment.concepts[x])
        else:
            pconcepts.append(x)

    final: List[Concept] = []
    for sub in pconcepts:
        if sub.purpose in (Purpose.PROPERTY, Purpose.METRIC) and sub.keys:
            if any([c in pconcepts for c in sub.keys]):
                continue
        if sub.purpose in (Purpose.METRIC,):
            if all([c in pconcepts for c in sub.grain.components]):
                continue
        if sub.granularity == Granularity.SINGLE_ROW:
            continue
        final.append(sub)
    final = unique(final, "address")
    v2 = sorted(final, key=lambda x: x.name)
    return v2


def function_to_concept(
    parent: Function | BoundFunction,
    name: str,
    environment: Environment,
    namespace: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    pkeys: List[Concept] = []
    namespace = namespace or environment.namespace
    concrete_concepts = [
        environment.concepts[x.address] for x in parent.concept_arguments
    ]
    pkeys += [
        x for x in concrete_concepts if not x.derivation == Derivation.CONSTANT
    ]
    grain: Grain | None = Grain()
    for x in pkeys:
        grain += x.grain
    if parent.operator in FunctionClass.ONE_TO_MANY.value:
        # if the function will create more rows, we don't know what grain this is at
        grain = None
    modifiers = get_upstream_modifiers(pkeys, environment=environment)
    key_grain: list[str] = []
    for x in pkeys:
        if x.keys:
            key_grain += [*x.keys]
        else:
            key_grain.append(x.address)
    keys = set(key_grain)
    if not pkeys:
        purpose = Purpose.CONSTANT
    else:
        purpose = parent.output_purpose
    fmetadata = metadata or Metadata()
    if parent.operator in FunctionClass.ONE_TO_MANY.value:
        granularity = Granularity.MULTI_ROW
    elif pkeys and all([x.granularity == Granularity.SINGLE_ROW for x in pkeys]):
        granularity = Granularity.SINGLE_ROW
    elif not pkeys:
        granularity = Granularity.SINGLE_ROW
    else:
        granularity = Granularity.MULTI_ROW

    if parent.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
        lineage = Derivation.AGGREGATE
    elif parent.operator == FunctionType.UNNEST:
        lineage = Derivation.UNNEST
    elif parent.operator == FunctionType.UNION:
        lineage = Derivation.UNION
    elif parent.operator in FunctionClass.SINGLE_ROW.value:
        lineage = Derivation.CONSTANT
    elif all([x.derivation == Derivation.CONSTANT for x in concrete_concepts]):
        lineage = Derivation.CONSTANT
    elif not concrete_concepts:
        lineage = Derivation.CONSTANT
    else:
        lineage = Derivation.BASIC
    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        derivation=lineage,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        grain=grain,
        metadata=fmetadata,
        granularity=granularity,
    )


def filter_item_to_concept(
    parent: FilterItem,
    name: str,
    namespace: str,
    environment: Environment,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    concrete_content = environment.concepts[parent.content.address]
    modifiers = get_upstream_modifiers(
        [concrete_content,], environment=environment
    )
    granularity = (
        Granularity.SINGLE_ROW
        if concrete_content.granularity == Granularity.SINGLE_ROW
        else Granularity.MULTI_ROW
    )
    return Concept(
        name=name,
        datatype=concrete_content.datatype,
        purpose=Purpose.PROPERTY,
        lineage=parent,
        derivation=Derivation.FILTER,
        metadata=fmetadata,
        namespace=namespace,
        # filtered copies cannot inherit keys
        keys=(
            concrete_content.keys
            if concrete_content.purpose == Purpose.PROPERTY
            else {
                concrete_content.address,
            }
        ),
        grain=(
            concrete_content.grain
            if concrete_content.purpose == Purpose.PROPERTY
            else Grain()
        ),
        modifiers=modifiers,
        granularity=granularity,
    )


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    environment: Environment,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    concrete_over = [environment.concepts[x.address] for x in parent.over]
    concrete_content = environment.concepts[parent.content.address]
    local_purpose, keys = get_purpose_and_keys(
        environment, purpose, (concrete_content,)
    )
    grain = concrete_over + [concrete_content]
    if parent.order_by:
        concrete_order_by = [
            environment.concepts[x.expr.address] for x in parent.order_by
        ]
        grain += concrete_order_by
    modifiers = get_upstream_modifiers([concrete_content], environment=environment)
    datatype = concrete_content.datatype
    if parent.type in (
        WindowType.RANK,
        WindowType.ROW_NUMBER,
        WindowType.COUNT,
        WindowType.COUNT_DISTINCT,
    ):
        datatype = DataType.INTEGER
    return Concept(
        name=name,
        datatype=datatype,
        purpose=local_purpose,
        lineage=parent,
        derivation=Derivation.WINDOW,
        metadata=fmetadata,
        # filters are implicitly at the grain of the base item
        grain=Grain.from_concepts(grain),
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
    )


def agg_wrapper_to_concept(
    parent: AggregateWrapper,
    namespace: str,
    name: str,
    metadata: Metadata | None = None,
    environment: Environment | None = None,
) -> Concept:
    concrete_by = [environment.concepts[x.address] for x in parent.by]
    # anything grouped to a grain should be a property
    # at that grain
    fmetadata = metadata or Metadata()
    aggfunction = parent.function
    concept_arguments = [
        environment.concepts[x.address] for x in parent.concept_arguments
    ]
    modifiers = get_upstream_modifiers(concept_arguments, environment=environment)
    granularity = (
        Granularity.SINGLE_ROW
        if all(
            x.granularity in (Granularity.SINGLE_ROW, Granularity.ALL_ROWS)
            for x in concrete_by
        )
        else Granularity.MULTI_ROW
    )
    grain = (
        Grain.from_concepts([x.address for x in concrete_by], environment)
        if concrete_by
        else Grain()
    )
    out = Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        derivation=Derivation.AGGREGATE,
        metadata=fmetadata,
        lineage=parent,
        grain=grain,
        namespace=namespace,
        keys=grain.components,
        modifiers=modifiers,
        granularity=granularity,
    )
    return out


def arbitrary_to_concept(
    parent: (
        AggregateWrapper
        | WindowItem
        | FilterItem
        | Function
        | ListWrapper
        | MapWrapper
        | int
        | float
        | str
    ),
    environment: Environment,
    namespace: str | None = None,
    name: str | None = None,
    metadata: Metadata | None = None,
    purpose: Purpose | None = None,
) -> Concept:
    namespace = namespace or environment.namespace
    if isinstance(parent, AggregateWrapper):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_agg_{parent.function.operator.value}_{string_to_hash(str(parent))}"
        return agg_wrapper_to_concept(
            parent, namespace, name, environment=environment, metadata=metadata
        )
    elif isinstance(parent, WindowItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(str(parent))}"
        return window_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            purpose=purpose,
            metadata=metadata,
        )
    elif isinstance(parent, FilterItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_filter_{parent.content.name}_{string_to_hash(str(parent))}"
        return filter_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            purpose=purpose,
            metadata=metadata,
        )
    elif isinstance(parent, Function):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_func_{parent.operator.value}_{string_to_hash(str(parent))}"
        return function_to_concept(
            parent,
            name,
            metadata=metadata,
            environment=environment,
            namespace=namespace,
        )
    elif isinstance(parent, ListWrapper):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"
        return constant_to_concept(parent, name, namespace, purpose, metadata)
    else:
        raise SyntaxError(f"Unknown type {type(parent)}")
