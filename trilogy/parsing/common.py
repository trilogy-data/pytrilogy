from datetime import date, datetime
from typing import List, Tuple

from trilogy.constants import (
    VIRTUAL_CONCEPT_PREFIX,
)
from trilogy.core.enums import (
    FunctionType,
    Granularity,
    Modifier,
    PurposeLineage,
    WindowType,
)
from trilogy.core.core_models import arg_to_datatype, args_to_output_purpose
from trilogy.core.execute_models import (
    BoundAggregateWrapper,
    BoundConcept,
    DataType,
    BoundEnvironment,
    BoundFilterItem,
    BoundFunction,
    FunctionClass,

    ListWrapper,
    MapWrapper,
    Meta,
    Metadata,
    BoundParenthetical,
    Purpose,
    BoundWindowItem,

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


def get_upstream_modifiers(keys: List[Concept], environment:Environment) -> list[Modifier]:
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
            print(arg.content)
            processed = process_function_args([arg.content], meta, environment) 
            print(processed)
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
                final.append(arg.instantiate(environment))
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
        environment: Environment,
    purpose: Purpose | None, args: Tuple[Concept, ...] | None
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
        grain=Grain(),
        namespace=namespace,
        metadata=fmetadata,
        granularity = Granularity.SINGLE_ROW
    )


def concepts_to_grain_concepts(
    concepts: List[Concept] | List[str] | set[str], environment: Environment | None
) -> list[Concept]:
    environment = Environment() if environment is None else environment
    pconcepts: list[Concept] = [
    ]
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
    concrete = parent.instantiate(environment) if isinstance(parent, Function) else parent
    for x in concrete.arguments:
        pkeys += [
            x
            for x in concrete.concept_arguments
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
    elif not pkeys and parent.operator in FunctionClass.SINGLE_ROW.value:
        granularity = Granularity.SINGLE_ROW
    else:
        granularity = Granularity.MULTI_ROW

    if grain is not None:
        return Concept(
            name=name,
            datatype=parent.output_datatype,
            purpose=purpose,
            lineage=parent,
            namespace=namespace,
            keys=keys,
            modifiers=modifiers,
            grain=grain,
            metadata=fmetadata,
            granularity=granularity,
        )

    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        lineage=parent,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        metadata=fmetadata,
    )


def filter_item_to_concept(
    parent: FilterItem,
    name: str,
    namespace: str,
    environment:BoundEnvironment,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    concrete = parent.instantiate(environment)
    modifiers = get_upstream_modifiers(concrete.content.concept_arguments, environment=environment)
    granularity = Granularity.SINGLE_ROW if concrete.content.grain == Granularity.SINGLE_ROW else Granularity.MULTI_ROW
    return Concept(
        name=name,
        datatype=concrete.content.datatype,
        purpose=Purpose.PROPERTY,
        lineage=parent,
        metadata=fmetadata,
        namespace=namespace,
        # filtered copies cannot inherit keys
        keys=(
            concrete.content.keys
            if concrete.content.purpose == Purpose.PROPERTY
            else {
                concrete.content.address,
            }
        ),
        grain=(
            concrete.content.grain
            if concrete.content.purpose == Purpose.PROPERTY
            else Grain()
        ),
        modifiers=modifiers,
        granularity=granularity,
    )


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    environment: BoundEnvironment,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    concrete = parent.instantiate(environment)
    local_purpose, keys = get_purpose_and_keys(environment, purpose, (concrete.content,))
    if concrete.order_by:
        grain = concrete.over + [concrete.content.output]
        for item in concrete.order_by:
            grain += [item.expr]
    else:
        grain = concrete.over + [concrete.content.output]
    modifiers = get_upstream_modifiers(concrete.content.concept_arguments, environment=environment)
    datatype = concrete.content.datatype
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
    environment: BoundEnvironment | None = None,
) -> Concept:
    concrete = parent.instantiate(environment)
    _, keys = get_purpose_and_keys(
        environment,
        Purpose.METRIC, tuple(x for x in concrete.by) if parent.by else None
    )
    # anything grouped to a grain should be a property
    # at that grain
    fmetadata = metadata or Metadata()
    aggfunction = concrete.function
    modifiers = get_upstream_modifiers(concrete.concept_arguments, environment=environment)
    granularity = Granularity.SINGLE_ROW if all(x.granularity == Granularity.SINGLE_ROW for x in concrete.by) else Granularity.MULTI_ROW
    out = Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        metadata=fmetadata,
        lineage=parent,
        grain=Grain(components=[x.address for x in concrete.by]) if concrete.by else Grain(),
        namespace=namespace,
        keys=set([x.address for x in concrete.by]) if concrete.by else keys,
        modifiers=modifiers,
        granularity = granularity
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
    environment: BoundEnvironment,
    namespace: str | None = None,
    name: str | None = None,
    metadata: Metadata | None = None,
    purpose: Purpose | None = None,
) -> Concept:
    namespace = namespace or environment.namespace
    if isinstance(parent, AggregateWrapper):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_agg_{parent.function.operator.value}_{string_to_hash(str(parent))}"
        return agg_wrapper_to_concept(parent, namespace, name, environment=environment, metadata=metadata)
    elif isinstance(parent, WindowItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(str(parent))}"
        return window_item_to_concept(parent, name, namespace, environment=environment, purpose=purpose, metadata=metadata)
    elif isinstance(parent, FilterItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_filter_{parent.content.name}_{string_to_hash(str(parent))}"
        return filter_item_to_concept(parent, name, namespace, environment=environment, purpose =purpose, metadata=metadata)
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
    # else:
    #     if not name:
    #         name = f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"
    #     return constant_to_concept(parent, name, namespace, purpose, metadata)
