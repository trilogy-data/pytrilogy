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
from trilogy.core.functions import arg_to_datatype, function_args_to_output_purpose
from trilogy.core.models import (
    AggregateWrapper,
    Concept,
    DataType,
    Environment,
    FilterItem,
    Function,
    FunctionClass,
    Grain,
    ListWrapper,
    MapWrapper,
    Meta,
    Metadata,
    Parenthetical,
    Purpose,
    WindowItem,
)
from trilogy.utility import string_to_hash, unique


def get_upstream_modifiers(keys: List[Concept]) -> list[Modifier]:
    modifiers = set()
    for pkey in keys:
        if pkey.modifiers:
            modifiers.update(pkey.modifiers)
    return list(modifiers)


def process_function_args(
    args,
    meta: Meta | None,
    environment: Environment,
) -> List[Concept | Function | str | int | float | date | datetime]:
    final: List[Concept | Function | str | int | float | date | datetime] = []
    for arg in args:
        # if a function has an anonymous function argument
        # create an implicit concept
        if isinstance(arg, Parenthetical):
            processed = process_function_args([arg.content], meta, environment)
            final.append(
                Function(
                    operator=FunctionType.PARENTHETICAL,
                    arguments=processed,
                    output_datatype=arg_to_datatype(processed[0]),
                    output_purpose=function_args_to_output_purpose(processed),
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

        else:
            final.append(arg)
    return final


def get_purpose_and_keys(
    purpose: Purpose | None, args: Tuple[Concept, ...] | None
) -> Tuple[Purpose, set[str] | None]:
    local_purpose = purpose or function_args_to_output_purpose(args)
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
        grain=const_function.output_grain,
        namespace=namespace,
        metadata=fmetadata,
    )


def concepts_to_grain_concepts(
    concepts: List[Concept] | List[str] | set[str], environment: Environment | None
) -> list[Concept]:
    environment = Environment() if environment is None else environment
    pconcepts: list[Concept] = [
        c if isinstance(c, Concept) else environment.concepts[c] for c in concepts
    ]

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
    parent: Function,
    name: str,
    environment: Environment,
    namespace: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    pkeys: List[Concept] = []
    namespace = namespace or environment.namespace
    for x in parent.arguments:
        pkeys += [
            x
            for x in parent.concept_arguments
            if not x.derivation == PurposeLineage.CONSTANT
        ]
    grain: Grain | None = Grain()
    for x in pkeys:
        grain += x.grain
    if parent.operator in FunctionClass.ONE_TO_MANY.value:
        # if the function will create more rows, we don't know what grain this is at
        grain = None
    modifiers = get_upstream_modifiers(pkeys)
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
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    modifiers = get_upstream_modifiers(parent.content.concept_arguments)
    return Concept(
        name=name,
        datatype=parent.content.datatype,
        purpose=Purpose.PROPERTY,
        lineage=parent,
        metadata=fmetadata,
        namespace=namespace,
        # filtered copies cannot inherit keys
        keys=(
            parent.content.keys
            if parent.content.purpose == Purpose.PROPERTY
            else {
                parent.content.address,
            }
        ),
        grain=(
            parent.content.grain
            if parent.content.purpose == Purpose.PROPERTY
            else Grain()
        ),
        modifiers=modifiers,
    )


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    local_purpose, keys = get_purpose_and_keys(purpose, (parent.content,))
    if parent.order_by:
        grain = parent.over + [parent.content.output]
        for item in parent.order_by:
            grain += [item.expr.output]
    else:
        grain = parent.over + [parent.content.output]
    modifiers = get_upstream_modifiers(parent.content.concept_arguments)
    datatype = parent.content.datatype
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
) -> Concept:
    _, keys = get_purpose_and_keys(
        Purpose.METRIC, tuple(parent.by) if parent.by else None
    )
    # anything grouped to a grain should be a property
    # at that grain
    fmetadata = metadata or Metadata()
    aggfunction = parent.function
    modifiers = get_upstream_modifiers(parent.concept_arguments)
    out = Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        metadata=fmetadata,
        lineage=parent,
        grain=Grain.from_concepts(parent.by) if parent.by else Grain(),
        namespace=namespace,
        keys=set([x.address for x in parent.by]) if parent.by else keys,
        modifiers=modifiers,
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
        return agg_wrapper_to_concept(parent, namespace, name, metadata)
    elif isinstance(parent, WindowItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(str(parent))}"
        return window_item_to_concept(parent, name, namespace, purpose, metadata)
    elif isinstance(parent, FilterItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_filter_{parent.content.name}_{string_to_hash(str(parent))}"
        return filter_item_to_concept(parent, name, namespace, purpose, metadata)
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
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"
        return constant_to_concept(parent, name, namespace, purpose, metadata)
