from preql.core.models import (
    AggregateWrapper,
    Concept,
    Function,
    Grain,
    Purpose,
    Metadata,
    FilterItem,
    ListWrapper,
    WindowItem,
)
from typing import List, Tuple
from preql.core.functions import (
    function_args_to_output_purpose,
    FunctionType,
    arg_to_datatype,
)
from preql.utility import unique
from preql.core.enums import PurposeLineage


def get_purpose_and_keys(
    purpose: Purpose | None, args: Tuple[Concept, ...] | None
) -> Tuple[Purpose, Tuple[Concept, ...] | None]:
    local_purpose = purpose or function_args_to_output_purpose(args)
    if local_purpose in (Purpose.PROPERTY, Purpose.METRIC) and args:
        keys = concept_list_to_keys(args)
    else:
        keys = None
    return local_purpose, keys


def concept_list_to_keys(concepts: Tuple[Concept, ...]) -> Tuple[Concept, ...]:
    final_keys: List[Concept] = []
    for concept in concepts:
        if concept.keys:
            final_keys += concept_list_to_keys(concept.keys)
        elif concept.purpose != Purpose.PROPERTY:
            final_keys.append(concept)
    return tuple(unique(final_keys, "address"))


def constant_to_concept(
    parent: ListWrapper | int | float | str,
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
    return Concept(
        name=name,
        datatype=const_function.output_datatype,
        purpose=Purpose.CONSTANT,
        lineage=const_function,
        grain=const_function.output_grain,
        namespace=namespace,
        metadata=metadata,
    )


def function_to_concept(parent: Function, name: str, namespace: str) -> Concept:
    pkeys = []
    for x in parent.arguments:
        pkeys += [
            x
            for x in parent.concept_arguments
            if not x.derivation == PurposeLineage.CONSTANT
        ]
    grain = Grain()
    for x in pkeys:
        grain += x.grain

    key_grain = []
    for x in pkeys:
        if x.keys:
            key_grain += [*x.keys]
        else:
            key_grain.append(x)
    keys = tuple(Grain(components=key_grain).components_copy)
    if not pkeys:
        purpose = Purpose.CONSTANT
    else:
        purpose = parent.output_purpose
    return Concept(
        name=name,
        datatype=parent.output_datatype,
        purpose=purpose,
        lineage=parent,
        namespace=namespace,
        grain=grain,
        keys=keys,
    )


def filter_item_to_concept(
    parent: FilterItem,
    name: str,
    namespace: str,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:

    return Concept(
        name=name,
        datatype=parent.content.datatype,
        purpose=parent.content.purpose,
        lineage=parent,
        metadata=metadata,
        namespace=namespace,
        keys=parent.content.keys,
        grain=(
            parent.content.grain
            if parent.content.purpose == Purpose.PROPERTY
            else Grain()
        ),
    )


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    purpose: Purpose | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    local_purpose, keys = get_purpose_and_keys(purpose, (parent.content,))
    if parent.order_by:
        grain = parent.over + [parent.content.output]
        for item in parent.order_by:
            grain += [item.expr.output]
    else:
        grain = parent.over + [parent.content.output]
    return Concept(
        name=name,
        datatype=parent.content.datatype,
        purpose=local_purpose,
        lineage=parent,
        metadata=metadata,
        # filters are implicitly at the grain of the base item
        grain=Grain(components=grain),
        namespace=namespace,
        keys=keys,
    )


def agg_wrapper_to_concept(
    parent: AggregateWrapper,
    namespace: str,
    name: str,
    metadata: Metadata | None = None,
    purpose: Purpose | None = None,
) -> Concept:
    local_purpose, keys = get_purpose_and_keys(
        Purpose.METRIC, tuple(parent.by) if parent.by else None
    )
    # anything grouped to a grain should be a property
    # at that grain
    aggfunction = parent.function
    return Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        metadata=metadata,
        lineage=aggfunction,
        grain=(Grain(components=parent.by) if parent.by else aggfunction.output_grain),
        namespace=namespace,
        keys=tuple(parent.by) if parent.by else keys,
    )
