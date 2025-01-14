from datetime import date, datetime
from typing import List, Sequence, Tuple, Iterable

from lark.tree import Meta

from trilogy.constants import (
    VIRTUAL_CONCEPT_PREFIX,
)
from trilogy.core.enums import (
    ConceptSource,
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
    Concept,
    FilterItem,
    Function,
    Grain,
    HavingClause,
    ListWrapper,
    MapWrapper,
    Metadata,
    MultiSelectLineage,
    Parenthetical,
    RowsetItem,
    RowsetLineage,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import DataType, arg_to_datatype
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import RowsetDerivationStatement, SelectStatement
from trilogy.utility import string_to_hash, unique


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


def get_upstream_modifiers(keys: List[Concept]) -> list[Modifier]:
    modifiers = set()
    for pkey in keys:
        if pkey.modifiers:
            modifiers.update(pkey.modifiers)
    return list(modifiers)


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


def concept_is_relevant(concept: Concept, others: list[Concept]) -> bool:
    if concept.is_aggregate and not (
        isinstance(concept.lineage, AggregateWrapper) and concept.lineage.by
    ):
        return False
    if concept.purpose in (Purpose.PROPERTY, Purpose.METRIC) and concept.keys:
        if any([c in others for c in concept.keys]):
            return False
    if concept.purpose in (Purpose.METRIC,):
        if all([c in others for c in concept.grain.components]):
            return False
    if concept.derivation in (Derivation.BASIC,):
        return any(concept_is_relevant(c, others) for c in concept.concept_arguments)
    if concept.granularity == Granularity.SINGLE_ROW:
        return False
    return True


def concepts_to_grain_concepts(
    concepts: Iterable[Concept | str], environment: Environment | None
) -> list[Concept]:
    environment = Environment() if environment is None else environment
    pconcepts: list[Concept] = [
        c if isinstance(c, Concept) else environment.concepts[c] for c in concepts
    ]

    final: List[Concept] = []
    for sub in pconcepts:
        if not concept_is_relevant(sub, pconcepts):
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
            if not x.derivation == Derivation.CONSTANT
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


def align_item_to_concept(
    parent: AlignItem,
    align_clause: AlignClause,
    selects: list[SelectStatement],
    local_concepts: dict[str, Concept],
    environment: Environment,
    where: WhereClause | None = None,
    having: HavingClause | None = None,
    limit: int | None = None,
) -> Concept:
    align = parent
    datatypes = set([c.datatype for c in align.concepts])
    purposes = set([c.purpose for c in align.concepts])
    if len(datatypes) > 1:
        raise InvalidSyntaxException(
            f"Datatypes do not align for merged statements {align.alias}, have {datatypes}"
        )
    if len(purposes) > 1:
        purpose = Purpose.KEY
    else:
        purpose = list(purposes)[0]
    new_selects = [x.as_lineage(environment) for x in selects]
    new = Concept(
        name=align.alias,
        datatype=datatypes.pop(),
        purpose=purpose,
        lineage=MultiSelectLineage(
            selects=new_selects,
            align=align_clause,
            namespace=align.namespace,
            local_concepts=local_concepts,
            where_clause=where,
            having_clause=having,
            limit=limit,
            hidden_components=set(y for x in new_selects for y in x.hidden_components),
        ),
        namespace=align.namespace,
    )
    return new


def rowset_to_concepts(rowset: RowsetDerivationStatement, environment: Environment):
    pre_output: list[Concept] = []
    orig: dict[str, Concept] = {}
    orig_map: dict[str, Concept] = {}
    for orig_concept in rowset.select.output_components:
        name = orig_concept.name
        if isinstance(orig_concept.lineage, FilterItem):
            if orig_concept.lineage.where == rowset.select.where_clause:
                name = orig_concept.lineage.content.name

        new_concept = Concept(
            name=name,
            datatype=orig_concept.datatype,
            purpose=orig_concept.purpose,
            lineage=None,
            grain=orig_concept.grain,
            # TODO: add proper metadata
            metadata=Metadata(concept_source=ConceptSource.CTE),
            namespace=(
                f"{rowset.name}.{orig_concept.namespace}"
                if orig_concept.namespace != rowset.namespace
                else rowset.name
            ),
            keys=orig_concept.keys,
        )
        orig[orig_concept.address] = new_concept
        orig_map[new_concept.address] = orig_concept
        pre_output.append(new_concept)
    select_lineage = rowset.select.as_lineage(environment)
    for x in pre_output:
        x.lineage = RowsetItem(
            content=orig_map[x.address],
            where=rowset.select.where_clause,
            rowset=RowsetLineage(
                name=rowset.name,
                derived_concepts=pre_output,
                select=select_lineage,
            ),
        )
    default_grain = Grain.from_concepts([*pre_output])
    # remap everything to the properties of the rowset
    for x in pre_output:
        if x.keys:
            if all([k in orig for k in x.keys]):
                x.keys = set([orig[k].address if k in orig else k for k in x.keys])
            else:
                # TODO: fix this up
                x.keys = set()
        if all([c in orig for c in x.grain.components]):
            x.grain = Grain(components={orig[c].address for c in x.grain.components})
        else:
            x.grain = default_grain
    return pre_output


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
