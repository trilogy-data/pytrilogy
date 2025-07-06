from datetime import date, datetime
from typing import Iterable, List, Sequence, Tuple

from lark.tree import Meta

from trilogy.constants import (
    VIRTUAL_CONCEPT_PREFIX,
)
from trilogy.core.constants import ALL_ROWS_CONCEPT
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
    CaseElse,
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
    Parenthetical,
    RowsetItem,
    RowsetLineage,
    SubselectComparison,
    TraitDataType,
    TupleWrapper,
    UndefinedConcept,
    WhereClause,
    WindowItem,
    address_with_namespace,
)
from trilogy.core.models.core import DataType, arg_to_datatype
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import RowsetDerivationStatement, SelectStatement
from trilogy.utility import string_to_hash, unique


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
            and arg.operator != FunctionType.UNNEST
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


def get_upstream_modifiers(
    keys: Sequence[Concept | ConceptRef], environment: Environment
) -> list[Modifier]:
    modifiers = set()
    for pkey in keys:
        if isinstance(pkey, ConceptRef):
            pkey = environment.concepts[pkey.address]
        if isinstance(pkey, UndefinedConcept):
            continue
        if pkey.modifiers:
            modifiers.update(pkey.modifiers)
    return list(modifiers)


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
    if concept.derivation == Derivation.CONSTANT:
        return False
    if concept.is_aggregate and not (
        isinstance(concept.lineage, AggregateWrapper) and concept.lineage.by
    ):

        return False
    if concept.purpose in (Purpose.PROPERTY, Purpose.METRIC) and concept.keys:
        if all([c in others for c in concept.keys]):
            return False
    if (
        concept.purpose == Purpose.KEY
        and concept.keys
        and all([c in others for c in concept.keys])
    ):
        return False
    if concept.purpose in (Purpose.METRIC,):
        if all([c in others for c in concept.grain.components]):
            return False
    if concept.derivation in (Derivation.BASIC,) and isinstance(
        concept.lineage, Function
    ):
        relevant = False
        for arg in concept.lineage.arguments:
            relevant = atom_is_relevant(arg, others, environment) or relevant
        return relevant
    if concept.granularity == Granularity.SINGLE_ROW:
        return False
    return True


def concepts_to_grain_concepts(
    concepts: Iterable[Concept | ConceptRef | str],
    environment: Environment | None,
    local_concepts: dict[str, Concept] | None = None,
) -> list[Concept]:
    preconcepts: list[Concept] = []
    for c in concepts:
        if isinstance(c, Concept):
            preconcepts.append(c)

        elif isinstance(c, ConceptRef) and environment:
            if local_concepts and c.address in local_concepts:
                preconcepts.append(local_concepts[c.address])
            else:
                preconcepts.append(environment.concepts[c.address])
        elif isinstance(c, str) and environment:
            if local_concepts and c in local_concepts:
                preconcepts.append(local_concepts[c])
            else:
                preconcepts.append(environment.concepts[c])
        else:
            raise ValueError(
                f"Unable to resolve input {c} without environment provided to concepts_to_grain call"
            )
    pconcepts = []
    for x in preconcepts:
        if (
            x.lineage
            and isinstance(x.lineage, Function)
            and x.lineage.operator == FunctionType.ALIAS
        ):
            # if the function is an alias, use the unaliased concept to calculate grain
            pconcepts.append(environment.concepts[x.lineage.arguments[0].address])  # type: ignore
        else:
            pconcepts.append(x)
    final: List[Concept] = []
    for sub in pconcepts:
        if not concept_is_relevant(sub, pconcepts, environment):  # type: ignore
            continue
        final.append(sub)
    final = unique(final, "address")
    v2 = sorted(final, key=lambda x: x.name)
    return v2


def _get_relevant_parent_concepts(arg) -> tuple[list[ConceptRef], bool]:
    from trilogy.core.models.author import get_concept_arguments

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
    modifiers = get_upstream_modifiers(pkeys, environment)
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
    modifiers = get_upstream_modifiers(pkeys, environment)
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
    elif parent.operator == FunctionType.UNNEST:
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
    modifiers = get_upstream_modifiers(
        cparent.concept_arguments, environment=environment
    )
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


def window_item_to_concept(
    parent: WindowItem,
    name: str,
    namespace: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    fmetadata = metadata or Metadata()
    if not isinstance(parent.content, ConceptRef):
        raise NotImplementedError(
            f"Window function wiht non ref content {parent.content} not yet supported"
        )
    bcontent = environment.concepts[parent.content.address]
    if isinstance(bcontent, UndefinedConcept):
        return UndefinedConcept(address=f"{namespace}.{name}", metadata=fmetadata)
    if bcontent.purpose == Purpose.METRIC:
        local_purpose, keys = get_purpose_and_keys(None, (bcontent,), environment)
    else:
        local_purpose = Purpose.PROPERTY
        keys = Grain.from_concepts(
            [bcontent.address] + [y.address for y in parent.over], environment
        ).components

    # when including the order by in discovery grain
    if parent.order_by:

        grain_components = parent.over + [bcontent.output]
        for item in parent.order_by:
            relevant, _ = get_relevant_parent_concepts(item.expr)
            grain_components += relevant
    else:
        grain_components = parent.over + [bcontent.output]

    final_grain = Grain.from_concepts(grain_components, environment)
    modifiers = get_upstream_modifiers(bcontent.concept_arguments, environment)
    datatype = parent.content.datatype
    if parent.type in (
        # WindowType.RANK,
        WindowType.ROW_NUMBER,
        WindowType.COUNT,
        WindowType.COUNT_DISTINCT,
    ):
        datatype = DataType.INTEGER
    if parent.type == WindowType.RANK:
        datatype = TraitDataType(type=DataType.INTEGER, traits=["rank"])
    return Concept(
        name=name,
        datatype=datatype,
        purpose=local_purpose,
        lineage=parent,
        metadata=fmetadata,
        grain=final_grain,
        namespace=namespace,
        keys=keys,
        modifiers=modifiers,
        derivation=Derivation.WINDOW,
        granularity=bcontent.granularity,
    )


def agg_wrapper_to_concept(
    parent: AggregateWrapper,
    namespace: str,
    name: str,
    environment: Environment,
    metadata: Metadata | None = None,
) -> Concept:
    _, keys = get_purpose_and_keys(
        Purpose.METRIC, tuple(parent.by) if parent.by else None, environment=environment
    )
    # anything grouped to a grain should be a property
    # at that grain
    fmetadata = metadata or Metadata()
    aggfunction = parent.function
    modifiers = get_upstream_modifiers(parent.concept_arguments, environment)
    grain = Grain.from_concepts(parent.by, environment) if parent.by else Grain()
    granularity = Concept.calculate_granularity(Derivation.AGGREGATE, grain, parent)

    out = Concept(
        name=name,
        datatype=aggfunction.output_datatype,
        purpose=Purpose.METRIC,
        metadata=fmetadata,
        lineage=parent,
        grain=grain,
        namespace=namespace,
        keys=set([x.address for x in parent.by]) if parent.by else keys,
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
    datatypes = set([c.datatype for c in align.concepts])
    if len(datatypes) > 1:
        raise InvalidSyntaxException(
            f"Datatypes do not align for merged statements {align.alias}, have {datatypes}"
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
        datatype=datatypes.pop(),
        purpose=Purpose.PROPERTY,
        lineage=multi_lineage,
        grain=grain,
        namespace=align.namespace,
        granularity=Granularity.MULTI_ROW,
        derivation=Derivation.MULTISELECT,
        keys=set(x.address for x in align.concepts),
    )
    return new


def rowset_concept(
    orig_address: ConceptRef,
    environment: Environment,
    rowset: RowsetDerivationStatement,
    pre_output: list[Concept],
    orig: dict[str, Concept],
    orig_map: dict[str, Concept],
):
    orig_concept = environment.concepts[orig_address.address]
    name = orig_concept.name
    if isinstance(orig_concept.lineage, FilterItem):
        if orig_concept.lineage.where == rowset.select.where_clause and isinstance(
            orig_concept.lineage.content, (ConceptRef, Concept)
        ):
            name = environment.concepts[orig_concept.lineage.content.address].name
    base_namespace = (
        f"{rowset.name}.{orig_concept.namespace}"
        if orig_concept.namespace != rowset.namespace
        else rowset.name
    )

    new_concept = Concept(
        name=name,
        datatype=orig_concept.datatype,
        purpose=orig_concept.purpose,
        lineage=None,
        grain=orig_concept.grain,
        metadata=Metadata(concept_source=ConceptSource.CTE),
        namespace=base_namespace,
        keys=orig_concept.keys,
        derivation=Derivation.ROWSET,
        granularity=orig_concept.granularity,
        pseudonyms={
            address_with_namespace(x, rowset.name) for x in orig_concept.pseudonyms
        },
    )
    for x in orig_concept.pseudonyms:
        new_address = address_with_namespace(x, rowset.name)
        origa = environment.alias_origin_lookup[x]
        environment.concepts[new_address] = new_concept
        environment.alias_origin_lookup[new_address] = origa.model_copy(
            update={"namespace": f"{rowset.name}.{origa.namespace}"}
        )
    orig[orig_concept.address] = new_concept
    orig_map[new_concept.address] = orig_concept
    pre_output.append(new_concept)


def rowset_to_concepts(rowset: RowsetDerivationStatement, environment: Environment):
    pre_output: list[Concept] = []
    orig: dict[str, Concept] = {}
    orig_map: dict[str, Concept] = {}
    for orig_address in rowset.select.output_components:
        rowset_concept(orig_address, environment, rowset, pre_output, orig, orig_map)
    select_lineage = rowset.select.as_lineage(environment)
    for x in pre_output:
        x.lineage = RowsetItem(
            content=orig_map[x.address].reference,
            rowset=RowsetLineage(
                name=rowset.name,
                derived_concepts=[x.reference for x in pre_output],
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
        | FunctionCallWrapper
        | WindowItem
        | FilterItem
        | Function
        | ListWrapper
        | MapWrapper
        | int
        | float
        | str
        | date
    ),
    environment: Environment,
    namespace: str | None = None,
    name: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    namespace = namespace or environment.namespace
    # this is purely for the parse tree, discard from derivation
    if isinstance(parent, FunctionCallWrapper):
        return arbitrary_to_concept(
            parent.content, environment, namespace, name, metadata  # type: ignore
        )
    elif isinstance(parent, AggregateWrapper):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_agg_{parent.function.operator.value}_{string_to_hash(str(parent))}"
        return agg_wrapper_to_concept(
            parent, namespace, name, metadata=metadata, environment=environment
        )
    elif isinstance(parent, WindowItem):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_window_{parent.type.value}_{string_to_hash(str(parent))}"
        return window_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            metadata=metadata,
        )
    elif isinstance(parent, FilterItem):
        if not name:
            if isinstance(parent.content, ConceptRef):
                name = f"{VIRTUAL_CONCEPT_PREFIX}_filter_{parent.content.name}_{string_to_hash(str(parent))}"
            else:
                name = f"{VIRTUAL_CONCEPT_PREFIX}_filter_{string_to_hash(str(parent))}"
        return filter_item_to_concept(
            parent,
            name,
            namespace,
            environment=environment,
            metadata=metadata,
        )
    elif isinstance(parent, Function):
        if not name:
            if parent.operator == FunctionType.GROUP:
                name = (
                    f"{VIRTUAL_CONCEPT_PREFIX}_group_to_{string_to_hash(str(parent))}"
                )
            else:
                name = f"{VIRTUAL_CONCEPT_PREFIX}_func_{parent.operator.value}_{string_to_hash(str(parent))}"

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
    elif isinstance(parent, ListWrapper):
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"
        return constant_to_concept(parent, name, namespace, metadata)
    else:
        if not name:
            name = f"{VIRTUAL_CONCEPT_PREFIX}_{string_to_hash(str(parent))}"
        return constant_to_concept(parent, name, namespace, metadata)
