from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    AddressType,
    DatasourceState,
    Granularity,
    Modifier,
    Purpose,
)
from trilogy.core.internal import INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    ArgBinding,
    Comment,
    Concept,
    ConceptRef,
    Grain,
    Metadata,
    OrderItem,
    SubselectItem,
    TraitDataType,
    UndefinedConcept,
    WhereClause,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    RawColumnExpr,
)
from trilogy.core.statements.author import (
    FunctionDeclaration,
    MergeStatementV2,
    RawSQLStatement,
    RowsetDerivationStatement,
)
from trilogy.parsing.common import rowset_to_concepts
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


@dataclass
class WholeGrainWrapper:
    where: WhereClause


@dataclass
class FunctionBindingType:
    type: DataType | TraitDataType | None = None


class DatasourceUpdateTrigger(Enum):
    INCREMENTAL = "incremental"
    FRESHNESS = "freshness"


@dataclass
class DatasourcePartitionClause:
    columns: list[ConceptRef]


@dataclass
class DatasourceUpdateTriggerClause:
    trigger_type: DatasourceUpdateTrigger
    columns: list[ConceptRef]


@dataclass
class DatasourceFreshnessProbeClause:
    path: str


REMOTE_PREFIXES = ("gs://", "s3://", "az://", "http://", "https://")


class File:
    def __init__(
        self,
        path: str,
        write_path: str | None = None,
        type: AddressType = AddressType.PARQUET,
        exists: bool = True,
    ):
        self.path = path
        self.write_path = write_path
        self.type = type
        self.exists = exists


class Query:
    def __init__(self, text: str):
        self.text = text


# --- Properties handlers ---


def inline_property(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[Any]:
    return hydrated_children(node, hydrate)


def inline_property_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[list[Any]]:
    args = hydrated_children(node, hydrate)
    props: list[list[Any]] = []
    for arg in args:
        if isinstance(arg, list):
            props.append(arg)
        elif isinstance(arg, Comment) and props:
            merged = arg.text.split("#")[1].rstrip()
            prop_args = props[-1]
            existing = next((a for a in prop_args if isinstance(a, Metadata)), None)
            if existing is None:
                prop_args.append(Metadata(description=merged))
            elif not existing.description:
                existing.description = merged
    return props


def prop_ident_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[str]:
    return [str(a) for a in hydrated_children(node, hydrate)]


def properties_declaration(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[Concept]:
    args = hydrated_children(node, hydrate)
    parents_arg = args[0]
    inline_props: list[list[Any]] = args[1]

    if isinstance(parents_arg, list):
        parents = [context.environment.concepts[k] for k in parents_arg]
    else:
        parents = [context.environment.concepts[str(parents_arg)]]

    grain_components = {x.address for x in parents}
    namespace = parents[0].namespace
    all_rows_addr = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
    is_abstract_grain = grain_components == {all_rows_addr}

    concepts: list[Concept] = []
    for prop_args in inline_props:
        name = str(prop_args[0])
        datatype = prop_args[1]
        metadata = Metadata()
        modifiers: list[Modifier] = []
        for extra in prop_args[2:]:
            if isinstance(extra, Metadata):
                metadata = extra
            elif isinstance(extra, Modifier):
                modifiers.append(extra)
        concept = Concept(
            name=name,
            datatype=datatype,
            purpose=Purpose.PROPERTY,
            metadata=metadata,
            grain=Grain(components=grain_components),
            namespace=namespace,
            keys=grain_components,
            modifiers=modifiers,
            granularity=(
                Granularity.SINGLE_ROW if is_abstract_grain else Granularity.MULTI_ROW
            ),
        )
        context.environment.add_concept(concept)
        concepts.append(concept)
    return concepts


# --- Datasource handlers ---


def concept_assignment(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[Any]:
    return hydrated_children(node, hydrate)


def column_assignment(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ColumnAssignment:
    args = hydrated_children(node, hydrate)
    modifiers: list[Modifier] = []
    if len(args) == 2:
        alias = args[0]
        concept_list = args[1]
        short_binding = False
    else:
        alias = args[0][-1]
        concept_list = args[0]
        short_binding = True
    if isinstance(concept_list, list) and len(concept_list) > 1:
        modifiers += [m for m in concept_list[:-1] if isinstance(m, Modifier)]
    concept_name = concept_list[-1] if isinstance(concept_list, list) else concept_list
    resolved = context.environment.concepts[concept_name]
    if short_binding:
        alias = resolved.safe_address
    return ColumnAssignment(
        alias=alias, modifiers=modifiers, concept=resolved.reference
    )


def column_assignment_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[ColumnAssignment]:
    return hydrated_children(node, hydrate)


def column_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[str]:
    return hydrated_children(node, hydrate)


def grain_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Grain:
    args = hydrated_children(node, hydrate)
    cols = args[0] if isinstance(args[0], list) else args
    return Grain(components=set(context.environment.concepts[a].address for a in cols))


def whole_grain_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> WholeGrainWrapper:
    args = hydrated_children(node, hydrate)
    return WholeGrainWrapper(where=args[0])


def address_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return Address(location=str(args[0]).strip("'\""), type=AddressType.TABLE)


def query_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Query:
    args = hydrated_children(node, hydrate)
    return Query(text=str(args[0]))


def file_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> File:
    args = hydrated_children(node, hydrate)
    write_path: str | None = None
    if len(args) == 2:
        read_path, write_path = str(args[0]), str(args[1])
    else:
        read_path = str(args[0])

    def process_path(ipath: str) -> tuple[str, str, bool]:
        is_cloud = ipath.startswith(REMOTE_PREFIXES)
        if is_cloud:
            base = ipath
            suffix = "." + ipath.rsplit(".", 1)[-1] if "." in ipath else ""
        else:
            path = Path(ipath)
            if path.is_relative_to("."):
                path = Path(context.environment.working_path) / path
            base = str(path.resolve().absolute())
            suffix = path.suffix
        exists = not is_cloud and Path(base).exists()
        return base, suffix, exists

    read_base, suffix, exists = process_path(read_path)
    write_base = process_path(write_path)[0] if write_path else None

    type_map = {
        ".sql": AddressType.SQL,
        ".py": AddressType.PYTHON_SCRIPT,
        ".csv": AddressType.CSV,
        ".tsv": AddressType.TSV,
        ".parquet": AddressType.PARQUET,
    }
    addr_type = type_map.get(suffix)
    if addr_type is None:
        raise fail(node, f"Unsupported file type {suffix}")
    return File(path=read_base, write_path=write_base, type=addr_type, exists=exists)


def raw_column_assignment(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RawColumnExpr:
    args = hydrated_children(node, hydrate)
    return RawColumnExpr(text=str(args[1]))


def datasource_status_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DatasourceState:
    args = hydrated_children(node, hydrate)
    return args[1]


def datasource_partition_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DatasourcePartitionClause:
    args = hydrated_children(node, hydrate)
    return DatasourcePartitionClause(
        columns=[ConceptRef(address=arg) for arg in args[0]]
    )


def datasource_update_trigger_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DatasourceUpdateTriggerClause | DatasourceFreshnessProbeClause:
    args = hydrated_children(node, hydrate)
    trigger_type = DatasourceUpdateTrigger(str(args[0]).lower())
    if isinstance(args[1], str):
        if trigger_type != DatasourceUpdateTrigger.FRESHNESS:
            raise fail(node, "Probe scripts only supported for freshness triggers")
        p = Path(args[1])
        if not p.is_absolute():
            p = (Path(context.environment.working_path) / p).resolve()
        return DatasourceFreshnessProbeClause(path=str(p))
    columns = [ConceptRef(address=arg) for arg in args[1]]
    return DatasourceUpdateTriggerClause(trigger_type=trigger_type, columns=columns)


def datasource_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Datasource:
    args = hydrated_children(node, hydrate)
    is_root = False
    is_partial = False
    filtered: list[Any] = []
    for a in args:
        if isinstance(a, str) and a.lower() == "root":
            is_root = True
        elif isinstance(a, Modifier) and a == Modifier.PARTIAL:
            is_partial = True
        else:
            filtered.append(a)

    name = filtered[0]
    columns: list[ColumnAssignment] = filtered[1]
    grain: Grain | None = None
    addr: Address | None = None
    where: WhereClause | None = None
    non_partial_for: WhereClause | None = None
    incremental_by: list[ConceptRef] = []
    partition_by: list[ConceptRef] = []
    freshness_by: list[ConceptRef] = []
    freshness_probe: str | None = None
    ds_status = DatasourceState.PUBLISHED

    for val in filtered[1:]:
        if isinstance(val, Address):
            addr = val
        elif isinstance(val, Grain):
            grain = val
        elif isinstance(val, WholeGrainWrapper):
            non_partial_for = val.where
        elif isinstance(val, Query):
            addr = Address(location=val.text, type=AddressType.QUERY)
        elif isinstance(val, File):
            addr = Address(
                location=val.path,
                write_location=val.write_path,
                type=val.type,
                exists=val.exists,
            )
        elif isinstance(val, WhereClause):
            where = val
        elif isinstance(val, DatasourceState):
            ds_status = val
        elif isinstance(val, DatasourceFreshnessProbeClause):
            freshness_probe = val.path
        elif isinstance(val, DatasourceUpdateTriggerClause):
            if val.trigger_type == DatasourceUpdateTrigger.INCREMENTAL:
                incremental_by = val.columns
            elif val.trigger_type == DatasourceUpdateTrigger.FRESHNESS:
                freshness_by = val.columns
        elif isinstance(val, DatasourcePartitionClause):
            partition_by = val.columns

    if not addr:
        raise fail(node, "Datasource missing address or query declaration")

    if addr.is_file and not addr.exists:
        ds_status = DatasourceState.UNPOPULATED
    if is_partial:
        for pc in columns:
            if Modifier.PARTIAL not in pc.modifiers:
                pc.modifiers.append(Modifier.PARTIAL)

    datasource = Datasource(
        name=name,
        columns=columns,
        grain=grain,  # type: ignore
        address=addr,
        namespace=context.environment.namespace,
        where=where,
        non_partial_for=non_partial_for,
        status=ds_status,
        incremental_by=incremental_by,
        partition_by=partition_by,
        freshness_by=freshness_by,
        freshness_probe=freshness_probe,
        is_root=is_root,
        is_partial=is_partial,
    )
    context.environment.add_datasource(datasource)
    return datasource


# --- Merge handler ---


def merge_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> MergeStatementV2 | None:
    args = hydrated_children(node, hydrate)
    modifiers: list[Modifier] = []
    cargs: list[str] = []
    for arg in args:
        if isinstance(arg, Modifier):
            modifiers.append(arg)
        else:
            cargs.append(str(arg))
    source_str, target_str = cargs[0], cargs[1]
    source_wildcard = None
    target_wildcard = None

    if source_str.endswith(".*"):
        if not target_str.endswith(".*"):
            raise fail(node, "Source is wildcard but target is not")
        source_wildcard = source_str[:-2]
        target_wildcard = target_str[:-2]
        sources = [
            v
            for v in context.environment.concepts.values()
            if v.namespace == source_wildcard
        ]
        targets: dict[str, Concept] = {}
        for x in sources:
            taddr = target_wildcard + "." + x.name
            if taddr in context.environment.concepts:
                targets[x.address] = context.environment.concepts[taddr]
        sources = [x for x in sources if x.address in targets]
    else:
        sources = [context.environment.concepts[source_str]]
        targets = {sources[0].address: context.environment.concepts[target_str]}

    for source_c in sources:
        if isinstance(source_c, UndefinedConcept):
            raise fail(
                node, f"Cannot merge non-existent source concept {source_c.address}"
            )

    result = MergeStatementV2(
        sources=sources,
        targets=targets,
        modifiers=modifiers,
        source_wildcard=source_wildcard,
        target_wildcard=target_wildcard,
    )
    for source_c in result.sources:
        context.environment.merge_concept(
            source_c, targets[source_c.address], modifiers
        )
    return result


# --- Function definition handlers ---


def function_binding_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[ArgBinding]:
    return hydrated_children(node, hydrate)


def function_binding_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionBindingType:
    args = hydrated_children(node, hydrate)
    return FunctionBindingType(type=args[0])


def function_binding_default(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return args[1] if len(args) > 1 else args[0]


def function_binding_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ArgBinding:
    args = hydrated_children(node, hydrate)
    default = None
    dtype = None
    for arg in args[1:]:
        if isinstance(arg, FunctionBindingType):
            dtype = arg.type
        else:
            default = arg
    return ArgBinding(
        name=str(args[0]), datatype=dtype or DataType.UNKNOWN, default=default
    )


def raw_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionDeclaration:
    from trilogy.core.models.author import CustomFunctionFactory

    args = hydrated_children(node, hydrate)
    identity = str(args[0])
    function_arguments: list[ArgBinding] = args[1]
    output = args[2]
    context.environment.functions[identity] = CustomFunctionFactory(
        function=output,
        namespace=context.environment.namespace,
        function_arguments=function_arguments,
        name=identity,
    )
    return FunctionDeclaration(name=identity, args=function_arguments, expr=output)


def table_function(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FunctionDeclaration:
    from trilogy.core.models.author import CustomFunctionFactory

    args = hydrated_children(node, hydrate)
    identity = str(args[0])
    idx = 1
    function_arguments: list[ArgBinding] = []
    if idx < len(args) and isinstance(args[idx], list):
        function_arguments = args[idx]
        idx += 1
    content = args[idx]
    if isinstance(content, Concept):
        content = content.reference
    idx += 1
    where = None
    order_by: list[OrderItem] = []
    limit = None
    for arg in args[idx:]:
        if isinstance(arg, WhereClause):
            where = arg
        elif isinstance(arg, list):
            order_by = arg
        elif isinstance(arg, int):
            limit = arg
    sub = SubselectItem(content=content, where=where, order_by=order_by, limit=limit)
    context.environment.functions[identity] = CustomFunctionFactory(
        function=sub,
        namespace=context.environment.namespace,
        function_arguments=function_arguments,
        name=identity,
    )
    return FunctionDeclaration(name=identity, args=function_arguments, expr=sub)


# --- RawSQL handler ---


def rawsql_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RawSQLStatement:
    args = hydrated_children(node, hydrate)
    return RawSQLStatement(text=str(args[0]))


# --- Node hydrator registry ---


def rowset_derivation_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> RowsetDerivationStatement:
    args = hydrated_children(node, hydrate)
    name = str(args[0])
    select = args[1]
    output = RowsetDerivationStatement(
        name=name,
        select=select,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    for new_concept in rowset_to_concepts(output, context.environment):
        context.environment.add_concept(new_concept, force=True)
    context.environment.add_rowset(
        output.name, output.select.as_lineage(context.environment)
    )
    return output


STATEMENT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    # properties
    SyntaxNodeKind.PROPERTIES_DECLARATION: properties_declaration,
    SyntaxNodeKind.INLINE_PROPERTY: inline_property,
    SyntaxNodeKind.INLINE_PROPERTY_LIST: inline_property_list,
    SyntaxNodeKind.PROP_IDENT_LIST: prop_ident_list,
    # datasource sub-components
    SyntaxNodeKind.CONCEPT_ASSIGNMENT: concept_assignment,
    SyntaxNodeKind.COLUMN_ASSIGNMENT: column_assignment,
    SyntaxNodeKind.COLUMN_ASSIGNMENT_LIST: column_assignment_list,
    SyntaxNodeKind.COLUMN_LIST: column_list,
    SyntaxNodeKind.GRAIN_CLAUSE: grain_clause,
    SyntaxNodeKind.ADDRESS: address_node,
    SyntaxNodeKind.QUERY: query_node,
    SyntaxNodeKind.FILE: file_node,
    SyntaxNodeKind.DATASOURCE: datasource_node,
    SyntaxNodeKind.WHOLE_GRAIN_CLAUSE: whole_grain_clause,
    SyntaxNodeKind.RAW_COLUMN_ASSIGNMENT: raw_column_assignment,
    SyntaxNodeKind.DATASOURCE_STATUS_CLAUSE: datasource_status_clause,
    SyntaxNodeKind.DATASOURCE_PARTITION_CLAUSE: datasource_partition_clause,
    SyntaxNodeKind.DATASOURCE_UPDATE_TRIGGER_CLAUSE: datasource_update_trigger_clause,
    # merge
    SyntaxNodeKind.MERGE_STATEMENT: merge_statement,
    # function definitions
    SyntaxNodeKind.RAW_FUNCTION: raw_function,
    SyntaxNodeKind.TABLE_FUNCTION: table_function,
    SyntaxNodeKind.FUNCTION_BINDING_LIST: function_binding_list,
    SyntaxNodeKind.FUNCTION_BINDING_ITEM: function_binding_item,
    SyntaxNodeKind.FUNCTION_BINDING_TYPE: function_binding_type,
    SyntaxNodeKind.FUNCTION_BINDING_DEFAULT: function_binding_default,
    # rawsql
    SyntaxNodeKind.RAWSQL_STATEMENT: rawsql_statement,
    # rowset
    SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT: rowset_derivation_statement,
}
