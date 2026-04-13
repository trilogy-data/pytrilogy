from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import REMOTE_PREFIXES
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
    Comment,
    Concept,
    ConceptRef,
    Grain,
    Metadata,
    WhereClause,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    RawColumnExpr,
)
from trilogy.parsing.helpers import comment_body
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


@dataclass
class WholeGrainWrapper:
    where: WhereClause


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
            merged = comment_body(arg)
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
        parents = [context.concepts.require(k) for k in parents_arg]
    else:
        parents = [context.concepts.require(str(parents_arg))]

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
        context.add_datasource_property_concept(concept, meta=core_meta(node.meta))
        concepts.append(concept)
    return concepts


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
    resolved = context.concepts.require(concept_name)
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
    return Grain(components=set(context.concepts.require(a).address for a in cols))


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
    raw = str(args[0])
    if len(raw) >= 2 and raw[0] == "`" and raw[-1] == "`":
        location = raw[1:-1]
    else:
        location = raw.strip("'\"")
    return Address(location=location, type=AddressType.TABLE)


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
        exists = is_cloud or Path(base).exists()
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
    return DatasourceState(str(args[1]).lower())


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
    # Propagate keys from datasource grain to foreign key concepts.
    # A KEY concept on a datasource that isn't part of the grain
    # gets the grain components as its keys (matching v1 second-pass behaviour).
    if grain:
        for column in columns:
            if column.concept.address in grain.components:
                continue
            target_c = context.concepts.require(column.concept.address)
            if target_c.purpose != Purpose.KEY:
                continue
            key_inputs = grain.components
            eligible = True
            for key in key_inputs:
                if column.concept.address in (
                    context.concepts.require(key).keys or set()
                ):
                    eligible = False
            if not eligible:
                continue
            keys = [context.concepts.require(g) for g in key_inputs]
            target_c.keys = set(x.address for x in keys)
    return datasource


DATASOURCE_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.PROPERTIES_DECLARATION: properties_declaration,
    SyntaxNodeKind.INLINE_PROPERTY: inline_property,
    SyntaxNodeKind.INLINE_PROPERTY_LIST: inline_property_list,
    SyntaxNodeKind.PROP_IDENT_LIST: prop_ident_list,
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
}
