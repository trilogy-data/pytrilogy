from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE, REMOTE_PREFIXES
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    AddressType,
    DatasourceState,
    FunctionType,
    Granularity,
    Modifier,
    Purpose,
)
from trilogy.core.internal import INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    Comment,
    Concept,
    ConceptRef,
    Function,
    Grain,
    Metadata,
    WhereClause,
)
from trilogy.core.models.core import ListWrapper
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
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind, SyntaxTokenKind


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
        additional_paths: list[str] | None = None,
    ):
        self.path = path
        self.write_path = write_path
        self.type = type
        self.exists = exists
        self.additional_paths = additional_paths or []


@dataclass
class FilePathList:
    paths: list[str]


@dataclass
class FileConstRef:
    name: str


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
    # Grain is a declarative set of addresses. v1 tolerated unresolved
    # references during import parse (via fail_on_missing=False) so grain
    # could point at virtual paths like `order_item.product.id` that only
    # exist as derivation chains. Mirror that by resolving when possible
    # and otherwise keeping the raw address string.
    components: set[str] = set()
    for a in cols:
        resolved = context.concepts.get(a)
        components.add(resolved.address if resolved is not None else a)
    return Grain(components=components)


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
    child = node.children[0]
    raw = str(hydrate(child))
    quoted = False
    if (
        isinstance(child, SyntaxNode) is False
        and getattr(child, "kind", None) == SyntaxTokenKind.F_QUOTED_ADDRESS
    ):
        location = raw
        quoted = True
    elif len(raw) >= 2 and raw[0] == "`" and raw[-1] == "`":
        location = raw[1:-1]
        quoted = True
    else:
        location = raw.strip("'\"")
    return Address(location=location, type=AddressType.TABLE, quoted=quoted)


def query_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Query:
    args = hydrated_children(node, hydrate)
    return Query(text=str(args[0]))


def file_path_list_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FilePathList:
    args = hydrated_children(node, hydrate)
    return FilePathList(paths=[str(a) for a in args])


def file_const_ref_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> FileConstRef:
    args = hydrated_children(node, hydrate)
    return FileConstRef(name=str(args[0]))


def _resolve_const_paths(
    node: SyntaxNode, context: RuleContext, name: str
) -> list[str]:
    lookup = f"{DEFAULT_NAMESPACE}.{name}" if "." not in name else name
    concept = context.environment.concepts.get(lookup)
    if concept is None:
        raise fail(node, f"Unknown reference '{name}' in file — not defined.")
    if (
        not isinstance(concept.lineage, Function)
        or concept.lineage.operator != FunctionType.CONSTANT
    ):
        raise fail(
            node,
            f"'{name}' in file must reference a constant, not {concept.purpose}.",
        )
    value = concept.lineage.arguments[0]
    if isinstance(value, (list, tuple, ListWrapper)):
        paths = [str(v) for v in value]
    else:
        paths = [str(value)]
    if not paths:
        raise fail(node, f"Constant '{name}' referenced by file is empty.")
    return paths


def _process_file_path(context: RuleContext, ipath: str) -> tuple[str, str, bool]:
    is_cloud = ipath.startswith(REMOTE_PREFIXES)
    is_glob = any(c in ipath for c in "*?[")
    if is_cloud:
        base = ipath
        suffix = "." + ipath.rsplit(".", 1)[-1] if "." in ipath else ""
    else:
        path = Path(ipath)
        if path.is_relative_to("."):
            path = Path(context.environment.working_path) / path
        base = str(path.resolve().absolute())
        suffix = path.suffix
    # Globs cannot be stat'd as a single file; trust the caller.
    exists = is_cloud or is_glob or Path(base).exists()
    return base, suffix, exists


_FILE_TYPE_MAP = {
    ".sql": AddressType.SQL,
    ".py": AddressType.PYTHON_SCRIPT,
    ".csv": AddressType.CSV,
    ".tsv": AddressType.TSV,
    ".parquet": AddressType.PARQUET,
}


def _build_file_from_paths(
    node: SyntaxNode, context: RuleContext, paths: list[str]
) -> File:
    processed = [_process_file_path(context, p) for p in paths]
    suffixes = {s for _, s, _ in processed}
    if len(suffixes) != 1:
        raise fail(
            node,
            f"All paths must share the same extension (got {sorted(suffixes)})",
        )
    suffix = next(iter(suffixes))
    addr_type = _FILE_TYPE_MAP.get(suffix)
    if addr_type is None:
        raise fail(node, f"Unsupported file type {suffix}")
    bases = [b for b, _, _ in processed]
    exists = all(e for _, _, e in processed)
    return File(
        path=bases[0],
        write_path=None,
        type=addr_type,
        exists=exists,
        additional_paths=bases[1:] if len(bases) > 1 else [],
    )


def file_node(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> File:
    args = hydrated_children(node, hydrate)

    # Array form: file [ `a.parquet`, `b.parquet`, ... ]
    if len(args) == 1 and isinstance(args[0], FilePathList):
        if not args[0].paths:
            raise fail(node, "file [...] requires at least one path")
        return _build_file_from_paths(node, context, args[0].paths)

    # Constant reference form: file URLS (a const list or single string)
    if len(args) == 1 and isinstance(args[0], FileConstRef):
        paths = _resolve_const_paths(node, context, args[0].name)
        return _build_file_from_paths(node, context, paths)

    write_path: str | None = None
    if len(args) == 2:
        read_path, write_path = str(args[0]), str(args[1])
    else:
        read_path = str(args[0])

    read_base, suffix, exists = _process_file_path(context, read_path)
    write_base = _process_file_path(context, write_path)[0] if write_path else None

    addr_type = _FILE_TYPE_MAP.get(suffix)
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
                additional_locations=list(val.additional_paths),
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

    if addr.is_file and partition_by:
        addr.partition_columns = [c.address.split(".")[-1] for c in partition_by]
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
            resolved_keys = [context.concepts.get(g) for g in key_inputs]
            # Skip inheritance if any grain component is a symbolic address
            # that hasn't resolved to a real concept yet.
            if any(k is None for k in resolved_keys):
                continue
            eligible = True
            for k in resolved_keys:
                if column.concept.address in (k.keys or set()):  # type: ignore[union-attr]
                    eligible = False
            if not eligible:
                continue
            target_c.keys = set(k.address for k in resolved_keys)  # type: ignore[union-attr]
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
    SyntaxNodeKind.FILE_PATH_LIST: file_path_list_node,
    SyntaxNodeKind.FILE_CONST_REF: file_const_ref_node,
    SyntaxNodeKind.DATASOURCE: datasource_node,
    SyntaxNodeKind.WHOLE_GRAIN_CLAUSE: whole_grain_clause,
    SyntaxNodeKind.RAW_COLUMN_ASSIGNMENT: raw_column_assignment,
    SyntaxNodeKind.DATASOURCE_STATUS_CLAUSE: datasource_status_clause,
    SyntaxNodeKind.DATASOURCE_PARTITION_CLAUSE: datasource_partition_clause,
    SyntaxNodeKind.DATASOURCE_UPDATE_TRIGGER_CLAUSE: datasource_update_trigger_clause,
}
