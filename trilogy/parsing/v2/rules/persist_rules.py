from __future__ import annotations

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import PersistMode
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.statements.author import PersistStatement, SelectStatement
from trilogy.parsing.v2.rules.datasource_rules import DatasourcePartitionClause
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.select_finalize import finalize_select_tree
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind

SUPPORTED_INCREMENTAL_TYPES: set[DataType] = {DataType.DATE, DataType.TIMESTAMP}


def persist_partition_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> DatasourcePartitionClause:
    args = hydrated_children(node, hydrate)
    cols = args[0] if isinstance(args[0], list) else args
    return DatasourcePartitionClause(
        columns=[context.concepts.reference(str(c)) for c in cols]
    )


def auto_persist(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> PersistStatement:
    from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta

    args = hydrated_children(node, hydrate)
    persist_mode = args[0]
    target_name = str(args[1])
    where = args[2] if len(args) > 2 else None
    if target_name not in context.environment.datasources:
        raise fail(
            node,
            f"Auto persist target datasource {target_name} does not exist in environment",
        )
    target = context.environment.datasources[target_name]
    select: SelectStatement = target.create_update_statement(
        context.environment, where, line_no=node.meta.line if node.meta else None
    )
    return PersistStatement(
        select=select,
        datasource=target,
        persist_mode=persist_mode,
        partition_by=target.incremental_by,
        meta=metadata_from_meta(node.meta),
    )


def full_persist(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> PersistStatement:
    from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta

    args = hydrated_children(node, hydrate)
    partition_clause = DatasourcePartitionClause([])
    labels: list[str] = [str(x) for x in args if isinstance(x, str)]
    for x in args:
        if isinstance(x, DatasourcePartitionClause):
            partition_clause = x
    if len(labels) == 2:
        identifier = labels[0]
        address_str: str | None = labels[1]
    else:
        identifier = labels[0]
        address_str = None
    target: Datasource | None = context.environment.datasources.get(identifier)

    if target:
        address_str = target.safe_address
    if not address_str:
        raise fail(
            node,
            f'Persist without address for unknown datasource "{identifier}"',
        )

    modes = [x for x in args if isinstance(x, PersistMode)]
    mode = modes[0] if modes else PersistMode.OVERWRITE
    select: SelectStatement = next(x for x in args if isinstance(x, SelectStatement))
    finalize_select_tree(select, context)

    if mode == PersistMode.APPEND:
        if target is None:
            raise fail(node, f"Cannot append to non-existent datasource {identifier}")
        new_datasource: Datasource = target
        if new_datasource.partition_by != partition_clause.columns:
            raise fail(node, "Partition mismatch for append")
        for x in partition_clause.columns:
            concept = context.concepts.require(x.address)
            if concept.output_datatype not in SUPPORTED_INCREMENTAL_TYPES:
                raise fail(
                    node,
                    f"Cannot incremental persist on {concept.address} of type {concept.output_datatype}",
                )
    elif target:
        new_datasource = target
    else:
        new_datasource = select.to_datasource(
            namespace=context.environment.namespace or DEFAULT_NAMESPACE,
            name=identifier,
            address=Address(location=address_str),
            grain=select.grain,
            environment=context.environment,
        )
    return PersistStatement(
        select=select,
        datasource=new_datasource,
        persist_mode=mode,
        partition_by=partition_clause.columns if partition_clause else [],
        meta=metadata_from_meta(node.meta),
    )


def persist_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> PersistStatement:
    args = hydrated_children(node, hydrate)
    return args[0]


PERSIST_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.PERSIST_STATEMENT: persist_statement,
    SyntaxNodeKind.AUTO_PERSIST: auto_persist,
    SyntaxNodeKind.FULL_PERSIST: full_persist,
    SyntaxNodeKind.PERSIST_PARTITION_CLAUSE: persist_partition_clause,
}
