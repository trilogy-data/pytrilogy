from __future__ import annotations

from dataclasses import dataclass

from trilogy.core.enums import Modifier, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Metadata,
)
from trilogy.core.models.core import DataType, TraitDataType
from trilogy.core.statements.author import SelectStatement, UnionSelectStatement
from trilogy.parsing.v2.concept_factory import union_item_to_concept_v2
from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.select_finalize import finalize_select_statement
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


@dataclass
class TVFOutputItem:
    """One positional output column of a TVF signature `-> (...)`."""

    name: str
    purpose: Purpose | None = None
    datatype: DataType | TraitDataType | None = None
    nullable: bool = False
    metadata: Metadata | None = None


def tvf_output_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> TVFOutputItem:
    # Children (all but name optional): [hide] name [purpose] [datatype] [nullable] [metadata]
    name: str | None = None
    purpose: Purpose | None = None
    datatype: DataType | TraitDataType | None = None
    nullable = False
    metadata: Metadata | None = None
    for arg in hydrated_children(node, hydrate):
        if arg is Modifier.HIDDEN:
            continue
        elif arg is Modifier.NULLABLE:
            nullable = True
        elif isinstance(arg, Purpose):
            purpose = arg
        elif isinstance(arg, Metadata):
            metadata = arg
        elif name is None and not isinstance(arg, (DataType, TraitDataType)):
            name = str(arg)
        else:
            datatype = arg
    if name is None:
        raise fail(node, "TVF output item requires a name")
    return TVFOutputItem(
        name=name,
        purpose=purpose,
        datatype=datatype,
        nullable=nullable,
        metadata=metadata,
    )


def tvf_output(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[TVFOutputItem]:
    return [x for x in hydrated_children(node, hydrate) if isinstance(x, TVFOutputItem)]


def tvf_rel_arg(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            return hydrate(child)
    raise fail(node, "union(...) argument must be a parenthesized select")


def tvf_union_invocation(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> UnionSelectStatement:
    arm_nodes: list[SyntaxNode] = []
    output_node: SyntaxNode | None = None
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.TVF_REL_ARG:
            arm_nodes.append(child)
        elif child.kind == SyntaxNodeKind.TVF_OUTPUT:
            output_node = child

    if output_node is None:
        raise fail(
            node,
            "A table-valued union(...) requires an output signature: "
            "`union(...) -> (col1, col2)`.",
        )
    if len(arm_nodes) < 2:
        raise InvalidSyntaxException(
            "union(...) requires at least two relational arms."
        )

    outputs: list[TVFOutputItem] = hydrate(output_node)

    # Each arm is hydrated under its own alias scope so identical surface names
    # (`-> dt` in every arm) mangle to distinct internal addresses; the
    # positional binding below ties each arm's i-th column to one output.
    arms: list[SelectStatement] = []
    for i, arm_node in enumerate(arm_nodes):
        with context.semantic_state.rowset_alias_scope(f"__tvf_arm_{i}"):
            arm = hydrate(arm_node)
        finalize_select_statement(arm, context)
        arms.append(arm)

    for i, arm in enumerate(arms):
        n = len(arm.output_components)
        if n != len(outputs):
            raise InvalidSyntaxException(
                f"union arm {i} projects {n} column(s) but the output signature "
                f"declares {len(outputs)}. Each arm must project exactly one "
                "column per output item, in order."
            )

    namespace = context.environment.namespace
    align_items = [
        AlignItem(
            alias=out.name,
            namespace=namespace,
            concepts=[arm.output_components[pos] for arm in arms],
            hidden=False,
        )
        for pos, out in enumerate(outputs)
    ]
    align_c = AlignClause(items=align_items)

    derived_concepts = []
    for pos, out in enumerate(outputs):
        concept = union_item_to_concept_v2(
            align_items[pos],
            align_c,
            arms,
            context,
            purpose=out.purpose,
            datatype=out.datatype,
            nullable=out.nullable,
        )
        derived_concepts.append(concept)
        context.add_multiselect_concept(concept, meta=core_meta(node.meta))

    return UnionSelectStatement(
        selects=arms,
        align=align_c,
        namespace=namespace,
        derived_concepts=derived_concepts,
        meta=metadata_from_meta(node.meta),
    )


def tvf_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
):
    """Inline `from union(...) -> (...) select ...`.

    Desugars to an anonymous (magic-named) rowset wrapping the union, then
    hydrates the trailing select. The union outputs become rowset concepts and
    bare names resolve to them as select-local bindings.
    """
    from trilogy.constants import DEFAULT_NAMESPACE, MAGIC_TVF_UNION_NAME
    from trilogy.core.statements.author import RowsetDerivationStatement
    from trilogy.parsing.v2.rowset_semantics import rowset_to_concepts_v2
    from trilogy.parsing.v2.select_finalize import finalize_select_tree

    union_node: SyntaxNode | None = None
    select_node: SyntaxNode | None = None
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.TVF_UNION_INVOCATION:
            union_node = child
        elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            select_node = child
    if union_node is None or select_node is None:
        raise fail(node, "inline union requires `from union(...) -> (...) select ...`")

    name = MAGIC_TVF_UNION_NAME
    with context.semantic_state.rowset_alias_scope(name):
        union_stmt: UnionSelectStatement = hydrate(union_node)
    finalize_select_tree(union_stmt, context)
    rowset = RowsetDerivationStatement(
        name=name,
        select=union_stmt,
        namespace=context.environment.namespace or DEFAULT_NAMESPACE,
    )
    result = rowset_to_concepts_v2(rowset, context)
    # Expose the union outputs as bare select-local bindings (resolvable without
    # the magic prefix) so the trailing select reads `dt`/`val` directly.
    for new_concept in result.concepts:
        context.add_rowset_concept(new_concept, meta=core_meta(node.meta), force=True)
        bare = new_concept.with_namespace(context.environment.namespace)
        context.add_select_concept(bare, meta=core_meta(node.meta))
    if result.alias_updates:
        context.semantic_state.stage_rowset_aliases(result.alias_updates)

    return hydrate(select_node)


TVF_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.TVF_OUTPUT_ITEM: tvf_output_item,
    SyntaxNodeKind.TVF_OUTPUT: tvf_output,
    SyntaxNodeKind.TVF_REL_ARG: tvf_rel_arg,
    SyntaxNodeKind.TVF_UNION_INVOCATION: tvf_union_invocation,
    SyntaxNodeKind.TVF_SELECT_STATEMENT: tvf_select_statement,
}
