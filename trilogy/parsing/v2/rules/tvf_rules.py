from __future__ import annotations

from dataclasses import dataclass

from trilogy.constants import MAGIC_TVF_UNION_NAME
from trilogy.core.enums import ConceptSource, Derivation, Modifier, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Concept,
    ConceptRef,
    Metadata,
    RowsetItem,
    RowsetLineage,
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


def _lower_union(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
    namespace: str,
) -> UnionSelectStatement:
    """Lower a `union(...)` node to a UnionSelectStatement whose output concepts
    are bound (and registered) under ``namespace``."""
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

    # Named form (`with combined as union(...) -> (k, v)`) is lowered inside the
    # rowset's alias scope: register the union's aligned outputs under the hidden
    # per-rowset name (`local._combined_k`) instead of the bare `local.k`, so they
    # don't collide with a later `select ... as k` that reads them back through the
    # rowset wrapper (`combined.k`), which would close a build cycle. The rowset
    # wrapping unmangles the name back to `combined.k`. The inline form has no
    # rowset scope, so its outputs keep their bare names (resolved in the trailing
    # select).
    rowset_name = context.semantic_state.current_rowset_name
    align_items = [
        AlignItem(
            alias=(
                context.semantic_state.mangle_rowset_alias(rowset_name, out.name)
                if rowset_name is not None
                else out.name
            ),
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


def tvf_union_invocation(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> UnionSelectStatement:
    return _lower_union(node, context, hydrate, context.environment.namespace)


def tvf_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
):
    """Inline `from union(...) -> (...) select ...`.

    Lowers the union under a hidden magic namespace, then exposes its outputs
    as bare ROWSET-derived local bindings (so they resolve in the trailing
    select without leaking globally, and build via the rowset path rather than
    recursing as a directly-selected multiselect concept)."""
    union_node: SyntaxNode | None = None
    select_node: SyntaxNode | None = None
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.TVF_UNION_INVOCATION:
            union_node = child
        elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            select_node = child
    if union_node is None or select_node is None:
        raise fail(node, "inline union requires `from union(...) -> (...) select ...`")

    union_stmt = _lower_union(
        node=union_node,
        context=context,
        hydrate=hydrate,
        namespace=MAGIC_TVF_UNION_NAME,
    )
    union_lineage = union_stmt.as_lineage(context.environment)
    env_ns = context.environment.namespace
    derived = union_stmt.derived_concepts
    # derived_concepts must reference the rowset OUTPUTS (the wrappers), not the
    # inner union concepts — mirrors rowset_to_concepts_v2 (else the build cycles).
    wrapper_refs = [
        ConceptRef(address=f"{env_ns}.{uc.name}", datatype=uc.datatype)
        for uc in derived
    ]
    for uc in derived:
        wrapper = Concept(
            name=uc.name,
            datatype=uc.datatype,
            purpose=uc.purpose,
            namespace=env_ns,
            # Keys (the per-arm internal columns) don't exist in the outer scope;
            # mirror rowset_to_concepts, which clears them for combined outputs.
            keys=set(),
            grain=uc.grain,
            modifiers=uc.modifiers,
            granularity=uc.granularity,
            derivation=Derivation.ROWSET,
            metadata=Metadata(concept_source=ConceptSource.CTE),
            lineage=RowsetItem(
                content=uc.reference,
                rowset=RowsetLineage(
                    name=MAGIC_TVF_UNION_NAME,
                    derived_concepts=wrapper_refs,
                    select=union_lineage,
                ),
            ),
        )
        context.add_select_concept(wrapper, meta=core_meta(node.meta))
    return hydrate(select_node)


TVF_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.TVF_OUTPUT_ITEM: tvf_output_item,
    SyntaxNodeKind.TVF_OUTPUT: tvf_output,
    SyntaxNodeKind.TVF_REL_ARG: tvf_rel_arg,
    SyntaxNodeKind.TVF_UNION_INVOCATION: tvf_union_invocation,
    SyntaxNodeKind.TVF_SELECT_STATEMENT: tvf_select_statement,
}
