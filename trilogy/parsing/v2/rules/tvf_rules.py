from __future__ import annotations

from trilogy.constants import MAGIC_TVF_UNION_NAME, UNION_ARM_KEY
from trilogy.core.enums import (
    ConceptSource,
    Derivation,
    FunctionType,
    Modifier,
    Purpose,
    SetOperator,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Concept,
    ConceptRef,
    Function,
    Metadata,
    RowsetItem,
    RowsetLineage,
)
from trilogy.core.models.core import (
    DataType,
    EnumType,
    NumericType,
    TraitDataType,
    ValidatedType,
)
from trilogy.core.statements.author import (
    SelectItem,
    SelectStatement,
    TVFOutputItem,
    UnionSelectStatement,
)
from trilogy.parsing.common import constant_to_concept, union_item_to_concept
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


def tvf_output_item(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> TVFOutputItem:
    # Children (all but name optional): [hide] name [purpose] [datatype] [nullable] [metadata]
    name: str | None = None
    purpose: Purpose | None = None
    datatype: DataType | TraitDataType | ValidatedType | None = None
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
        elif name is None and not isinstance(
            arg, (DataType, TraitDataType, ValidatedType, EnumType, NumericType)
        ):
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
    raise fail(
        node, "union/except/intersect(...) argument must be a parenthesized select"
    )


TVF_INVOCATION_OPERATORS: dict[SyntaxNodeKind, SetOperator] = {
    SyntaxNodeKind.TVF_UNION_INVOCATION: SetOperator.UNION_ALL,
    SyntaxNodeKind.TVF_EXCEPT_INVOCATION: SetOperator.EXCEPT,
    SyntaxNodeKind.TVF_INTERSECT_INVOCATION: SetOperator.INTERSECT,
}

_TVF_SURFACE_NAMES: dict[SetOperator, str] = {
    SetOperator.UNION_ALL: "union",
    SetOperator.EXCEPT: "except",
    SetOperator.INTERSECT: "intersect",
}


ARM_KEY = UNION_ARM_KEY


def _add_arm_key(
    arm: SelectStatement, scope: str, index: int, context: RuleContext
) -> None:
    key = constant_to_concept(index, f"{scope}{ARM_KEY}", context.environment.namespace)
    context.add_select_concept(key)
    arm.selection.append(SelectItem(content=key.reference))


def _identity_positions(
    arms: list[SelectStatement],
    operator: SetOperator,
    width: int,
    context: RuleContext,
) -> set[int]:
    """Output positions identifying a set op's tuples: each arm's grain (read
    through its select aliases), or its whole output tuple where its grain is
    not among its outputs (as for any select), over the arms its tuples come
    from."""
    positions: set[int] = set()
    for arm in _tuple_sources(arms, operator):
        addresses = [
            _alias_source(c.address, context) for c in arm.output_components[:width]
        ]
        grain = arm.grain.components
        if grain and grain <= set(addresses):
            positions |= {i for i, a in enumerate(addresses) if a in grain}
        else:
            positions |= set(range(width))
    return positions


def _tuple_sources(
    arms: list[SelectStatement], operator: SetOperator
) -> list[SelectStatement]:
    """The arms a set op's tuples come from: an except/intersect's are the
    first arm's."""
    return arms if operator == SetOperator.UNION_ALL else arms[:1]


def _is_arm_key(arms: list[SelectStatement], pos: int, context: RuleContext) -> bool:
    """A key every source arm passes through stays a key outside the
    identity."""
    for arm in arms:
        concept = context.concepts.get(arm.output_components[pos].address)
        if concept is None or concept.purpose != Purpose.KEY:
            return False
    return True


def _alias_source(address: str, context: RuleContext) -> str:
    concept = context.concepts.get(address)
    if (
        concept is not None
        and isinstance(concept.lineage, Function)
        and concept.lineage.operator == FunctionType.ALIAS
    ):
        return concept.lineage.concept_arguments[0].address
    return address


def _lower_set_op(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
    namespace: str,
    operator: SetOperator,
) -> UnionSelectStatement:
    """Lower a `union(...)`/`except(...)`/`intersect(...)` node to a
    UnionSelectStatement whose output concepts are bound (and registered)
    under ``namespace``."""
    surface = _TVF_SURFACE_NAMES[operator]
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
            f"A table-valued {surface}(...) requires an output signature: "
            f"`{surface}(...) -> (col1, col2)`.",
        )
    if len(arm_nodes) < 2:
        raise InvalidSyntaxException(
            f"{surface}(...) requires at least two relational arms."
        )

    outputs: list[TVFOutputItem] = hydrate(output_node)
    declared = len(outputs)
    signature = list(outputs)
    if any(out.name == ARM_KEY for out in outputs):
        raise InvalidSyntaxException(
            f"{surface}(...) output name `{ARM_KEY}` is reserved; rename it."
        )

    # Each arm is hydrated under its own alias scope so identical surface names
    # (`-> dt` in every arm) mangle to distinct internal addresses; the
    # positional binding below ties each arm's i-th column to one output. The
    # scope carries the rowset name, or a set op whose arm reads another set-op
    # rowset would rebind that rowset's arm columns to its own.
    rowset_name = context.semantic_state.current_rowset_name
    arm_scope = f"__{rowset_name}_arm" if rowset_name is not None else "__tvf_arm"
    stacked = operator == SetOperator.UNION_ALL
    arms: list[SelectStatement] = []
    arm_scopes = [f"{arm_scope}_{i}" for i in range(len(arm_nodes))]
    for i, arm_node in enumerate(arm_nodes):
        with context.semantic_state.rowset_alias_scope(arm_scopes[i]):
            arm = hydrate(arm_node)
        if stacked:
            _add_arm_key(arm, arm_scopes[i], i, context)
        finalize_select_statement(arm, context)
        arms.append(arm)

    for i, arm in enumerate(arms):
        n = len(arm.output_components) - stacked
        if n != declared:
            raise InvalidSyntaxException(
                f"{surface} arm {i} projects {n} column(s) but the output signature "
                f"declares {declared}. Each arm must project exactly one "
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
    if stacked:
        outputs = [*outputs, TVFOutputItem(name=ARM_KEY, purpose=Purpose.KEY)]
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

    positions = _identity_positions(arms, operator, declared, context)
    if stacked:
        positions.add(declared)
    identity = {f"{namespace}.{align_items[p].alias}" for p in positions}
    derived_concepts = []
    for pos, out in enumerate(outputs):
        concept = union_item_to_concept(
            align_items[pos],
            align_c,
            arms,
            context.environment,
            purpose=out.purpose
            or (
                Purpose.KEY
                if pos in positions
                or _is_arm_key(_tuple_sources(arms, operator), pos, context)
                else Purpose.PROPERTY
            ),
            keys=identity,
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
        operator=operator,
        meta=metadata_from_meta(node.meta),
        outputs=signature,
        arm_scopes=arm_scopes,
    )


def tvf_set_op_invocation(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> UnionSelectStatement:
    operator = TVF_INVOCATION_OPERATORS.get(node.kind) if node.kind else None
    if operator is None:
        raise fail(node, "unknown set-op invocation")
    return _lower_set_op(
        node,
        context,
        hydrate,
        context.environment.namespace,
        operator,
    )


def tvf_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
):
    """Inline `from union|except|intersect(...) -> (...) select ...`.

    Lowers the set op under a hidden magic namespace, then exposes its outputs
    as bare ROWSET-derived local bindings (so they resolve in the trailing
    select without leaking globally, and build via the rowset path rather than
    recursing as a directly-selected multiselect concept)."""
    union_node: SyntaxNode | None = None
    operator: SetOperator | None = None
    select_node: SyntaxNode | None = None
    for child in node.child_nodes():
        if child.kind is not None and child.kind in TVF_INVOCATION_OPERATORS:
            union_node = child
            operator = TVF_INVOCATION_OPERATORS[child.kind]
        elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            select_node = child
    if union_node is None or operator is None or select_node is None:
        raise fail(
            node,
            "inline set op requires `from union|except|intersect(...) -> (...) select ...`",
        )

    union_stmt = _lower_set_op(
        node=union_node,
        context=context,
        hydrate=hydrate,
        namespace=MAGIC_TVF_UNION_NAME,
        operator=operator,
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
    SyntaxNodeKind.TVF_UNION_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_EXCEPT_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_INTERSECT_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_SELECT_STATEMENT: tvf_select_statement,
}
