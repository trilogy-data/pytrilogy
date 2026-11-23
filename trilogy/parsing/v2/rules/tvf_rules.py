from __future__ import annotations

from typing import Any

from trilogy.constants import MAGIC_TVF_UNION_NAME, UNION_ARM_KEY
from trilogy.core.enums import (
    AggregateGroupingMode,
    ConceptSource,
    Derivation,
    FunctionType,
    Modifier,
    Purpose,
    SetOperator,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    Concept,
    ConceptRef,
    Function,
    Metadata,
    RowsetItem,
    RowsetLineage,
)
from trilogy.core.models.build import GROUPING_IDENTITY_FUNCTIONS
from trilogy.core.models.core import (
    DataType,
    EnumType,
    NumericType,
    TraitDataType,
    ValidatedType,
)
from trilogy.core.statements.author import (
    RollupSelectStatement,
    SelectItem,
    SelectStatement,
    TVFOutputItem,
    UnionSelectStatement,
)
from trilogy.parsing.common import constant_to_concept, union_item_to_concept
from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules.function_rules import grouping_spec
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
    hidden = False
    for arg in hydrated_children(node, hydrate):
        if arg is Modifier.HIDDEN:
            hidden = True
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
        hidden=hidden,
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


def _is_grouping_flag(address: str, context: RuleContext) -> bool:
    concept = context.concepts.get(address)
    return (
        concept is not None
        and isinstance(concept.lineage, AggregateWrapper)
        and concept.lineage.function.operator in GROUPING_IDENTITY_FUNCTIONS
    )


def lower_grouping_select(
    arm: SelectStatement, context: RuleContext, scope: str, meta: Any = None
) -> SelectStatement | RollupSelectStatement:
    """Lower a finalized `select ... by rollup|cube|grouping sets (...)` to the
    relational TVF over it: one new output concept per arm column, registered
    under ``scope``. The arm's grain (its keys and grouping flags) is the
    relation's identity; a rolled key is nullable, its subtotal NULL a value.
    A spec over no keys is one standard group and stays a plain select."""
    if arm.grouping is None or not arm.grain.components:
        if arm.signature:
            raise InvalidSyntaxException(
                "a rollup over no keys is a plain select and takes no output "
                "signature; drop the `-> (...)`."
            )
        return arm
    namespace = context.environment.namespace
    columns = arm.output_components
    hidden = arm.hidden_components
    validate_grouping_signature(arm, len(columns) - len(hidden))
    align_items = [
        AlignItem(
            alias=f"_{scope}_rollup_{ref.address.replace('.', '_')}",
            namespace=namespace,
            concepts=[ref],
            hidden=ref.address in hidden,
        )
        for ref in columns
    ]
    align_c = AlignClause(items=align_items)
    positions = _identity_positions([arm], SetOperator.UNION_ALL, len(columns), context)
    identity = {align_items[p].aligned_concept for p in positions}
    statement = RollupSelectStatement(
        selects=[arm],
        align=align_c,
        namespace=namespace,
        derived_concepts=[],
        meta=arm.meta,
        outputs=arm.signature,
    )
    lineage = statement.as_lineage(context.environment)
    for pos, ref in enumerate(columns):
        key = pos in positions
        concept = union_item_to_concept(
            align_items[pos],
            align_c,
            [arm],
            context.environment,
            purpose=Purpose.KEY if key else Purpose.PROPERTY,
            keys=identity,
            nullable=key and not _is_grouping_flag(ref.address, context),
            lineage=lineage,
        )
        statement.derived_concepts.append(concept)
        context.add_multiselect_concept(concept, meta=meta)
    return statement


def validate_grouping_signature(arm: SelectStatement, width: int) -> None:
    if not arm.signature:
        return
    if len(arm.signature) != width:
        raise InvalidSyntaxException(
            f"the arm projects {width} column(s) but the output signature "
            f"declares {len(arm.signature)}. Name each arm column once, in order."
        )
    for out in arm.signature:
        if out.purpose is not None or out.datatype is not None:
            raise InvalidSyntaxException(
                f"output `{out.name}` of a rollup/cube/grouping_sets call takes "
                "its type from the arm column; give a name only."
            )


TVF_GROUPING_MODES: dict[SyntaxNodeKind | None, AggregateGroupingMode] = {
    SyntaxNodeKind.TVF_ROLLUP_INVOCATION: AggregateGroupingMode.ROLLUP,
    SyntaxNodeKind.TVF_CUBE_INVOCATION: AggregateGroupingMode.CUBE,
    SyntaxNodeKind.TVF_GROUPING_SETS_INVOCATION: AggregateGroupingMode.GROUPING_SETS,
}

_KEY_LIST_KINDS = (SyntaxNodeKind.EXPR_OVER_LIST, SyntaxNodeKind.GROUPING_SET)


def tvf_grouping_invocation(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> SelectStatement:
    """`rollup|cube|grouping_sets((select ...), keys)`: the arm select carrying
    the grouping spec and any `-> (...)` names, not yet finalized. The caller
    finalizes it and lowers it with `lower_grouping_select`."""
    arm: SelectStatement | None = None
    key_lists: list[list[Any]] = []
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.TVF_REL_ARG:
            arm = hydrate(child)
        elif child.kind == SyntaxNodeKind.TVF_OUTPUT and arm is not None:
            arm.signature = hydrate(child)
        elif child.kind in _KEY_LIST_KINDS:
            key_lists.append(hydrate(child))
    if arm is None:
        raise fail(node, "a grouping call needs a parenthesized select as its arm")
    if arm.grouping is not None:
        raise fail(node, "the arm of a grouping call cannot carry its own grouping")
    arm.grouping = grouping_spec(
        TVF_GROUPING_MODES[node.kind], key_lists, context, node
    )
    return arm


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


def bind_set_op(
    call: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
    meta: Any,
) -> tuple[UnionSelectStatement, list[Concept]]:
    """Lower an inline `union|except|intersect(...) -> (...)` under a hidden
    magic namespace and expose its signature columns as bare ROWSET-derived
    local bindings, so they resolve in the select that reads them without
    leaking globally and build via the rowset path. Returns the set op and
    those bindings, in signature order."""
    assert call.kind is not None
    union_stmt = _lower_set_op(
        node=call,
        context=context,
        hydrate=hydrate,
        namespace=MAGIC_TVF_UNION_NAME,
        operator=TVF_INVOCATION_OPERATORS[call.kind],
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
    wrappers: list[Concept] = []
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
        context.add_select_concept(wrapper, meta=meta)
        wrappers.append(wrapper)
    return union_stmt, wrappers[: len(union_stmt.outputs)]


TVF_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.TVF_OUTPUT_ITEM: tvf_output_item,
    SyntaxNodeKind.TVF_OUTPUT: tvf_output,
    SyntaxNodeKind.TVF_REL_ARG: tvf_rel_arg,
    SyntaxNodeKind.TVF_UNION_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_EXCEPT_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_INTERSECT_INVOCATION: tvf_set_op_invocation,
    SyntaxNodeKind.TVF_ROLLUP_INVOCATION: tvf_grouping_invocation,
    SyntaxNodeKind.TVF_CUBE_INVOCATION: tvf_grouping_invocation,
    SyntaxNodeKind.TVF_GROUPING_SETS_INVOCATION: tvf_grouping_invocation,
}
