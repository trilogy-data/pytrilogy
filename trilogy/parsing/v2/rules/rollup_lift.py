"""Lift the post-aggregate columns of a grouping arm into a select over its TVF.

A `rollup|cube|grouping_sets` arm computes one pass: its keys, its aggregates
and the grouping flags. A window, a scalar over measures or a HAVING reads that
pass's rows, so it belongs to a select over the relation's handles, not to the
arm. `split_arm` decides the split; `outer_select` re-points the lifted columns
at the handles once the pure arm is lowered.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, fields, is_dataclass, replace
from typing import Any

from trilogy.core.enums import Derivation, Modifier, Purpose
from trilogy.core.models.author import (
    AggregateWrapper,
    Concept,
    ConceptRef,
    HavingClause,
    NumberingWindowItem,
    OrderBy,
    OrderItem,
)
from trilogy.core.statements.author import (
    ConceptTransform,
    SelectItem,
    SelectStatement,
)
from trilogy.parsing.common import arbitrary_to_concept
from trilogy.parsing.v2.rules_context import RuleContext

_POST = (Derivation.WINDOW, Derivation.BASIC)


@dataclass
class ArmSplit:
    """The pure arm, and what the select over its relation reads back."""

    arm: SelectStatement
    # authored arm column -> its lifted expression, still in arm space
    lifted: dict[str, Any] = field(default_factory=dict)
    having: Any = None
    order_by: OrderBy | None = None
    limit: int | None = None


def _substitute(node: Any, match: Callable[[Any], Any]) -> Any:
    """Replace every node `match` answers for, anywhere in an expression."""
    hit = match(node)
    if hit is not None:
        return hit
    if isinstance(node, list):
        new = [_substitute(x, match) for x in node]
        return node if all(a is b for a, b in zip(new, node)) else new
    if not is_dataclass(node) or isinstance(node, (type, ConceptRef, Concept)):
        return node
    changed = {}
    for f in (f for f in fields(node) if f.init):
        value = getattr(node, f.name)
        new_value = _substitute(value, match)
        if new_value is not value:
            changed[f.name] = new_value
    return replace(node, **changed) if changed else node


def _concept(select: SelectStatement, address: str, context: RuleContext) -> Concept:
    return select.local_concepts.get(address) or context.concepts.require(address)


def _is_inline_aggregate(node: Any) -> bool:
    return isinstance(node, AggregateWrapper)


def _leaves(node: Any) -> list[Any]:
    """The concept references and inline aggregates an expression reads."""
    found: list[Any] = []

    def match(n: Any) -> Any:
        if isinstance(n, ConceptRef) or _is_inline_aggregate(n):
            found.append(n)
            return n
        return None

    _substitute(node, match)
    return found


def _reads_pass(node: Any, select: SelectStatement, context: RuleContext) -> bool:
    for leaf in _leaves(node):
        if not isinstance(leaf, ConceptRef):
            return True
        concept = _concept(select, leaf.address, context)
        if concept.derivation == Derivation.AGGREGATE:
            return True
        if concept.derivation in _POST and _reads_pass(
            concept.lineage, select, context
        ):
            return True
    return False


def _is_post(concept: Concept, select: SelectStatement, context: RuleContext) -> bool:
    if (
        concept.address in select.grain.components
        or concept.lineage is None
        or _is_inline_aggregate(concept.lineage)
    ):
        return False
    if concept.derivation == Derivation.WINDOW:
        return True
    return concept.derivation == Derivation.BASIC and _reads_pass(
        concept.lineage, select, context
    )


class _Minter:
    """Hidden arm columns for the pass values a lifted expression reads."""

    def __init__(
        self, arm: SelectStatement, context: RuleContext, lifted: set[str]
    ) -> None:
        self.arm = arm
        self.context = context
        self.lifted = lifted
        self.outputs = {item.concept.address for item in arm.selection}
        self.added: list[SelectItem] = []
        self.ok = True

    def _add(self, item: SelectItem, address: str) -> None:
        if address not in self.outputs:
            self.outputs.add(address)
            self.added.append(item)

    def leaf(self, node: Any) -> ConceptRef | None:
        if _is_inline_aggregate(node):
            concept = arbitrary_to_concept(node, self.context.environment)
            self.arm.local_concepts[concept.address] = concept
            self._add(
                SelectItem(
                    content=ConceptTransform(function=node, output=concept),
                    modifiers=[Modifier.HIDDEN],
                ),
                concept.address,
            )
            return concept.reference
        if isinstance(node, ConceptRef):
            self.check(node.address)
        return None

    def check(self, address: str) -> None:
        if address in self.outputs or address in self.lifted:
            return
        concept = _concept(self.arm, address, self.context)
        if concept.purpose == Purpose.CONSTANT:
            return
        if (
            address in self.arm.grain.components
            or concept.derivation == Derivation.AGGREGATE
        ):
            self._add(
                SelectItem(content=concept.reference, modifiers=[Modifier.HIDDEN]),
                address,
            )
            return
        # a row-level column or an unprojected post value: no handle reads it
        self.ok = False

    def rewrite(self, node: Any) -> Any:
        return _substitute(node, self.leaf)


def _lifted_expression(item: SelectItem, concept: Concept) -> Any:
    if isinstance(item.content, ConceptTransform):
        return item.content.function
    return concept.lineage


def split_arm(arm: SelectStatement, context: RuleContext) -> ArmSplit | None:
    """The pure arm and the columns lifted out of it, or None when nothing
    reads the pass or a lifted column reads what no handle carries."""
    post = [
        item
        for item in arm.selection
        if _is_post(_concept(arm, item.concept.address, context), arm, context)
    ]
    if not post and arm.having_clause is None:
        return None
    lifted_addresses = {item.concept.address for item in post}
    keep = [
        item for item in arm.selection if item.concept.address not in lifted_addresses
    ]
    pure = replace(arm, selection=keep, having_clause=None, order_by=None, limit=None)
    minter = _Minter(pure, context, lifted_addresses)
    lifted = {
        item.concept.address: minter.rewrite(
            _lifted_expression(item, _concept(arm, item.concept.address, context))
        )
        for item in post
    }
    having = (
        minter.rewrite(arm.having_clause.conditional) if arm.having_clause else None
    )
    order_by = (
        OrderBy(
            items=[
                OrderItem(expr=minter.rewrite(o.expr), order=o.order)
                for o in arm.order_by.items
            ]
        )
        if arm.order_by
        else None
    )
    for expression in [*lifted.values(), having, order_by]:
        for leaf in _leaves(expression):
            minter.check(leaf.address)
    if not minter.ok:
        return None
    pure.selection = [*keep, *minter.added]
    return ArmSplit(
        arm=pure,
        lifted=lifted,
        having=having,
        order_by=order_by,
        limit=arm.limit,
    )


def _ranked_by_identity(expression: Any, identity: list[ConceptRef]) -> Any:
    """A rank of a tuple ranks the relation's rows: in the arm every pass row
    was one, and the visible keys do not tell a subtotal from a NULL key."""
    if not isinstance(expression, NumberingWindowItem) or not expression.arguments:
        return expression
    named = {arg.address for arg in expression.arguments}
    extra = [ref for ref in identity if ref.address not in named]
    return replace(expression, arguments=[*expression.arguments, *extra])


def outer_select(
    authored: SelectStatement,
    split: ArmSplit,
    handles: dict[str, Concept],
    scope: str,
    context: RuleContext,
) -> SelectStatement:
    """The select over the pure arm's relation: each authored column in order,
    a pass column as its handle and a lifted one re-pointed at the handles.
    Every output is labelled with the arm column it shows as."""
    refs: dict[str, ConceptRef] = {a: h.reference for a, h in handles.items()}

    def repoint(node: Any) -> Any:
        return _substitute(
            node,
            lambda n: refs.get(n.address) if isinstance(n, ConceptRef) else None,
        )

    identity = [
        refs[address]
        for address in sorted(split.arm.grain.components)
        if address in refs
    ]
    hidden = authored.hidden_components
    selection: list[SelectItem] = []
    labels: dict[str, ConceptRef] = {}
    for item in authored.selection:
        address = item.concept.address
        modifiers = [Modifier.HIDDEN] if address in hidden else []
        if address not in split.lifted:
            selection.append(SelectItem(content=refs[address], modifiers=modifiers))
            labels[refs[address].address] = item.concept
            continue
        source = _concept(authored, address, context)
        expression = _ranked_by_identity(repoint(split.lifted[address]), identity)
        output = arbitrary_to_concept(
            expression,
            context.environment,
            name=f"_{scope}_lift_{source.name}",
            metadata=source.metadata,
        )
        context.add_select_concept(output)
        refs[address] = output.reference
        labels[output.address] = item.concept
        selection.append(
            SelectItem(
                content=ConceptTransform(function=expression, output=output),
                modifiers=modifiers,
            )
        )
    condition = repoint(split.having) if split.having is not None else None
    # the relation's whole width, so a read of it never dedups its identity
    shown = {item.concept.address for item in selection}
    selection += [
        SelectItem(content=ref, modifiers=[Modifier.HIDDEN])
        for address, ref in refs.items()
        if address in handles and ref.address not in shown
    ]
    return SelectStatement(
        selection=selection,
        having_clause=HavingClause(conditional=condition) if condition else None,
        order_by=(
            OrderBy(
                items=[
                    OrderItem(expr=repoint(o.expr), order=o.order)
                    for o in split.order_by.items
                ]
            )
            if split.order_by
            else None
        ),
        limit=split.limit,
        meta=authored.meta,
        labels=labels,
    )
