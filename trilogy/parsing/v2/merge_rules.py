from __future__ import annotations

from trilogy.core.enums import Modifier
from trilogy.core.models.author import Concept, UndefinedConcept
from trilogy.core.statements.author import MergeStatementV2
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


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
            v for v in context.concepts.values() if v.namespace == source_wildcard
        ]
        targets: dict[str, Concept] = {}
        for x in sources:
            taddr = target_wildcard + "." + x.name
            if context.concepts.contains(taddr):
                targets[x.address] = context.concepts.require(taddr)
        sources = [x for x in sources if x.address in targets]
    else:
        sources = [context.concepts.require(source_str)]
        targets = {sources[0].address: context.concepts.require(target_str)}

    for source_c in sources:
        if isinstance(source_c, UndefinedConcept):
            raise fail(
                node, f"Cannot merge non-existent source concept {source_c.address}"
            )

    return MergeStatementV2(
        sources=sources,
        targets=targets,
        modifiers=modifiers,
        source_wildcard=source_wildcard,
        target_wildcard=target_wildcard,
    )


MERGE_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.MERGE_STATEMENT: merge_statement,
}
