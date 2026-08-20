"""Shared v4 helpers for projection widening and lineage satisfiability."""

from collections.abc import Iterable

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildConceptArgs, BuildFilterItem
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode


def parent_output_addresses(node: StrategyNode) -> set[str]:
    # A hidden parent output is dropped from that parent's CTE SELECT, so a
    # consumer cannot read it — exclude it from what's "available".
    return {
        output.address
        for parent in node.parents
        for output in parent.output_concepts
        if output.address not in parent.hidden_concepts
    }


def renderable_addresses(node: StrategyNode) -> set[str]:
    """Addresses `node` can project: its parents' visible outputs plus, for a leaf
    scan, every column its datasource binds (a leaf has no parent nodes, so
    `parent_output_addresses` alone reports nothing)."""
    available = parent_output_addresses(node)
    if isinstance(node, SelectNode) and node.datasource is not None:
        available |= {c.address for c in node.datasource.output_concepts}
    return available


def lineage_existence_only(concept: BuildConcept) -> set[str]:
    """Addresses that appear ONLY as existence args in the concept's lineage (a
    semijoin RHS like `zips in substring(p_cust_zip,1,5)`). These feed a
    side-channel subselect, not the concept's row stream. Two shapes: a FILTER's
    where, and a membership comparison authored as a derived/projected boolean
    (`auto flag <- a in b`, `(20, 1) in (pairs.val, pairs.cat) as present`)
    whose lineage IS (or propagates from) the SubselectComparison."""
    args: BuildConceptArgs
    if isinstance(concept.lineage, BuildFilterItem):
        args = concept.lineage.where
    elif isinstance(concept.lineage, BuildConceptArgs):
        args = concept.lineage
    else:
        return set()
    existence = {ec.address for grp in (args.existence_arguments or []) for ec in grp}
    return existence - {r.address for r in args.row_arguments}


def row_lineage_arguments(concept: BuildConcept) -> list[BuildConcept]:
    if concept.lineage is None:
        return []
    args = list(concept.lineage.concept_arguments)
    existence = lineage_existence_only(concept)
    if not existence:
        return args
    return [arg for arg in args if arg.address not in existence]


def concept_satisfiable(
    concept: BuildConcept,
    available: set[str],
    keep_addrs: set[str] | None = None,
    cache: dict[str, bool] | None = None,
) -> bool:
    """Whether `concept` can render from available row inputs.

    Existence-only FILTER args are side-channel subselect inputs, so they are
    intentionally ignored for row-stream satisfiability.
    """
    keep = keep_addrs or set()
    seen = cache if cache is not None else {}
    if concept.address in available or concept.address in keep:
        return True
    # A constant is a literal rendered inline (e.g. the `by all_rows` grand-total
    # marker), never sourced from a row parent — always satisfiable. Without this,
    # dropping its standalone constant scan (a cross-joined `SELECT 1`) would make
    # an output whose grain references it (the `count() by all_rows`) look
    # unsatisfiable and get pruned.
    if concept.derivation == Derivation.CONSTANT:
        return True
    # A merged/struct concept can be available under a pseudonym address (e.g.
    # the unnest exposes `local.unnest_array`, the attr-access arg is its merge
    # alias `local.wrapper`); they name the same column, so either satisfies.
    if any(p in available or p in keep for p in concept.pseudonyms):
        return True
    if concept.address in seen:
        return seen[concept.address]
    args = row_lineage_arguments(concept)
    if not args:
        seen[concept.address] = False
        return False
    seen[concept.address] = False
    result = all(concept_satisfiable(arg, available, keep, seen) for arg in args)
    seen[concept.address] = result
    return result


def literal_producible(concept: BuildConcept, _seen: set[str] | None = None) -> bool:
    """Renderable with no row parent at all: a constant, or a value whose whole
    lineage bottoms out in literals (`sum(1)`, a parameter, `unnest([1,2])`).

    Distinct from `concept_satisfiable`, which reads "no row arguments" as
    unsatisfiable because with parents present a lineage that reaches nothing
    is a dead end. Without parents that same shape is the ONLY thing that can
    still render, so it needs its own rule rather than a shared one."""
    if concept.derivation == Derivation.CONSTANT:
        return True
    if concept.lineage is None or concept.derivation == Derivation.ROOT:
        return False
    seen = _seen if _seen is not None else set()
    if concept.address in seen:
        return True
    seen.add(concept.address)
    return all(literal_producible(arg, seen) for arg in row_lineage_arguments(concept))


def satisfiable_outputs(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
    # A parentless group keeps its outputs here even when it cannot source them.
    # That looks wrong, but it is load-bearing: the bogus node has to survive
    # long enough for the post-assembly checks (`_has_unsourced_leaf`) and the
    # disconnected-subgraph diagnostics to run on the assembled tree and report
    # WHICH concepts split. Pruning it here instead leaves a partial plan that
    # renders INVALID_REFERENCE_BUG for the condition args nothing produces.
    if not parents:
        return outputs
    available = {
        output.address for parent in parents for output in parent.output_concepts
    }
    keep_addrs: set[str] = set()
    changed = True
    while changed:
        changed = False
        for concept in outputs:
            if concept.address in keep_addrs:
                continue
            if concept_satisfiable(concept, available, keep_addrs):
                keep_addrs.add(concept.address)
                changed = True
    return [concept for concept in outputs if concept.address in keep_addrs]


def widen_projection(
    node: StrategyNode,
    output_concepts: Iterable[BuildConcept],
    *,
    input_candidates: Iterable[BuildConcept] = (),
    available_addresses: set[str] | None = None,
    rebuild: bool = True,
) -> bool:
    """Widen `node`'s projection in place; returns whether anything changed.

    `rebuild=False` defers the resolve to the caller — only safe while nothing
    resolves the node (or a descendant of it) before that rebuild lands."""
    changed = False
    # A union's columns are the STACK of its arms' columns; widening the union
    # alone claims a column no arm produces, and the renderer's union escape
    # hatch emits it as a bare reference rather than raising (a phantom
    # `"cheerful"."_virt_filter_*"` shipped to the db). Every arm has to compute
    # it from its own scan, so this is all-or-nothing: one arm that cannot
    # render it means the union cannot carry it at all.
    if isinstance(node, UnionNode) and node.parents:
        arm_candidates = list(input_candidates)
        arm_outputs = list(output_concepts)
        arm_available = [renderable_addresses(arm) for arm in node.parents]
        for arm, available in zip(node.parents, arm_available):
            arm_addrs = {concept.address for concept in arm.output_concepts}
            if not all(
                concept.address in arm_addrs or concept_satisfiable(concept, available)
                for concept in arm_outputs
            ):
                return False
        for arm, available in zip(node.parents, arm_available):
            changed |= widen_projection(
                arm,
                arm_outputs,
                input_candidates=arm_candidates,
                available_addresses=available,
                rebuild=rebuild,
            )
    in_addrs = {concept.address for concept in node.input_concepts}
    out_addrs = {concept.address for concept in node.output_concepts}
    for concept in input_candidates:
        if (
            available_addresses is not None
            and concept.address not in available_addresses
        ):
            continue
        if concept.address not in in_addrs:
            node.input_concepts.append(concept)
            in_addrs.add(concept.address)
            changed = True
    for concept in output_concepts:
        if concept.address not in out_addrs:
            node.output_concepts.append(concept)
            out_addrs.add(concept.address)
            changed = True
    if changed and rebuild:
        node.rebuild_cache()
    return changed
