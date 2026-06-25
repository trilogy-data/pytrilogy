"""Shared v4 helpers for projection widening and lineage satisfiability."""

from collections.abc import Iterable

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildFilterItem
from trilogy.core.processing.nodes import StrategyNode


def parent_output_addresses(node: StrategyNode) -> set[str]:
    # A hidden parent output is dropped from that parent's CTE SELECT, so a
    # consumer cannot read it — exclude it from what's "available".
    return {
        output.address
        for parent in node.parents
        for output in parent.output_concepts
        if output.address not in parent.hidden_concepts
    }


def row_lineage_arguments(concept: BuildConcept) -> list[BuildConcept]:
    if concept.lineage is None:
        return []
    args = list(concept.lineage.concept_arguments)
    if not isinstance(concept.lineage, BuildFilterItem):
        return args
    existence = {
        ec.address
        for group in (concept.lineage.where.existence_arguments or ())
        for ec in group
    } - {row.address for row in concept.lineage.where.row_arguments}
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


def satisfiable_outputs(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
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
) -> bool:
    changed = False
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
    if changed:
        node.rebuild_cache()
    return changed
