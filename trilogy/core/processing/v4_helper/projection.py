"""Shared v4 helpers for projection widening and lineage satisfiability."""

from collections.abc import Iterable

from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildConceptArgs,
    BuildFilterItem,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
    resolve_rowset_content_address,
)
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode

from .constants import ROW_STREAM_DERIVATIONS
from .functional_dependency import build_fd_determines


def parent_output_addresses(node: StrategyNode) -> set[str]:
    # A hidden parent output is dropped from that parent's CTE SELECT, so a
    # consumer cannot read it; exclude it from what's "available".
    return {
        output.address
        for parent in node.parents
        for output in parent.output_concepts
        if output.address not in parent.hidden_concepts
    }


def renderable_addresses(node: StrategyNode) -> set[str]:
    """Addresses `node` can project: its parents' visible outputs plus, for a leaf
    scan, every column its datasource binds (a leaf has no parent nodes, so
    `parent_output_addresses` alone reports nothing). A union's columns are the
    stack of its arms', so it can render whatever EVERY arm can (the
    all-or-nothing rule `widen_projection` applies when widening one)."""
    available = parent_output_addresses(node)
    if isinstance(node, SelectNode) and node.datasource is not None:
        available |= {c.address for c in node.datasource.output_concepts}
    if isinstance(node, UnionNode) and node.parents:
        available |= set.intersection(
            *(renderable_addresses(arm) for arm in node.parents)
        )
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


def reads_rows_only(concept: BuildConcept) -> bool:
    """A row-stream derivation whose lineage never crosses an aggregate. One
    over an aggregate is evaluated on a ``~`` extension row (`count(...) > 0`
    is false there, not NULL); one over rows alone is NULL there."""
    if concept.derivation not in ROW_STREAM_DERIVATIONS or concept.lineage is None:
        return False
    return all(
        arg.derivation in (Derivation.ROOT, Derivation.CONSTANT) or reads_rows_only(arg)
        for arg in concept.lineage.concept_arguments
    )


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
    # marker), never sourced from a row parent, so it is always satisfiable even
    # when its standalone constant scan is dropped.
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

    `rebuild=False` defers the resolve to the caller; only safe while nothing
    resolves the node (or a descendant of it) before that rebuild lands."""
    changed = False
    # A union's columns are the STACK of its arms' columns; widening the union
    # alone claims a column no arm produces, and the renderer's union escape
    # hatch emits it as a bare reference rather than raising. Every arm has to
    # compute it from its own scan, so this is all-or-nothing: one arm that
    # cannot render it means the union cannot carry it at all.
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
        # The arms compute the column from THEIR inputs; the union itself only
        # reads what the arms now stack. Its inputs are the widened outputs,
        # never the arms' source columns.
        input_candidates = arm_outputs
        available_addresses = parent_output_addresses(node)
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


def output_rowset_base_keys(
    mandatory_list: list[BuildConcept], environment: BuildEnvironment
) -> set[str]:
    """Base addresses the grain keys of the output rowset boundaries unwrap to.

    A boundary over `select oid, amt` is grained on `rs.oid`, which unwraps to
    `local.oid`. The boundary can expose that base column beneath its handle, so
    a scan keyed by it pairs with the boundary on a real key instead of
    cross-joining."""
    keys: set[str] = set()
    for concept in mandatory_list:
        if not isinstance(concept.lineage, BuildRowsetItem) or concept.grain is None:
            continue
        for component in concept.grain.components:
            resolved = resolve_rowset_content_address(component, environment)
            if resolved != component:
                keys.add(resolved)
    return keys


def decided_at_output_grain(
    address: str, outputs: Iterable[BuildConcept], environment: BuildEnvironment
) -> bool:
    """Whether a WHERE reading `address` can be applied to the statement's
    final rows: every output that crosses an aggregate is grouped at a grain
    determining it, so the rows it rejects above the aggregate are the rows
    the aggregate's input would have lost. A launch-day filter under a
    per-month count is not: the count must see the filter."""
    for concept in outputs:
        if reads_rows_only(concept):
            continue
        grain = frozenset(concept.grain.components) if concept.grain else frozenset()
        if not grain or not build_fd_determines(environment, grain, address):
            return False
    return True


def _has_concept_existence(where: BuildWhereClause) -> bool:
    """True only for a REAL subselect arg (`x in <other column/select>`), one
    whose existence side carries concepts. A literal IN-list (`month in (1,2,3,4)`)
    is also modeled as a subselect comparison but has no existence concepts, so it
    is a plain scalar predicate safe to push into a WHERE."""
    return any(arg for tup in (where.existence_arguments or ()) for arg in tup)


def shared_filter_predicate(concepts: list[BuildConcept]) -> BuildWhereClause | None:
    """The one predicate every filter concept among `concepts` is gated on, or
    None. Distinct predicates are fused conditional columns (`price ? channel =
    'STORE'`, `price ? channel = 'WEB'`), each its own CASE over the shared
    scan: AND-ing them into one WHERE would null out every row. A predicate
    with an existence arg needs its subselect source wired as a side parent,
    which no WHERE push does."""
    distinct: dict[str, BuildWhereClause] = {}
    for c in concepts:
        if isinstance(c.lineage, BuildFilterItem):
            distinct.setdefault(str(c.lineage.where.conditional), c.lineage.where)
    if len(distinct) != 1:
        return None
    where = next(iter(distinct.values()))
    return None if _has_concept_existence(where) else where


def statement_filter_population(
    mandatory_list: list[BuildConcept],
) -> BuildWhereClause | None:
    """When every output a statement shows is a filter value over one
    predicate, a NULL row is one nothing would keep: `gen_filter` pushes the
    predicate into its WHERE, and the keyspace and pin-heal read it as the
    statement's own, so a region those rows are absent on is emptied and the
    `~` it would pad for is healed, never padded back."""
    if not all(isinstance(c.lineage, BuildFilterItem) for c in mandatory_list):
        return None
    return shared_filter_predicate(mandatory_list)
