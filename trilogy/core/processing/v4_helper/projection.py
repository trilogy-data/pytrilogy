"""Shared v4 helpers for projection widening and lineage satisfiability."""

from collections.abc import Collection, Iterable, Iterator, Mapping
from types import MappingProxyType

from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConceptArgs,
    BuildFilterItem,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
)
from trilogy.core.processing.condition_utility import is_scalar_condition
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.nodes.select_node_v2 import RowsetNode

from .constants import ROW_SHAPE_BARRIER_DERIVATIONS, ROW_STREAM_DERIVATIONS
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


def renders_at(node: StrategyNode, concept: BuildConcept, available: set[str]) -> bool:
    """`concept` renders on `node` from `available`. A handle is a leaf there,
    except on its own rowset's boundary, which computes it off its content.
    The boundary vouches for its handles, not their addresses: a `union join`
    mate still pairs as authored, never as a pseudonym of one."""
    if not isinstance(node, RowsetNode):
        return concept_satisfiable(concept, available)
    handles = boundary_handles(_bounded_handles(node), available)
    return concept_satisfiable(concept, available, cache=dict.fromkeys(handles, True))


def _bounded_handles(node: RowsetNode) -> list[BuildConcept]:
    """Every handle of the rowsets `node` is the boundary of."""
    concepts = node.environment.concepts
    rowsets = {
        o.lineage.rowset.name: o.lineage.rowset
        for o in node.output_concepts
        if isinstance(o.lineage, BuildRowsetItem)
    }
    return [
        concepts[address]
        for rowset in rowsets.values()
        for address in rowset.derived_concepts
        if address in concepts
    ]


def boundary_handles(handles: Iterable[BuildConcept], available: set[str]) -> set[str]:
    """The handles a rowset's own boundary renders from `available`: there a
    handle is computed off its content. Anywhere else it is a leaf
    (`row_lineage_arguments`), whose value exists only on its boundary."""
    return {
        h.address
        for h in handles
        if isinstance(h.lineage, BuildRowsetItem)
        and concept_satisfiable(h.lineage.content, available)
    }


def lineage_existence_parts(concept: BuildConcept) -> tuple[set[str], set[str]]:
    """The existence args of the concept's lineage, and the addresses its row
    reads. Two shapes: a FILTER (its where, plus the filtered content), and a
    membership comparison authored as a derived/projected boolean (`auto flag
    <- a in b`, `(20, 1) in (pairs.val, pairs.cat) as present`) whose lineage
    IS (or propagates from) the SubselectComparison."""
    args: BuildConceptArgs
    row_read: set[str] = set()
    if isinstance(concept.lineage, BuildFilterItem):
        args = concept.lineage.where
        row_read = {c.address for c in concept.lineage.content_concept_arguments}
    elif isinstance(concept.lineage, BuildConceptArgs):
        args = concept.lineage
    else:
        return set(), set()
    existence = {ec.address for grp in (args.existence_arguments or []) for ec in grp}
    return existence, row_read | {r.address for r in args.row_arguments}


def lineage_existence_only(concept: BuildConcept) -> set[str]:
    """Addresses that appear ONLY as existence args in the concept's lineage (a
    semijoin RHS like `zips in substring(p_cust_zip,1,5)`). These feed a
    side-channel subselect, not the concept's row stream; one the row also
    reads (`dx ? x in dx`) is both."""
    existence, row_read = lineage_existence_parts(concept)
    return existence - row_read


def row_lineage_arguments(concept: BuildConcept) -> list[BuildConcept]:
    # a rowset handle is a leaf: its value is its body's rows, never computed
    # off another relation's (`boundary_handles` is the one exception)
    if concept.lineage is None or concept.derivation == Derivation.ROWSET:
        return []
    args = list(concept.lineage.concept_arguments)
    existence = lineage_existence_only(concept)
    if not existence:
        return args
    return [arg for arg in args if arg.address not in existence]


def iter_row_lineage(concept: BuildConcept) -> Iterator[BuildConcept]:
    """`concept` and its row-lineage ancestors, each address visited once."""
    seen: set[str] = set()
    stack = [concept]
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        yield current
        stack.extend(row_lineage_arguments(current))


def lineage_crosses_row_shape_barrier(concept: BuildConcept) -> bool:
    return any(
        c.derivation in ROW_SHAPE_BARRIER_DERIVATIONS for c in iter_row_lineage(concept)
    )


def inline_scalar(concept: BuildConcept) -> bool:
    """A per-row expression a scan computes off its own columns: BASIC all the
    way down. A BASIC wrapping a barrier (`substring((max(x) by k), 1, 200)`)
    is not; it takes the barrier's shape and has to be materialized like one."""
    return (
        concept.derivation == Derivation.BASIC
        and not lineage_crosses_row_shape_barrier(concept)
    )


def reads_rows_only(concept: BuildConcept) -> bool:
    """A row-stream derivation whose lineage never crosses an aggregate. One
    over an aggregate is evaluated on a ``~`` extension row (`count(...) > 0`
    is false there, not NULL); one over rows alone is NULL there. Windows and
    filters count: unlike `region_domains._null_propagating`, this asks what an
    extension row lacks, not what a row keyed on a NULL member computes."""
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


def origin_inputs_sourceable(origin: BuildConcept, available: set[str]) -> bool:
    """A merge origin's inputs render from `available`. The origin itself is in
    its merged key's class, so a scan of the key would vouch for it; judging the
    inputs keeps a derivation whose inputs are hidden or dropped out of play."""
    seen: dict[str, bool] = {}
    return origin.lineage is not None and all(
        concept_satisfiable(arg, available, cache=seen)
        for arg in origin.lineage.concept_arguments
    )


def literal_producible(concept: BuildConcept, _seen: set[str] | None = None) -> bool:
    """Renderable with no row parent at all: a constant, or a value whose whole
    lineage bottoms out in literals (`sum(1)`, a parameter, `unnest([1,2])`).

    Distinct from `concept_satisfiable`, which reads "no row arguments" as
    unsatisfiable because with parents present a lineage that reaches nothing
    is a dead end. Without parents that same shape is the ONLY thing that can
    still render, so it needs its own rule rather than a shared one."""
    if concept.derivation == Derivation.CONSTANT:
        return True
    if concept.lineage is None or concept.derivation in (
        Derivation.ROOT,
        Derivation.ROWSET,
    ):
        return False
    seen = _seen if _seen is not None else set()
    if concept.address in seen:
        return True
    seen.add(concept.address)
    return all(literal_producible(arg, seen) for arg in row_lineage_arguments(concept))


def satisfiable_outputs(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    partials: Mapping[str, BuildConcept] = MappingProxyType({}),
    boundary: bool = False,
) -> list[BuildConcept]:
    """What `outputs` render over `parents`; a rowset's `boundary` also
    computes its handles off their content."""
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
    # an aggregate re-aggregated from a partial renders from that partial
    reaggregated = {
        c.address
        for c in outputs
        if (partial := partials.get(c.address)) is not None
        and partial.address in available
    }
    if boundary:
        reaggregated |= boundary_handles(outputs, available)
    return satisfiable_from(outputs, available, reaggregated)


def satisfiable_from(
    outputs: list[BuildConcept],
    available: set[str],
    satisfied: Collection[str] = (),
) -> list[BuildConcept]:
    keep_addrs: set[str] = set(satisfied)
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
    if isinstance(node, RowsetNode):
        # the boundary computes its handles off their content, its inputs
        output_concepts = list(output_concepts)
        bounded = {h.address for h in _bounded_handles(node)}
        # a constant renders inline: no parent supplies it
        input_candidates = [
            content
            for c in [*input_candidates, *output_concepts]
            if c.derivation != Derivation.CONSTANT
            for content in (
                iter_row_lineage(c.lineage.content)
                if c.address in bounded and isinstance(c.lineage, BuildRowsetItem)
                else (c,)
            )
        ]
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
    new_outputs = [c for c in output_concepts if c.address not in out_addrs]
    if new_outputs:
        # the node keeps its null and partial stamps current for the columns
        node.add_output_concepts(new_outputs, rebuild=False, unhide=False)
        changed = True
    if changed and rebuild:
        node.rebuild_cache()
    return changed


def decided_at_output_grain(
    address: str, outputs: Iterable[BuildConcept], environment: BuildEnvironment
) -> bool:
    """Whether a WHERE reading `address` can be applied to the statement's
    final rows: every output that crosses an aggregate is grouped at a grain
    determining it, so the rows it rejects above the aggregate are the rows
    the aggregate's input would have lost. A launch-day filter under a
    per-month count is not: the count must see the filter. A ROOT column
    crosses no aggregate, whatever entity it is keyed on (`city`)."""
    for concept in outputs:
        if concept.derivation == Derivation.ROOT or reads_rows_only(concept):
            continue
        grain = frozenset(concept.grain.components) if concept.grain else frozenset()
        if not grain or not build_fd_determines(environment, grain, address):
            return False
    return True


def _reads_a_set(where: BuildWhereClause) -> bool:
    """A REAL membership (`x in <other column/select>`), whose existence side
    carries concepts. A literal IN-list (`month in (1,2,3,4)`) is also
    modeled as a subselect comparison but has none: a plain scalar predicate."""
    return any(arg for tup in (where.existence_arguments or ()) for arg in tup)


def shared_filter_predicate(
    concepts: list[BuildConcept], sets: bool = False
) -> BuildWhereClause | None:
    """The one predicate every filter concept among `concepts` is gated on, or
    None. Distinct predicates are fused conditional columns (`price ? channel =
    'STORE'`, `price ? channel = 'WEB'`), each its own CASE over the shared
    scan: AND-ing them into one WHERE would null out every row. A predicate
    reading a set is one only under `sets`: the statement's own filter values,
    whose node the set is wired to either way
    (`Filters.alone_keeps_passing_rows`)."""
    distinct: dict[str, BuildWhereClause] = {}
    for c in concepts:
        if isinstance(c.lineage, BuildFilterItem):
            distinct.setdefault(str(c.lineage.where.conditional), c.lineage.where)
    if len(distinct) != 1:
        return None
    where = next(iter(distinct.values()))
    return None if _reads_a_set(where) and not sets else where


def filter_row_predicate(
    outputs: list[BuildConcept],
    parent_outputs: set[str],
    may_narrow: bool,
    sets: bool = False,
) -> BoolExpr | None:
    """The predicate a filter group's node takes into its WHERE, when its rows
    may narrow: scalar, or over aggregates its parents already emit."""
    where = shared_filter_predicate(outputs, sets) if may_narrow else None
    if where is None:
        return None
    agg_args = [r for r in where.row_arguments if r.derivation == Derivation.AGGREGATE]
    if is_scalar_condition(where.conditional) or (
        agg_args and all(r.address in parent_outputs for r in where.row_arguments)
    ):
        return where.conditional
    return None


def statement_filter_population(
    mandatory_list: list[BuildConcept],
    hidden: set[str] | None,
    sets: bool = False,
) -> BuildWhereClause | None:
    """When every output a statement shows is a filter value over one
    predicate, a NULL row is one nothing would keep: `gen_filter` pushes the
    predicate into its WHERE, and the keyspace and pin-heal read it as the
    statement's own, so a region those rows are absent on is emptied and the
    `~` it would pad for is healed, never padded back. A hidden output (a
    HAVING's aggregate promoted to the projection) is not shown: it is
    evaluated over the rows the shown values keep."""
    shown = [c for c in mandatory_list if not hidden or c.address not in hidden]
    if not all(isinstance(c.lineage, BuildFilterItem) for c in shown):
        return None
    return shared_filter_predicate(shown, sets)


def rollup_padded_keys(environment: BuildEnvironment) -> frozenset[str]:
    """Grouping keys of the select's ROLLUP/CUBE/GROUPING SETS pass.
    The subtotal rows NULL these, so they are not a row identity and must never
    be volunteered as a join axis: pairing on one drops every subtotal row (a
    rolled-up NULL matches nothing). The join resolver applies the same rule
    per-datasource via `rollup_padded_addresses`; this is the environment-wide
    view the demand pass needs before any datasource exists."""
    grouping = environment.grouping
    return frozenset(grouping.keys) if grouping is not None else frozenset()


def reads_a_rollup(address: str, environment: BuildEnvironment) -> bool:
    """Whether the concept at `address` is computed on the rows of a
    ROLLUP/CUBE/GROUPING SETS pass: it reads one of the pass's aggregates,
    directly or through scalars. Those rows are subtotals of whatever entered
    below the pass, so no region is absent on them."""
    seen: set[str] = set()
    stack = [address]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        concept = environment.concepts.get(current)
        if concept is None or concept.lineage is None:
            continue
        for arg in concept.lineage.concept_arguments:
            if environment.pass_of(arg) is not None:
                return True
            stack.append(arg.address)
    return False
