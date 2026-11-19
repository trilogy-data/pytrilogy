"""A plan's keyspace, with every rowset it reads as a witness
(`keyspace.RowsetWitness`).

A rowset's rows are its body's regions. The body's keyspace is computed here
the way the body's own plan will compute it (built in its own scope, healed,
its WHERE and filter population applied), then respelled in the handles the
outer plan reads. It is a fact of the rowset, cached on the history.
"""

from trilogy.core.graph_models import ScopeDatasources
from trilogy.core.models.author import SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace
from trilogy.core.processing.v4_helper.constants import NO_WITNESS_FLOOR
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.core.processing.v4_helper.keyspace import (
    RowsetWitness,
    build_keyspace,
    rowset_witness,
)
from trilogy.core.processing.v4_helper.models import (
    ConceptAttrs,
    PlanFront,
    PlanFrontKey,
)
from trilogy.core.processing.v4_helper.projection import statement_filter_population

from .nested_select import body_mandatory, build_nested_select


def statement_keyspace(
    concept_attrs: dict[str, ConceptAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    history: V4History,
    scope: ScopeDatasources,
) -> Keyspace:
    """One plan's row universe. A statement showing only filter values over
    one predicate is filtered by it, so the keyspace empties the regions it
    rejects; only the keyspace sees it (as a plan WHERE it would narrow the
    aggregates the predicate reads). Only what the STATEMENT projects asks for
    a region's rows: a sub-plan (a condition's aggregate feeder, `sum(...) by
    part.id`) lists its grain keys as outputs, but those are the axis it joins
    back on, not rows."""
    population = statement_filter_population(
        mandatory_list, environment.statement_hidden_addresses
    )
    if population is not None:
        conditions = conditions + [population]
    statement_outputs = environment.statement_output_addresses
    outputs = [
        c
        for c in mandatory_list
        if statement_outputs is None or {c.address, *c.pseudonyms} & statement_outputs
    ]
    witnesses = rowset_witnesses(concept_attrs, environment, history)
    return build_keyspace(
        concept_attrs,
        outputs,
        environment,
        conditions,
        scope,
        rowset_witnesses=witnesses,
    )


def rowset_witnesses(
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    history: V4History,
) -> tuple[RowsetWitness, ...]:
    """Every rowset the plan reads a handle of, as a source of the plan."""
    lineages: dict[str, BuildRowsetLineage] = {}
    for attrs in concept_attrs.values():
        if attrs.rowset_name is None or attrs.rowset_name in lineages:
            continue
        concept = environment.concepts.get(attrs.address)
        if concept is not None and isinstance(concept.lineage, BuildRowsetItem):
            lineages[attrs.rowset_name] = concept.lineage.rowset
    out: list[RowsetWitness] = []
    for name in sorted(lineages):
        witness = history.read_rowset_witness(name)
        if witness is None:
            witness = _computed_witness(name, lineages[name], environment, history)
        out.append(witness)
    return tuple(out)


def _computed_witness(
    name: str,
    lineage: BuildRowsetLineage,
    environment: BuildEnvironment,
    history: V4History,
) -> RowsetWitness:
    # A body reading its own rowset (a membership over the rowset inside its
    # own body) cannot witness itself, so it reads an empty placeholder.
    # A result that read the placeholder of an enclosing computation
    # understates it and is not cached, nor is the body's plan front it was
    # read off: mutually recursive bodies would otherwise persist each other's
    # partial answer for the rest of the build. Every other result is, or a
    # chain of rowsets each reading an earlier one twice recomputes it once
    # per path. A raising `_witness` must not leave the placeholder standing
    # as the answer.
    depth = len(history.live_witnesses)
    outer_floor = history.witness_floor
    history.witness_floor = NO_WITNESS_FLOOR
    history.live_witnesses[name] = depth
    history.rowset_witnesses[name] = RowsetWitness(name=name, regions=())
    try:
        witness, front = _witness(lineage, environment, history)
    finally:
        floor = history.witness_floor
        history.witness_floor = min(outer_floor, floor)
        del history.live_witnesses[name]
        history.rowset_witnesses.pop(name, None)
    if floor >= depth:
        history.rowset_witnesses[name] = witness
        if front is not None:
            history.plan_fronts[front[0]] = front[1]
    return witness


def _witness(
    rowset: BuildRowsetLineage, environment: BuildEnvironment, history: V4History
) -> tuple[RowsetWitness, tuple[PlanFrontKey, PlanFront] | None]:
    """The body's keyspace, as its own plan's first attempt builds it (the
    plan takes the front built here). When that attempt builds nothing, the
    body's plan retries without its materialized roots (`_search_concepts`);
    the witness cannot, since it never plans, so a body whose summary source
    does not combine is witnessed over a concept graph its plan abandons."""
    from trilogy.core.processing.concept_strategies_v4 import (  # cycle
        first_materialized_roots,
        plan_front,
        plan_front_key,
    )

    # a multiselect body is planned arm by arm, with no keyspace of its own
    if not isinstance(rowset.select, SelectLineage):
        return RowsetWitness(name=rowset.name, regions=()), None
    built, env, where, graph = build_nested_select(
        rowset.select, history, exclude_derived=rowset.derived_concepts
    )
    assert isinstance(built, BuildSelectLineage)
    mandatory = body_mandatory(built, env)
    conditions = [where] if where else []
    staged = built.where_clauses or None
    roots = first_materialized_roots(mandatory, env, conditions, graph)
    front = plan_front(mandatory, env, graph, history, conditions, roots, staged)
    handles = [
        concept
        for address in rowset.derived_concepts
        if (concept := environment.concepts.get(address)) is not None
    ]
    witness = rowset_witness(rowset.name, handles, front.keyspace, env, graph.scope)
    key = plan_front_key(mandatory, env, conditions, roots, staged)
    return witness, (key, front)
