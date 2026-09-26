"""A plan's keyspace, with every rowset it reads as a witness
(`keyspace.RowsetWitness`, docs/keyspace_phase_plan.md).

A rowset's rows are its body's regions. The body's keyspace is computed here
the way the body's own plan will compute it (built in its own scope, healed,
its WHERE and filter population applied), then respelled in the handles the
outer plan reads. It is a fact of the rowset, cached on the history.
"""

from trilogy.core.models.author import SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.core.processing.v4_helper.keyspace import (
    RowsetWitness,
    build_keyspace,
    rowset_witness,
)
from trilogy.core.processing.v4_helper.keyspace_audit import audit_heal_keyspace
from trilogy.core.processing.v4_helper.models import ConceptAttrs, Keyspace
from trilogy.core.processing.v4_helper.projection import statement_filter_population

from .nested_select import build_nested_select


def statement_keyspace(
    concept_attrs: dict[str, ConceptAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    history: V4History,
) -> Keyspace:
    """One plan's row universe. A statement showing only filter values over
    one predicate is filtered by it, so the keyspace empties the regions it
    rejects; only the keyspace sees it (as a plan WHERE it would narrow the
    aggregates the predicate reads). Only what the STATEMENT projects asks for
    a region's rows: a sub-plan (a condition's aggregate feeder, `sum(...) by
    part.id`) lists its grain keys as outputs, but those are the axis it joins
    back on, not rows."""
    population = statement_filter_population(mandatory_list)
    if population is not None:
        conditions = conditions + [population]
    statement_outputs = environment.statement_output_addresses
    outputs = [
        c
        for c in mandatory_list
        if statement_outputs is None or {c.address, *c.pseudonyms} & statement_outputs
    ]
    witnesses = rowset_witnesses(concept_attrs, environment, history)
    keyspace = build_keyspace(
        concept_attrs, outputs, environment, conditions, rowset_witnesses=witnesses
    )
    audit_heal_keyspace(
        keyspace, concept_attrs, outputs, environment, conditions, witnesses
    )
    return keyspace


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
        witness = history.rowset_witnesses.get(name)
        if witness is None:
            witness = _witness(lineages[name], environment, history)
            history.rowset_witnesses[name] = witness
        out.append(witness)
    return tuple(out)


def _witness(
    rowset: BuildRowsetLineage, environment: BuildEnvironment, history: V4History
) -> RowsetWitness:
    from trilogy.core.processing.concept_strategies_v4 import (  # cycle
        _materialized_root_addresses,
    )

    # a multiselect body is planned arm by arm, with no keyspace of its own
    if not isinstance(rowset.select, SelectLineage):
        return RowsetWitness(name=rowset.name, regions=())
    built, env, where = build_nested_select(
        rowset.select, history, exclude_derived=rowset.derived_concepts
    )
    assert isinstance(built, BuildSelectLineage)
    outputs = list(built.output_components)
    conditions = [where] if where else []
    _, attrs, _ = build_concept_graph(
        outputs,
        env,
        conditions,
        _materialized_root_addresses(outputs, env, conditions),
        staged_conditions=built.where_clauses or None,
    )
    body = statement_keyspace(attrs, outputs, env, conditions, history)
    handles = [
        concept
        for address in rowset.derived_concepts
        if (concept := environment.concepts.get(address)) is not None
    ]
    return rowset_witness(rowset.name, handles, body, env)
