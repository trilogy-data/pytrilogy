"""What one plan's statement references, recorded on the build environment it
plans in, plus the ``~`` heal and partition hiding its WHERE licenses."""

from __future__ import annotations

from trilogy.core.models.author import (
    Concept,
    HavingClause,
    MultiSelectLineage,
    OrderBy,
    SelectLineage,
    WhereClause,
)
from trilogy.core.models.build import BuildMultiSelectLineage, BuildSelectLineage
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.partial_bridging import (
    decide_exclusion,
    decide_heal,
    gate_excluded_enum_values,
)
from trilogy.core.processing.v4_helper.keyspace import build_datasources
from trilogy.core.processing.v4_helper.projection import statement_filter_population
from trilogy.core.processing.v4_helper.staged_where import universal_row_bound


def authored_reference_addresses(
    statement: SelectLineage | MultiSelectLineage,
    environment: Environment,
    include_where: bool = True,
) -> set[str]:
    """Transitive closure of author-referenced concept addresses for this
    select: outputs, WHERE/HAVING/ORDER BY arguments, and their lineage,
    walked on author objects before scoped-join canonical substitution
    rewrites addresses. Scoped-join declarations are excluded: a declared
    relation whose far side the author never references is domain metadata
    and must not force that side into the plan. `include_where=False` drops
    the WHERE clauses; the outputs-only closure distinguishes row-stream
    contributors from population-scope (condition) references."""
    selects = (
        statement.selects if isinstance(statement, MultiSelectLineage) else [statement]
    )
    stack: list[str] = []
    locals_pool: dict[str, Concept] = {}
    clauses: list[WhereClause | HavingClause | OrderBy | None] = [
        statement.having_clause,
        statement.order_by,
    ]
    if include_where:
        clauses.append(statement.where_clause)
    for select in selects:
        stack.extend(ref.address for ref in select.output_components)
        clauses.extend([select.having_clause, select.order_by])
        if include_where:
            clauses.append(select.where_clause)
        locals_pool.update(select.local_concepts)
    for clause in clauses:
        if clause is not None:
            stack.extend(ref.address for ref in clause.concept_arguments)
    closure: set[str] = set()
    while stack:
        address = stack.pop()
        if address in closure:
            continue
        closure.add(address)
        concept = locals_pool.get(address) or environment.concepts.get(address)
        if concept is None:
            continue
        stack.extend(ref.address for ref in concept.concept_arguments)
    return closure


def scope_statement(
    build_environment: BuildEnvironment,
    statement: SelectLineage | MultiSelectLineage,
    environment: Environment,
    build_statement: BuildSelectLineage | BuildMultiSelectLineage,
) -> None:
    """Make ``build_environment`` this one plan's: record what the select
    references, heal the ``~`` bindings its WHERE completes, hide the
    partitions its row bound contradicts. Runs before the reference graph is
    generated, at every seam that materializes a build environment for a plan.

    Staged (``then where``) chains are not healed: intermediate stages see
    populations the combined WHERE has not yet filtered. A statement showing
    nothing but filter values over one predicate is filtered by it
    (``statement_filter_population``), the same as by a WHERE.

    Copy-on-write: healed datasources replace the authored ones in the
    environment's per-statement mapping and excluded ones leave it; the shared
    build-cache objects are never mutated.
    """
    build_environment.statement_authored_addresses = authored_reference_addresses(
        statement, environment
    )
    build_environment.statement_output_addresses = authored_reference_addresses(
        statement, environment, include_where=False
    )
    build_environment.statement_hidden_addresses = set(
        build_statement.hidden_components
    )
    if not isinstance(build_statement, BuildSelectLineage):
        return
    if not build_statement.where_clauses:
        outputs = list(build_statement.output_components)
        replacements = decide_heal(
            build_environment,
            build_datasources(build_environment),
            outputs,
            [
                clause
                for clause in (
                    build_statement.where_clause,
                    statement_filter_population(
                        outputs, build_statement.hidden_components
                    ),
                )
                if clause is not None
            ],
        )
        for name, existing in list(build_environment.datasources.items()):
            if existing.identifier in replacements:
                build_environment.datasources[name] = replacements[existing.identifier]
    bound = universal_row_bound(
        build_statement.where_clauses, build_statement.where_clause
    )
    if bound is None:
        return
    build_environment.excluded_enum_values = gate_excluded_enum_values(
        build_environment, bound
    )
    excluded = decide_exclusion(build_datasources(build_environment), bound)
    for name, existing in list(build_environment.datasources.items()):
        if existing.identifier in excluded:
            del build_environment.datasources[name]
