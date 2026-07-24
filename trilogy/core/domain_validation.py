"""Opt-in validation of declared domain relations against data.

A `merge a into ~b` declares a SUBSET domain (a ⊆ b); a non-partial
`merge a into b` declares EQUAL domains (mutual subset). The planner TRUSTS
these declarations — narrowing a preserving join on a declared key drops the
declaration-violating rows (docs/subset_union_join_design.md), so a lying
declaration is an author error the engine will not paper over. This module
generates the cheap data checks that surface such lies: one COUNT query per
declared containment, counting non-null source values absent from the target
(NULL is not a value — nullability never violates a domain declaration).

The checks must run on an executor whose environment does NOT carry the
declarations being validated: an active merge collapses the two concepts onto
one canonical, making the containment query self-referential. Read the
declarations from the authored environment (``declared_domain_relations``),
then execute the checks against a clean parse of the same models.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from trilogy.core.domain_graph import (
    DomainGraph,
    EdgeScope,
)
from trilogy.core.domain_graph import (
    DomainRelation as DomainRelationKind,
)

if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment
    from trilogy.executor import Executor


@dataclass(frozen=True)
class DomainRelation:
    source: str  # the declared SUBSET side for "subset"; checked source ⊆ target
    target: str
    declaration: str  # "subset" | "equal"


@dataclass(frozen=True)
class DomainCheck:
    source: str
    target: str
    declaration: str
    query: str


@dataclass(frozen=True)
class DomainViolation:
    check: DomainCheck
    violating_values: int


def declared_domain_relations(environment: Environment) -> list[DomainRelation]:
    """Declared relations from the domain graph's edges: a SUBSET edge is
    stored subset-side-first (source ⊑ target), so the containment check
    direction is correct by construction. (The former merge-tuple reading
    checked the REVERSED direction for subsets — merge tuples store the
    anchor first — and the adversarial proof data, carrying one exclusive
    value per side, could not tell the difference.)"""
    graph = DomainGraph.from_scoped_joins(
        [(merge, EdgeScope.GLOBAL) for merge in environment.merges]
    )
    out: list[DomainRelation] = []
    for edge in graph.edges:
        if edge.relation is DomainRelationKind.SUBSET:
            out.append(DomainRelation(edge.source, edge.target, "subset"))
        elif edge.relation is DomainRelationKind.EQUAL:
            out.append(DomainRelation(edge.source, edge.target, "equal"))
    return out


def _containment_query(sub: str, sup: str) -> str:
    return (
        f"SELECT count_distinct({sub}) -> violation_count "
        f"WHERE {sub} is not null and {sub} not in ({sup});"
    )


def generate_domain_checks(
    relations: list[DomainRelation],
) -> list[DomainCheck]:
    """One containment check per declared direction: a SUBSET declaration
    checks source ⊆ target; an EQUAL declaration checks both directions."""
    checks: list[DomainCheck] = []
    for relation in relations:
        checks.append(
            DomainCheck(
                relation.source,
                relation.target,
                relation.declaration,
                _containment_query(relation.source, relation.target),
            )
        )
        if relation.declaration == "equal":
            checks.append(
                DomainCheck(
                    relation.target,
                    relation.source,
                    relation.declaration,
                    _containment_query(relation.target, relation.source),
                )
            )
    return checks


def validate_domains(
    clean_executor: Executor,
    relations: list[DomainRelation],
) -> list[DomainViolation]:
    """Run every declared-domain check and return the violations found.

    ``clean_executor`` must NOT have the validated merges active (see module
    docstring); ``relations`` typically come from
    ``declared_domain_relations`` over the authored environment."""
    violations: list[DomainViolation] = []
    for check in generate_domain_checks(relations):
        rows = clean_executor.execute_text(check.query)[-1].fetchall()
        count = rows[0][0] if rows else 0
        if count:
            violations.append(DomainViolation(check=check, violating_values=count))
    return violations
