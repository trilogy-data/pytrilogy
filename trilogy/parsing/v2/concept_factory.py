"""v2-native concept factory helpers.

Wraps the equivalent functions from ``trilogy.parsing.common`` with an API
that takes a ``RuleContext`` instead of an ``Environment`` directly. This
defines the single migration boundary where v2 rule code meets the
v1-shaped concept builders.

The underlying v1 helpers still read the environment's concept dict to
resolve ``ConceptRef`` arguments. Pending concepts are never written to
the environment at ``add`` time; ``SemanticState.pending_overlay_scope``
installs a ``MappingProxyType`` overlay on the env concept dict for the
duration of hydrate/validate/commit. The overlay is strictly read-only:
the underlying store is never mutated, and the overlay is popped on
scope exit (success or failure), so parses exercise these wrappers
without any persistent parse-time environment mutation.

None of these wrappers call ``environment.add_concept`` directly; the
helpers they wrap only read from the environment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from trilogy.core.models.author import (
    AlignClause,
    AlignItem,
    Concept,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    WhereClause,
)
from trilogy.core.statements.author import SelectStatement
from trilogy.parsing.common import (
    align_item_to_concept,
    arbitrary_to_concept,
    derive_item_to_concept,
    unwrap_transformation,
)

if TYPE_CHECKING:
    from trilogy.parsing.v2.rules_context import RuleContext


def arbitrary_to_concept_v2(
    parent,
    context: "RuleContext",
    namespace: str | None = None,
    name: str | None = None,
    metadata: Metadata | None = None,
) -> Concept:
    """v2 wrapper for ``arbitrary_to_concept``.

    Pending concepts are resolved through the pending-overlay scope owned
    by ``SemanticState`` — v1 helpers see them as if they were in the
    environment's concept dict, without any mutation.
    """
    return arbitrary_to_concept(
        parent,
        environment=context.environment,
        namespace=namespace,
        name=name,
        metadata=metadata,
    )


def unwrap_transformation_v2(input, context: "RuleContext"):
    """v2 wrapper for ``unwrap_transformation``."""
    return unwrap_transformation(input, context.environment)


def align_item_to_concept_v2(
    parent: AlignItem,
    align_clause: AlignClause,
    selects: list[SelectStatement],
    context: "RuleContext",
    where: WhereClause | None = None,
    having: HavingClause | None = None,
    limit: int | None = None,
) -> Concept:
    """v2 wrapper for ``align_item_to_concept``.

    The v1 helper calls ``SelectStatement.as_lineage(environment)`` which
    reads from the environment. Those reads are served by the pending
    overlay scope installed by ``SemanticState`` — read-only, no store
    mutation.
    """
    return align_item_to_concept(
        parent,
        align_clause,
        selects,
        environment=context.environment,
        where=where,
        having=having,
        limit=limit,
    )


def derive_item_to_concept_v2(
    parent,
    name: str,
    lineage: MultiSelectLineage,
    context: "RuleContext",
) -> Concept:
    """v2 wrapper for ``derive_item_to_concept``.

    Only consults ``context.environment.namespace``; no concept reads.
    """
    return derive_item_to_concept(
        parent,
        name,
        lineage,
        namespace=context.environment.namespace,
    )
