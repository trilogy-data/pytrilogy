"""v2-native concept factory helpers.

Wraps the equivalent functions from ``trilogy.parsing.common`` with an API
that takes a ``RuleContext`` instead of an ``Environment`` directly. This
defines the migration boundary between parse-time helper calls that must
eventually stop relying on ``Environment`` mutation and reads, and the
remaining v1 helpers used in execution-time paths.

Today these wrappers still call through to the v1 helpers, which read
``environment.concepts`` (currently kept in sync via the SemanticState
mirror). The wrappers exist so that once the mirror is removed, the
remaining environment-coupling lives in exactly one place and can be
migrated helper-by-helper.

None of these wrappers call ``environment.add_concept``; the helpers they
wrap only read from ``environment.concepts``.
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

    Reads concepts through ``context.environment`` today; the mirror keeps
    pending concepts visible. When the mirror is removed, this wrapper is
    the migration point to a context-native implementation.
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
    reads ``environment.concepts``. This remains a dependency to resolve
    as part of the mirror-removal follow-up.
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
