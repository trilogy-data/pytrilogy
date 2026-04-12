"""v2-native concept factory helpers.

Wraps the equivalent functions from ``trilogy.parsing.common`` with an API
that takes a ``RuleContext`` instead of an ``Environment`` directly. This
defines the single migration boundary where v2 rule code meets the
v1-shaped concept builders.

The underlying v1 helpers still read ``environment.concepts`` to resolve
``ConceptRef`` arguments. When ``SemanticState.mirror_to_environment`` is
disabled, the ``visible_in_environment`` scope installed by
``NativeHydrator.parse`` temporarily exposes pending concepts via
``environment.concepts.data`` for the duration of hydrate/validate/commit.
That scope lives inside ``SemanticState`` (the documented compatibility
exception) and is fully reverted on exit, so mirror-off parses exercise
these wrappers without any persistent parse-time environment mutation.

None of these wrappers call ``environment.add_concept`` directly; the
helpers they wrap only read from ``environment.concepts``.
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

    Pending concepts are resolved through the ``visible_in_environment``
    compatibility scope owned by ``SemanticState``.
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
    reads ``environment.concepts``. Mirror-off parses resolve those reads
    through the ``visible_in_environment`` compatibility scope.
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
