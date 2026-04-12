"""v2-native rowset concept derivation.

Replaces direct calls to ``trilogy.parsing.common.rowset_to_concepts`` from
v2 rule modules. Today this wrapper still delegates to the v1 helper, which
reads ``environment.concepts`` and writes alias/pseudonym data directly to
``environment.concepts`` and ``environment.alias_origin_lookup``. The
wrapper:

  * Returns a structured ``RowsetConceptResult`` rather than a bare list so
    future changes (once mirror removal lands) can attach alias-update
    bookkeeping without touching rule modules.
  * Provides the single migration point for eliminating parse-time
    environment writes from the rowset path.

See ``select_finalize.py`` for the broader audit of v2 ``context.environment``
usage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from trilogy.core.models.author import Concept
from trilogy.core.statements.author import RowsetDerivationStatement
from trilogy.parsing.common import rowset_to_concepts

if TYPE_CHECKING:
    from trilogy.parsing.v2.rules_context import RuleContext


@dataclass
class AliasUpdate:
    """Pending alias/pseudonym entry needed by a rowset statement.

    Currently reserved — the v1 helper applies alias updates to
    ``environment.alias_origin_lookup`` directly. Once that write is
    migrated out of parse time, ``rowset_to_concepts_v2`` will populate
    this list and the statement plan will apply it during ``commit``.
    """

    address: str
    concept: Concept


@dataclass
class RowsetConceptResult:
    concepts: list[Concept]
    alias_updates: list[AliasUpdate] = field(default_factory=list)


def rowset_to_concepts_v2(
    rowset: RowsetDerivationStatement,
    context: "RuleContext",
) -> RowsetConceptResult:
    """v2 wrapper for ``rowset_to_concepts``.

    Delegates to the v1 helper today; future work will move the alias
    bookkeeping into ``RowsetConceptResult.alias_updates`` so the mirror
    can be removed without losing rowset pseudonym wiring.
    """
    concepts = list(rowset_to_concepts(rowset, context.environment))
    return RowsetConceptResult(concepts=concepts)
