"""v2-native rowset concept derivation.

Replaces direct calls to ``trilogy.parsing.common.rowset_to_concepts`` from
v2 rule modules. Reads source concepts through ``context.concepts`` and
records pseudonym/alias origin updates as structured data so no
hydrate-time writes to ``environment.concepts`` or
``environment.alias_origin_lookup`` happen here. The list of
``AliasUpdate`` records is stashed on ``SemanticState`` and drained by
``RowsetStatementPlan.commit`` at commit time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses import replace as dc_replace
from typing import TYPE_CHECKING

from trilogy.core.enums import ConceptSource, Derivation
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    FilterItem,
    Grain,
    Metadata,
    RowsetItem,
    RowsetLineage,
    address_with_namespace,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import RowsetDerivationStatement

if TYPE_CHECKING:
    from trilogy.parsing.v2.rules_context import RuleContext


@dataclass
class AliasUpdate:
    """Pending alias/pseudonym entry for a rowset output concept.

    ``RowsetStatementPlan.commit`` consumes these to write
    ``environment.alias_origin_lookup`` and mirror the rowset pseudonym
    into ``environment.concepts`` at materialization time, so hydrate
    and validate never touch those maps directly.
    """

    new_address: str
    origin_address: str
    rowset_name: str
    concept: Concept


@dataclass
class RowsetConceptResult:
    concepts: list[Concept]
    alias_updates: list[AliasUpdate] = field(default_factory=list)


def _rowset_concept(
    orig_address: ConceptRef,
    rowset: RowsetDerivationStatement,
    context: "RuleContext",
    pre_output: list[Concept],
    orig: dict[str, Concept],
    orig_map: dict[str, Concept],
    alias_updates: list[AliasUpdate],
) -> None:
    orig_concept = context.concepts.require(orig_address.address)
    name = orig_concept.name
    if isinstance(orig_concept.lineage, FilterItem):
        lineage = orig_concept.lineage
        if lineage.where == rowset.select.where_clause and isinstance(
            lineage.content, (ConceptRef, Concept)
        ):
            name = context.concepts.require(lineage.content.address).name
    base_namespace = (
        f"{rowset.name}.{orig_concept.namespace}"
        if orig_concept.namespace != rowset.namespace
        else rowset.name
    )
    new_concept = Concept(
        name=name,
        datatype=orig_concept.datatype,
        purpose=orig_concept.purpose,
        lineage=None,
        grain=orig_concept.grain,
        metadata=Metadata(concept_source=ConceptSource.CTE),
        namespace=base_namespace,
        keys=orig_concept.keys,
        derivation=Derivation.ROWSET,
        granularity=orig_concept.granularity,
        pseudonyms={
            address_with_namespace(x, rowset.name) for x in orig_concept.pseudonyms
        },
    )
    for pseudonym_address in orig_concept.pseudonyms:
        new_address = address_with_namespace(pseudonym_address, rowset.name)
        alias_updates.append(
            AliasUpdate(
                new_address=new_address,
                origin_address=pseudonym_address,
                rowset_name=rowset.name,
                concept=new_concept,
            )
        )
    orig[orig_concept.address] = new_concept
    orig_map[new_concept.address] = orig_concept
    pre_output.append(new_concept)


def rowset_to_concepts_v2(
    rowset: RowsetDerivationStatement,
    context: "RuleContext",
) -> RowsetConceptResult:
    """Derive rowset output concepts via ``context.concepts``.

    Reads source concepts through the parser-owned lookup and returns
    structured alias update records. No parse-time writes to
    ``environment.concepts`` or ``environment.alias_origin_lookup`` occur
    here; ``RowsetStatementPlan.commit`` applies the updates.
    """
    pre_output: list[Concept] = []
    orig: dict[str, Concept] = {}
    orig_map: dict[str, Concept] = {}
    alias_updates: list[AliasUpdate] = []
    for orig_address in rowset.select.output_components:
        _rowset_concept(
            orig_address, rowset, context, pre_output, orig, orig_map, alias_updates
        )
    select_lineage = rowset.select.as_lineage(context.environment)
    for x in pre_output:
        x.lineage = RowsetItem(
            content=orig_map[x.address].reference,
            rowset=RowsetLineage(
                name=rowset.name,
                derived_concepts=[y.reference for y in pre_output],
                select=select_lineage,
            ),
        )
    default_grain = Grain.from_concepts([*pre_output])
    for x in pre_output:
        if x.keys:
            if all(k in orig for k in x.keys):
                x.keys = set([orig[k].address if k in orig else k for k in x.keys])
            else:
                x.keys = set()
        if all(c in orig for c in x.grain.components):
            x.grain = Grain(components={orig[c].address for c in x.grain.components})
        else:
            x.grain = default_grain
    return RowsetConceptResult(concepts=pre_output, alias_updates=alias_updates)


def apply_alias_updates(
    updates: list[AliasUpdate],
    environment: Environment,
) -> None:
    """Materialize alias updates into the environment during commit."""
    for update in updates:
        origin = environment.alias_origin_lookup[update.origin_address]
        environment.concepts[update.new_address] = update.concept
        environment.alias_origin_lookup[update.new_address] = dc_replace(
            origin, namespace=f"{update.rowset_name}.{origin.namespace}"
        )
