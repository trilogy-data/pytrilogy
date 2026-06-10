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
    SelectLineage,
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


def _unmangle_alias_name(name: str, rowset_name: str) -> str:
    """Recover the user-facing alias name from its hidden per-rowset name.

    SELECT aliases declared inside a rowset are stored under the hidden
    name ``_{rowset_name}_{name}`` (see
    ``SemanticState.mangle_rowset_alias``). The rowset *output* concept
    must carry the user-facing name (``cust_id``, not
    ``_buyers_a_cust_id``). ``rowset_name`` is exact here, so stripping
    the precise prefix is unambiguous even when names contain underscores.
    """
    prefix = f"_{rowset_name}_"
    if name.startswith(prefix):
        return name[len(prefix) :]
    return name


def rowset_output_namespace(
    rowset_name: str,
    rowset_namespace: str,
    source_namespace: str,
) -> str:
    """Target namespace for a rowset output concept.

    When the source concept's namespace matches the rowset's own
    namespace, the output collapses to ``{rowset_name}`` (so
    ``local.revenue`` in a ``local``-scoped rowset becomes
    ``even_orders.revenue``). Otherwise the source namespace is nested
    under the rowset name.
    """
    if not source_namespace or source_namespace == rowset_namespace:
        return rowset_name
    return f"{rowset_name}.{source_namespace}"


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
    name = _unmangle_alias_name(orig_concept.name, rowset.name)
    if isinstance(orig_concept.lineage, FilterItem):
        lineage = orig_concept.lineage
        if lineage.where == rowset.select.where_clause and isinstance(
            lineage.content, (ConceptRef, Concept)
        ):
            name = _unmangle_alias_name(
                context.concepts.require(lineage.content.address).name,
                rowset.name,
            )
    base_namespace = rowset_output_namespace(
        rowset.name, rowset.namespace, orig_concept.namespace
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
            address_with_namespace(x, rowset.name)
            for x in orig_concept.pseudonyms
            if x in context.environment.alias_origin_lookup
        },
    )
    for pseudonym_address in orig_concept.pseudonyms:
        if pseudonym_address not in context.environment.alias_origin_lookup:
            continue
        new_address = address_with_namespace(pseudonym_address, rowset.name)
        alias_updates.append(
            AliasUpdate(
                new_address=new_address,
                origin_address=pseudonym_address,
                rowset_name=rowset.name,
                concept=new_concept,
            )
        )
    for equivalent_address in orig_concept.equivalent_addresses:
        orig[equivalent_address] = new_concept
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
            orig_address,
            rowset,
            context,
            pre_output,
            orig,
            orig_map,
            alias_updates,
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
    # The rowset's grain in its own (namespaced) output space — the select
    # grain remapped through `orig`, exactly as the dimension columns below
    # are remapped. Grain-less columns (unspecified aggregates like
    # `max(..) as in_year1`) live at this grain but carry empty grain from the
    # select output, so they'd otherwise read as scalar downstream. Restricted
    # to plain-select rowsets: a multiselect's grain is alignment-based and its
    # per-arm grouping (rollup) / source maps don't fit this single-grain rule.
    rowset_grain: set[str] = set()
    if isinstance(select_lineage, SelectLineage):
        rowset_grain = {
            orig[c].address for c in select_lineage.grain.components if c in orig
        }
    for x in pre_output:
        if x.keys:
            if all(k in orig for k in x.keys):
                x.keys = set([orig[k].address if k in orig else k for k in x.keys])
            else:
                x.keys = set()
        if not x.grain.components and rowset_grain:
            x.grain = Grain(components=set(rowset_grain))
            x.keys = set(rowset_grain)
        elif all(c in orig for c in x.grain.components):
            x.grain = Grain(components={orig[c].address for c in x.grain.components})
        else:
            x.grain = default_grain
        # When the source dimension carried no key of its own, its alias is
        # keyed by the source concept itself; remapping that through `orig`
        # (a rowset derived twice from a shared parent) yields a self-key
        # (`p.brand` keyed by `p.brand`). A self-referential key makes the
        # dimension read as already-grouped and drops it from the rowset's
        # grain, collapsing the outer join to 1=1. Strip it so the dimension
        # stays a free grain component, matching the once-derived case.
        if x.keys:
            x.keys.discard(x.address)
    return RowsetConceptResult(
        concepts=pre_output,
        alias_updates=alias_updates,
    )


def apply_alias_updates(
    updates: list[AliasUpdate],
    environment: Environment,
) -> None:
    """Materialize alias updates into the environment during commit.

    This is an allowed parse-time env write: it runs from
    ``RowsetStatementPlan.commit`` as final statement materialization,
    not during hydrate/validate. The corresponding concept store write
    is the only rowset-side exception to the v2 no-mutation rule.
    """
    for update in updates:
        origin = environment.alias_origin_lookup[update.origin_address]
        environment.concepts[update.new_address] = update.concept
        environment.alias_origin_lookup[update.new_address] = dc_replace(
            origin, namespace=f"{update.rowset_name}.{origin.namespace}"
        )
