"""What a scan binds only partially, decided once for its three readers: the
network candidate (`network_build._candidate`), the scan node's stamp
(`select_node_v2.scan_stamps`) and the union node's
(`datasource_nodes.create_union_datasource_candidate`)."""

from collections.abc import Collection, Iterable

from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
)


def scan_partial_addresses(
    datasource: BuildDatasource | BuildUnionDatasource,
    emitted: Iterable[BuildConcept],
    stored: Collection[str],
    exempt: Collection[str] = (),
    partial_is_full: bool = False,
) -> set[str]:
    """Addresses (authored and canonical) of the `emitted` values this scan
    binds partially.

    A `~` column with no complete binding of the same address beside it is
    partial (`partial_concepts`), unless this read need not complete it: an
    `exempt` key (a span a region domain completes above, a key a membership
    WHERE pins to this table) binds as fully as the read needs. A satisfied
    `complete where` (`partial_is_full`) completes every `~` the partition
    heals and leaves the rest as extension licenses
    (`pinned_partial_addresses`). A row value the scan computes inline, off
    no `stored` column, is as partial as the `~` keys it is a function of:
    the scan computes it for its own rows, and the rows it lacks (the lines
    no return references) hold a value it cannot (`ret_qty is not null`), so
    a complete column elsewhere must bind them. Only a BASIC is such a value:
    an aggregate the scan hosts is a function of its own rows that no other
    column binds, so its keys' partiality is the only partiality it has."""
    exempted = set(exempt)
    structural = (
        datasource.pinned_partial_addresses
        if partial_is_full and isinstance(datasource, BuildDatasource)
        else None
    )
    partial: set[str] = set()
    for column in datasource.partial_concepts:
        spellings = column.spellings
        if spellings & exempted:
            continue
        if structural is not None and column.address not in structural:
            continue
        partial |= spellings
    if not partial:
        return set()
    stored_addresses = set(stored)
    out: set[str] = set()
    for concept in emitted:
        spellings = concept.spellings
        if spellings & partial or (
            concept.derivation == Derivation.BASIC
            and not spellings & stored_addresses
            and set(concept.keys or concept.grain.components) & partial
        ):
            out |= spellings
    return out
