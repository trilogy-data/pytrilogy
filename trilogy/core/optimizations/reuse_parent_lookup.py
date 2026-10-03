"""Read a lookup's columns off a parent that already joined it.

A consumer ``C`` that LEFT-joins a lookup ``X`` on a key read off another parent
``Y``, where ``Y`` itself joined ``X``, repeats work: ``Y`` can carry the
columns ``C`` wants from ``X`` and the join goes. The two reads agree on every
row of ``Y`` when ``X`` is unique on the key and ``Y``'s key is anchored on
``X``:

- ``Y`` reads the key off ``X`` alone: a row holding ``X`` holds the one ``X``
  row with that key, and a row without it has a NULL key, which matches
  nothing in ``C`` either.
- ``Y`` coalesces the key over ``X`` and the side ``W`` its plain-equality join
  pairs ``X`` with: a row holding ``X`` has ``Y.key = X.key``; a row without
  ``X`` takes ``W.key``, which no ``X`` row has, or the join would have paired.

``C`` must read only non-key columns off ``X`` (its own read of ``X.key`` is
NULL where the join misses), and ``Y`` must not group.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    DatasourceCTE,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import is_grouped_cte


def _plain(join: Join) -> bool:
    return (
        join.condition is None
        and Modifier.NULLABLE not in join.modifiers
        and all(
            Modifier.NULLABLE not in pair.modifiers for pair in join.joinkey_pairs or []
        )
    )


def _regular_parent(cte: CTE, name: str) -> CTE | None:
    for parent in cte.parent_ctes:
        if parent.name == name and isinstance(parent, CTE):
            if isinstance(parent, DatasourceCTE) and cte.renders_inline(parent):
                return None
            return parent
    return None


def _unique_on(node: CTE, key: BuildConcept) -> bool:
    grain = set(node.grain.components)
    return bool(grain) and grain <= key.equivalent_addresses


def _key_anchored_on(holder: CTE, lookup: CTE, key: BuildConcept) -> bool:
    sources = holder.source_map.get(key.address, [])
    if sources == [lookup.name]:
        return True
    if lookup.name not in sources:
        return False
    for join in holder.joins:
        if not (isinstance(join, Join) and join.right_cte.name == lookup.name):
            continue
        pairs = join.joinkey_pairs or []
        return (
            _plain(join)
            and len(pairs) == 1
            and bool(pairs[0].right.equivalent_addresses & key.equivalent_addresses)
            and set(sources) == {lookup.name, pairs[0].cte.name}
        )
    return False


def _reads_off(consumer: CTE, lookup: CTE) -> list[str] | None:
    """Addresses `consumer` reads off `lookup` alone; None if any read is
    shared with another source (a coalesce)."""
    out = []
    for address, sources in consumer.source_map.items():
        if lookup.name not in sources:
            continue
        if sources != [lookup.name]:
            return None
        out.append(address)
    return out


def _referenced_elsewhere(consumer: CTE, join: Join, lookup: CTE) -> bool:
    for other in consumer.joins:
        if other is join or not isinstance(other, Join):
            continue
        if other.right_cte.name == lookup.name or (
            other.left_cte is not None and other.left_cte.name == lookup.name
        ):
            return True
        if any(pair.cte.name == lookup.name for pair in other.joinkey_pairs or []):
            return True
    return lookup.name in {
        s for sources in consumer.existence_source_map.values() for s in sources
    }


def _carry(holder: CTE, lookup: CTE, addresses: list[str]) -> None:
    present = {c.address for c in holder.output_columns}
    for column in lookup.output_columns:
        if column.address not in addresses or column.address in present:
            continue
        holder.output_columns.append(column)
        holder.source_map[column.address] = [lookup.name]
        holder.source.output_concepts.append(column)
        holder.source.source_map.setdefault(column.address, set()).add(lookup.source)
        if column.address not in {c.address for c in holder.nullable_concepts}:
            holder.nullable_concepts.append(column)
        holder.hidden_concepts.discard(column.address)


def _drop_lookup(consumer: CTE, join: Join, lookup: CTE, holder: CTE) -> None:
    consumer.joins = [j for j in consumer.joins if j is not join]
    consumer.source.joins = [
        bj
        for bj in consumer.source.joins
        if not (
            isinstance(bj, BaseJoin)
            and bj.right_datasource.identifier == lookup.source.identifier
        )
    ]
    consumer.source.datasources = [
        ds
        for ds in consumer.source.datasources
        if ds.identifier != lookup.source.identifier
    ]
    consumer.parent_ctes = [p for p in consumer.parent_ctes if p.name != lookup.name]
    for address, sources in list(consumer.source_map.items()):
        if sources == [lookup.name]:
            consumer.source_map[address] = [holder.name]
            consumer.source.source_map[address] = {holder.source}


class ReuseParentLookup(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        for join in cte.joins:
            if self._reuse(cte, join):
                return True, None
        return False, None

    def _reuse(self, cte: CTE, join: object) -> bool:
        if not (
            isinstance(join, Join)
            and join.jointype == JoinType.LEFT_OUTER
            and _plain(join)
            and len(join.joinkey_pairs or []) == 1
        ):
            return False
        pair = (join.joinkey_pairs or [])[0]
        lookup = _regular_parent(cte, join.right_cte.name)
        holder = _regular_parent(cte, pair.cte.name)
        if lookup is None or holder is None or holder.name == lookup.name:
            return False
        if (
            _regular_parent(holder, lookup.name) is None
            or is_grouped_cte(holder)
            or not isinstance(holder.source, QueryDatasource)
            or not _unique_on(lookup, pair.right)
            or not _key_anchored_on(holder, lookup, pair.left)
            or _referenced_elsewhere(cte, join, lookup)
        ):
            return False
        reads = _reads_off(cte, lookup)
        if not reads or pair.right.equivalent_addresses & set(reads):
            return False
        self.log(
            f"{cte.name} reads {sorted(reads)} off {holder.name}, not {lookup.name}"
        )
        _carry(holder, lookup, reads)
        _drop_lookup(cte, join, lookup, holder)
        return True
