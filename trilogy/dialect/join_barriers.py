"""Which CTEs must be optimization barriers on an engine that inlines CTEs and
reorders the joins it finds inside them.

A join on several keys is one pairing only while one relation holds all of
them. When a side's keys were assembled by a join (``city`` off one parent,
``density_cell`` off another), an inlining optimizer sees each key as its own
edge to a different relation and may join on one of them alone: on a key of
few values that is close to a cross product, and the other key only filters it
afterwards (#1027: 337k trees joined to 45k cells on a one-value ``city``).
Materialized, the side is one relation again and the join keeps every key.
"""

from collections.abc import Sequence

from trilogy.core.models.execute import CTE, Join, UnionCTE

# A relation is one reference to a table, not the table: a CTE read twice is
# two relations to a join order, so an origin is the path of names down to it.
_Origin = tuple[str, ...]
_Memo = dict[tuple[str, str], frozenset[_Origin]]


def reorderable(cte: CTE | UnionCTE) -> bool:
    """Whether an inlining optimizer reorders this CTE's joins with its
    consumer's: a plain select. A scan is one relation; an aggregate, a limit,
    a union and a recursion each compute their rows before anything joins."""
    return type(cte) is CTE and not cte.group_to_grain and cte.limit is None


def _computed_from(cte: CTE, address: str) -> list[str]:
    concept = next((c for c in cte.output_columns if c.address == address), None)
    if concept is None or concept.lineage is None:
        return []
    return [
        a.address for a in concept.lineage.concept_arguments if a.address != address
    ]


def key_origins(
    cte: CTE | UnionCTE,
    address: str,
    by_name: dict[str, CTE | UnionCTE],
    barriers: set[str],
    memo: _Memo,
) -> frozenset[_Origin]:
    """The relations `address` is read off once every reorderable CTE under
    `cte` is inlined, each as its path from `cte`."""
    if not isinstance(cte, CTE) or not reorderable(cte) or cte.name in barriers:
        return frozenset({(cte.name,)})
    key = (cte.name, address)
    if key in memo:
        return memo[key]
    # a column computed from itself through a cycle has no further origin
    memo[key] = frozenset()
    origins: set[_Origin] = set()
    sources = cte.source_map.get(address) or []
    for name in sources:
        parent = by_name.get(name)
        if parent is None or name == cte.name:
            origins.add((cte.name, name))
            continue
        for path in key_origins(parent, address, by_name, barriers, memo):
            origins.add((cte.name, *path))
    if not sources:
        for argument in _computed_from(cte, address):
            origins |= key_origins(cte, argument, by_name, barriers, memo)
    memo[key] = frozenset(origins)
    return memo[key]


def _side_keys(join: Join) -> dict[str, set[str]]:
    """Each CTE the join reads keys off -> the keys it reads off it."""
    keys: dict[str, set[str]] = {}
    for pair in join.pairs:
        keys.setdefault(pair.node.name, set()).add(pair.left.address)
        keys.setdefault(join.right.name, set()).add(pair.right.address)
    return keys


def split_key_barriers(ctes: Sequence[CTE | UnionCTE]) -> set[str]:
    """The names of the CTEs to materialize: each one a join reads several
    keys off that trace to more than one relation. `ctes` is in dependency
    order, so a barrier already placed ends the trace through it."""
    by_name = {cte.name: cte for cte in ctes}
    barriers: set[str] = set()
    memo: _Memo = {}
    for consumer in ctes:
        if not isinstance(consumer, CTE):
            continue
        for join in consumer.joins:
            if not isinstance(join, Join):
                continue
            for name, addresses in _side_keys(join).items():
                side = by_name.get(name)
                if side is None or len(addresses) < 2 or name in barriers:
                    continue
                origins: set[_Origin] = set()
                for address in sorted(addresses):
                    origins |= key_origins(side, address, by_name, barriers, memo)
                if len(origins) > 1:
                    barriers.add(name)
                    memo.clear()
    return barriers
