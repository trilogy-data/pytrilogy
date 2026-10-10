from types import SimpleNamespace

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join


def stub_pair(side: str, address: str) -> SimpleNamespace:
    """A join key pair read off CTE `side`, the same concept on both ends."""
    concept = SimpleNamespace(address=address, pseudonyms=set())
    return SimpleNamespace(node=SimpleNamespace(name=side), left=concept, right=concept)


def stub_join(
    right: str, jointype: JoinType, lefts: list[str], address: str = "k"
) -> Join:
    return Join(
        right=SimpleNamespace(name=right),  # type: ignore[arg-type]
        join_type=jointype,
        pairs=[stub_pair(side, address) for side in lefts],  # type: ignore[misc]
    )
