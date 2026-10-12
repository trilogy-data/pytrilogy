from collections.abc import Hashable
from typing import TypeVar

T = TypeVar("T", bound=Hashable)


def find(parent: dict[T, T], node: T) -> T:
    """Root of `node`, registering it as its own root on first sight."""
    while (up := parent.setdefault(node, node)) != node:
        parent[node] = node = parent.setdefault(up, up)
    return node
