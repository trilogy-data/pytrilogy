"""Which concepts and datasources an author environment reads a name through.

A build of a concept reads the model by address: its lineage's references,
its grain, keys and pseudonyms, each resolved in the environment and read in
turn. So what a concept can depend on is bounded by the strings its own
object graph holds, and the index is exactly that: every string under each
concept and datasource. Coarse on purpose. A name or a literal that happens to
spell an address only marks one more unit to rebuild."""

from collections.abc import Iterable
from enum import Enum
from typing import Any
from weakref import ReferenceType, ref

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.environment import Environment

_LEAVES: set[type] = {int, float, bool, type(None), bytes}
_CONTAINERS = (list, tuple, set, frozenset)
_LOCAL = f"{DEFAULT_NAMESPACE}."

ReferenceIndex = dict[str, frozenset[str]]
# id(environment) -> (weak handle, build stamp, index)
_INDEXES: dict[int, tuple[ReferenceType[Environment], tuple, ReferenceIndex]] = {}


def _fields(node: Any) -> Iterable[Any]:
    held = getattr(node, "__dict__", None)
    if held is not None:
        return held.values()
    return [
        getattr(node, name)
        for cls in type(node).__mro__
        for name in getattr(cls, "__slots__", ())
        if hasattr(node, name)
    ]


def strings_in(root: Any) -> frozenset[str]:
    out: set[str] = set()
    seen: set[int] = set()
    stack = [root]
    while stack:
        node = stack.pop()
        kind = type(node)
        if kind is str:
            out.add(node)
            continue
        if kind in _LEAVES:
            continue
        if issubclass(kind, Enum):
            _LEAVES.add(kind)
            continue
        if id(node) in seen:
            continue
        seen.add(id(node))
        if kind in _CONTAINERS:
            stack.extend(node)
        elif kind is dict:
            stack.extend(node)
            stack.extend(node.values())
        else:
            stack.extend(_fields(node))
    return frozenset(out)


def _evict(key: int, _dead: ReferenceType) -> None:
    _INDEXES.pop(key, None)


def reference_index(environment: Environment) -> ReferenceIndex:
    key = id(environment)
    stamp = (environment.build_stamp(), len(environment.concepts.data))
    held = _INDEXES.get(key)
    if held is not None and held[0]() is environment and held[1] == stamp:
        return held[2]
    index = {
        address: strings_in(concept)
        for address, concept in environment.concepts.data.items()
    }
    for name, datasource in environment.datasources.items():
        index.setdefault(name, strings_in(datasource))
    _INDEXES[key] = (ref(environment, lambda dead: _evict(key, dead)), stamp, index)
    return index


def _spellings(addresses: Iterable[str]) -> set[str]:
    """Each address and the bare name a default-namespace one also answers to."""
    out = set(addresses)
    out.update(a[len(_LOCAL) :] for a in list(out) if a.startswith(_LOCAL))
    return out


def dependents(
    parent: Environment,
    duplicate: Environment,
    changed: set[str],
    joins: Iterable[tuple[str, str, Any]],
) -> set[str]:
    """Every unit of `parent` whose build could differ in `duplicate`, where
    the `changed` addresses are rebound or gone: those addresses, the ones
    either version of them is filed under (a concept kept under another's key
    is its pseudonym, and so are its own), and everything that reads any of
    those, transitively. A join substitutes one endpoint for the other, so a
    reader of either reads both."""
    index = reference_index(parent)
    partners: dict[str, set[str]] = {}
    for left, right, _ in joins:
        partners.setdefault(left, set()).add(right)
        partners.setdefault(right, set()).add(left)
    reached = set(changed)
    for address in changed:
        for held in (parent.concepts.data, duplicate.concepts.data):
            concept = held.get(address)
            if concept is not None:
                reached.add(concept.address)
                reached |= concept.pseudonyms
    dirty: set[str] = set()
    while reached:
        for address in list(reached):
            reached |= partners.get(address, set())
        reached -= dirty
        dirty |= reached
        frontier = _spellings(reached)
        reached = {
            key
            for key, strings in index.items()
            if key not in dirty and not strings.isdisjoint(frontier)
        }
    return dirty
