"""A direct `__deepcopy__` for the model dataclasses.

`copy.deepcopy` reaches a plain dataclass through `__reduce_ex__` and
`copy._reconstruct`, the slowest path it has, and `Environment.duplicate` walks
~15k model objects that way. This copies the instance `__dict__` straight
into a fresh instance: the same state, the same memo, the same identities the
generic path produces, without its per-object reduce/rebuild overhead."""

from copy import deepcopy
from dataclasses import is_dataclass
from enum import Enum
from typing import Any

_ATOMIC: frozenset[type] = frozenset({str, int, float, bool, type(None), bytes})
_MISSING = object()
# Types found to be shared rather than copied (enums) or to copy through
# `dataclass_deepcopy`; both are decided once per type.
_SHARED: set[type] = set(_ATOMIC)
_DIRECT: set[type] = set()
_GENERIC: set[type] = set()


def _classify(kind: type) -> None:
    if issubclass(kind, Enum):
        _SHARED.add(kind)
    elif getattr(kind, "__deepcopy__", None) is dataclass_deepcopy:
        _DIRECT.add(kind)
    else:
        _GENERIC.add(kind)


def _copy_value(value: Any, memo: dict[int, Any]) -> Any:
    kind = type(value)
    if kind in _SHARED:
        return value
    if kind in _DIRECT:
        # `deepcopy` would land here too, after its own dispatch
        found = memo.get(id(value), _MISSING)
        return dataclass_deepcopy(value, memo) if found is _MISSING else found
    if kind is set:
        # `deepcopy` has no set fast path: it reduces and rebuilds each one.
        found = memo.get(id(value), _MISSING)
        if found is not _MISSING:
            return found
        copied = (
            set(value)
            if all(type(item) in _SHARED for item in value)
            else {_copy_value(item, memo) for item in value}
        )
        memo[id(value)] = copied
        return copied
    if kind is list:
        found = memo.get(id(value), _MISSING)
        if found is not _MISSING:
            return found
        items: list[Any] = []
        memo[id(value)] = items
        items.extend([_copy_value(item, memo) for item in value])
        return items
    if kind is tuple:
        # a tuple of shared values is itself shared, as `deepcopy` returns it
        if all(type(item) in _SHARED for item in value):
            return value
        return deepcopy(value, memo)
    if kind not in _GENERIC:
        _classify(kind)
        return _copy_value(value, memo)
    return deepcopy(value, memo)


def dataclass_deepcopy(self: Any, memo: dict[int, Any]) -> Any:
    new = object.__new__(type(self))
    memo[id(self)] = new
    target = new.__dict__
    for key, value in self.__dict__.items():
        target[key] = value if type(value) in _SHARED else _copy_value(value, memo)
    return new


def _copies_itself(cls: type) -> bool:
    """Whether `cls` keeps state outside `__dict__` or says how it copies."""
    if "__slots__" in vars(cls):
        return True
    return any(
        getattr(cls, hook, None) is not getattr(object, hook, None)
        for hook in (
            "__deepcopy__",
            "__reduce__",
            "__reduce_ex__",
            "__getstate__",
            "__setstate__",
        )
    )


def install_dataclass_deepcopy(namespace: dict[str, Any]) -> None:
    """Give every dataclass defined in a module (its `globals()`) the direct copy."""
    for obj in list(namespace.values()):
        if (
            isinstance(obj, type)
            and is_dataclass(obj)
            and obj.__module__ == namespace["__name__"]
            and not _copies_itself(obj)
        ):
            obj.__deepcopy__ = dataclass_deepcopy  # type: ignore[attr-defined]
