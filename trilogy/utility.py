import hashlib
from typing import Callable, List, TypeVar, Union

from trilogy.constants import DEFAULT_NAMESPACE

INT_HASH_SIZE = 16


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10**INT_HASH_SIZE
    )


UniqueArg = TypeVar("UniqueArg")


def unique(inputs: List[UniqueArg], property: Union[str, Callable]) -> List[UniqueArg]:
    final = []
    dedupe = set()
    if isinstance(property, str):

        def getter(x):
            return getattr(x, property, DEFAULT_NAMESPACE)

    else:
        getter = property
    for input in inputs:
        key = getter(input)
        if key in dedupe:
            continue
        dedupe.add(key)
        final.append(input)
    return final
