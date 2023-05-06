import hashlib
from typing import List, Any, Union, Callable

from preql.constants import DEFAULT_NAMESPACE

INT_HASH_SIZE = 16


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10**INT_HASH_SIZE
    )


def unique(inputs: List, property: Union[str, Callable]) -> List[Any]:
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
