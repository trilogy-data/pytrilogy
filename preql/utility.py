import hashlib
from typing import List, Any

INT_HASH_SIZE = 16


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10 ** INT_HASH_SIZE
    )


def unique(inputs: List, property: str) -> List[Any]:
    final = []
    dedupe = set()
    for input in inputs:
        key = getattr(input, property, "default")
        if key in dedupe:
            continue
        dedupe.add(key)
        final.append(input)
    return final
