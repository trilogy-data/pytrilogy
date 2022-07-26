import hashlib

INT_HASH_SIZE = 16


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10 ** INT_HASH_SIZE
    )
