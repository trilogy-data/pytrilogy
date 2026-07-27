import hashlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from os import PathLike
from typing import TextIO, TypeVar, cast

from trilogy.constants import DEFAULT_NAMESPACE

INT_HASH_SIZE = 16


def utc_now_iso() -> str:
    """Current UTC time, ISO-8601. The emitting process's own clock — fine for
    display, never for ordering events across machines."""
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def safe_open(
    path: str | PathLike,
    mode: str = "r",
    *,
    encoding: str = "utf-8",
    errors: str | None = None,
    newline: str | None = None,
) -> Iterator[TextIO]:
    """Open a text file with UTF-8 by default.

    On read, invalid bytes become U+FFFD instead of raising — Windows' cp1252
    default otherwise trips on UTF-8 content. Binary mode is rejected; use
    open() directly for that.
    """
    if "b" in mode:
        raise ValueError("safe_open is text-only; use open() for binary modes")
    if errors is None:
        errors = "replace" if "r" in mode else "strict"
    with open(path, mode, encoding=encoding, errors=errors, newline=newline) as f:
        yield cast(TextIO, f)


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10**INT_HASH_SIZE
    )


UniqueArg = TypeVar("UniqueArg")


def unique(inputs: list[UniqueArg], property: str | Callable) -> list[UniqueArg]:
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
