import gc
import hashlib
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import partial
from functools import singledispatchmethod as _stdlib_singledispatchmethod
from os import PathLike
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    SupportsIndex,
    TextIO,
    TypeVar,
    cast,
    overload,
)

from _preql_import_resolver import unique_by_attr as _unique_by_attr

from trilogy.constants import DEFAULT_NAMESPACE

INT_HASH_SIZE = 16


def utc_now_iso() -> str:
    """Current UTC time, ISO-8601. The emitting process's own clock — fine for
    display, never for ordering events across machines."""
    return datetime.now(UTC).isoformat()


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


@contextmanager
def gc_paused() -> Iterator[None]:
    """Planning allocates heavily but leaves little cyclic garbage, and each
    full collection rescans the whole environment for it: ~15% of planning
    CPU. The deferred garbage is collected once the outermost pause ends."""
    if not gc.isenabled():
        yield
        return
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


def string_to_hash(input: str) -> int:
    return (
        int(hashlib.sha256(input.encode("utf-8")).hexdigest(), 16) % 10**INT_HASH_SIZE
    )


UniqueArg = TypeVar("UniqueArg")

if TYPE_CHECKING:
    from typing_extensions import deprecated

    class UniqueList(list[UniqueArg]):
        """A list `unique` built: no two items share a key (address for
        concepts, name for CTEs). Typing-only - at runtime it is `list`, so
        marking one costs nothing. Concatenating gives a plain list again;
        in-place mutation is flagged, so a holder stays unique."""

        @overload
        def __init__(self) -> None: ...

        @overload
        def __init__(self, iterable: "UniqueList[UniqueArg]", /) -> None: ...

        def __init__(self, *args: Any) -> None: ...

        @deprecated("may add a duplicate; build a new list with unique()")
        def append(self, object: UniqueArg, /) -> None: ...

        @deprecated("may add duplicates; build a new list with unique()")
        def extend(self, iterable: Iterable[UniqueArg], /) -> None: ...

        @deprecated("may add a duplicate; build a new list with unique()")
        def insert(self, index: SupportsIndex, object: UniqueArg, /) -> None: ...

        @deprecated("may add duplicates; build a new list with unique()")
        def __iadd__(self, value: Iterable[UniqueArg], /) -> Self: ...  # type: ignore[override, misc]

    @overload  # type: ignore[no-overload-impl]
    @deprecated("already unique: drop this call")
    def unique(
        inputs: UniqueList[UniqueArg], property: str | Callable
    ) -> UniqueList[UniqueArg]: ...

    @overload
    def unique(
        inputs: list[UniqueArg], property: str | Callable
    ) -> UniqueList[UniqueArg]: ...


def _unique(inputs: list[UniqueArg], property: str | Callable) -> list[UniqueArg]:
    if not inputs:
        return []
    if isinstance(property, str):
        if type(inputs) is list:
            return _unique_by_attr(inputs, property, DEFAULT_NAMESPACE)
        property = partial(_attr_or_default, property)
    # dict.setdefault keeps the first holder of each key, in first-seen order
    first: dict[Any, UniqueArg] = {}
    for input in inputs:
        first.setdefault(property(input), input)
    return list(first.values())


if not TYPE_CHECKING:
    UniqueList = list
    unique = _unique


def assume_unique(items: list[UniqueArg]) -> UniqueList[UniqueArg]:
    """Mark a list the caller knows is unique, such as a filter of one."""
    return cast("UniqueList[UniqueArg]", items)


def append_new(
    items: UniqueList[UniqueArg], item: UniqueArg, property: str = "address"
) -> bool:
    """Append `item` unless an item already holds its key; whether it did."""
    key = getattr(item, property, DEFAULT_NAMESPACE)
    if any(getattr(x, property, DEFAULT_NAMESPACE) == key for x in items):
        return False
    list.append(items, item)
    return True


def _attr_or_default(name: str, item: object) -> Any:
    return getattr(item, name, DEFAULT_NAMESPACE)


def _dispatch_bound(dispatch: Callable, obj, cls, arg, *args, **kwargs):
    return dispatch(arg.__class__).__get__(obj, cls)(arg, *args, **kwargs)


class singledispatchmethod(_stdlib_singledispatchmethod):
    """`functools.singledispatchmethod` without its per-instance method cache.

    On CPython 3.12.8 and 3.13.0-3.13.1 that cache is a WeakKeyDictionary whose
    values close over their own key, so every instance that ever called the
    method is kept alive for the life of the process (gh-127750).

    The bound method is a bare partial, without the `register`/`__name__`/
    `__doc__` the stdlib's sets: nothing reads them, and setting them on every
    access slows each dispatch by half (it is on the renderer's hot path)."""

    def __get__(self, obj, cls=None):
        return partial(_dispatch_bound, self.dispatcher.dispatch, obj, cls)
