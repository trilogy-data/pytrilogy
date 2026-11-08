"""The `like` pattern language (formal/Trilogy/StringMatch.lean).

`%` matches any run of characters, `_` any one, and `\\` makes the next
character literal; a trailing `\\` is itself literal. Every dialect renders
this one language, whatever its own default escape is.
"""

from dataclasses import dataclass
from functools import cache

ESCAPE = "\\"
WILDCARDS = {"%", "_"}


@dataclass(frozen=True)
class Token:
    char: str
    wildcard: bool


def tokenize(pattern: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == ESCAPE and i + 1 < len(pattern):
            tokens.append(Token(pattern[i + 1], False))
            i += 2
            continue
        tokens.append(Token(c, c in WILDCARDS))
        i += 1
    return tokens


def literal_value(pattern: str) -> str | None:
    """The one string a wildcard-free pattern matches, else None."""
    tokens = tokenize(pattern)
    if any(t.wildcard for t in tokens):
        return None
    return "".join(t.char for t in tokens)


def matches_any(pattern: str) -> bool:
    """Whether the pattern matches every string: it is one or more `%`."""
    tokens = tokenize(pattern)
    return bool(tokens) and all(t.wildcard and t.char == "%" for t in tokens)


def canonical(pattern: str, escaped: str = "%_\\") -> str:
    """The pattern with every literal in `escaped` written as `\\c`."""
    return "".join(
        t.char if t.wildcard or t.char not in escaped else ESCAPE + t.char
        for t in tokenize(pattern)
    )


def to_glob(pattern: str) -> str:
    """A SQLite GLOB pattern matching the same strings, case-sensitively."""
    out = []
    for t in tokenize(pattern):
        if t.wildcard:
            out.append("*" if t.char == "%" else "?")
        elif t.char in "*?[":
            out.append(f"[{t.char}]")
        else:
            out.append(t.char)
    return "".join(out)


@cache
def _match(value: str, tokens: tuple[Token, ...], i: int, j: int) -> bool:
    if j == len(tokens):
        return i == len(value)
    t = tokens[j]
    if t.wildcard and t.char == "%":
        return _match(value, tokens, i, j + 1) or (
            i < len(value) and _match(value, tokens, i + 1, j)
        )
    return (
        i < len(value)
        and (t.wildcard or t.char == value[i])
        and _match(value, tokens, i + 1, j + 1)
    )


def matches(value: str, pattern: str, case_insensitive: bool = False) -> bool:
    """The reference matcher: what every rendering must agree with."""
    if case_insensitive:
        value, pattern = value.lower(), pattern.lower()
    return _match(value, tuple(tokenize(pattern)), 0, 0)
