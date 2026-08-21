"""Sample values from a declared string validator's regex.

``type ipv4_address string['\\d{1,3}(\\.\\d{1,3}){3}']`` is a *domain*, not
decoration: `validate_datasource` runs the same pattern over the mocked column,
so a value that ignores it fails validation exactly as bad real data would.
Without a sampler the whole unit tier is unavailable to any model using the
stdlib's pattern-carrying types (``::url``, ``::email_address``, ``::hex``).

The grammar covered is the one that appears in a type declaration — literals,
character classes, alternation, groups, repetition. Lookaround, backreferences
and the rest still raise: a value that silently ignored the pattern would be
worse than a loud gap.
"""

import random
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import product

# Unbounded repetition has to stop somewhere; three is enough to show that a
# repeated group repeats without making values unreadable.
STAR_CAP = 3
DIGITS = "0123456789"
LOWER = "abcdefghijklmnopqrstuvwxyz"
UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
WORD = LOWER + UPPER + DIGITS + "_"
SPACE = " "
# what `.` and `\S` draw from: printable, no whitespace, no quoting hazards
PRINTABLE = WORD + "-.+"
ESCAPE_CLASSES = {
    "d": DIGITS,
    "w": WORD,
    "s": SPACE,
    "D": LOWER + UPPER + "_-.",
    "W": "-.+ ",
    "S": PRINTABLE,
}


class PatternUnsupported(NotImplementedError):
    pass


@dataclass
class Chars:
    options: str


@dataclass
class Seq:
    parts: list["Node"]


@dataclass
class Alt:
    options: list["Node"]


@dataclass
class Repeat:
    node: "Node"
    low: int
    high: int


Node = Chars | Seq | Alt | Repeat


class _Parser:
    def __init__(self, pattern: str):
        self.pattern = pattern
        self.pos = 0

    def fail(self, message: str) -> PatternUnsupported:
        return PatternUnsupported(
            f"Mocking is not implemented for pattern {self.pattern!r}: {message}"
        )

    def peek(self) -> str | None:
        return self.pattern[self.pos] if self.pos < len(self.pattern) else None

    def parse(self) -> Node:
        node = self.alternation()
        if self.pos != len(self.pattern):
            raise self.fail(f"unexpected {self.pattern[self.pos]!r}")
        return node

    def alternation(self) -> Node:
        options: list[Node] = [self.sequence()]
        while self.peek() == "|":
            self.pos += 1
            options.append(self.sequence())
        return options[0] if len(options) == 1 else Alt(options)

    def sequence(self) -> Node:
        parts: list[Node] = []
        while (char := self.peek()) is not None and char not in "|)":
            # fullmatch is what validation applies, so anchors add nothing
            if char in "^$":
                self.pos += 1
                continue
            parts.append(self.quantified())
        return parts[0] if len(parts) == 1 else Seq(parts)

    def quantified(self) -> Node:
        node = self.atom()
        char = self.peek()
        if char == "?":
            self.pos += 1
            node = Repeat(node, 0, 1)
        elif char == "*":
            self.pos += 1
            node = Repeat(node, 0, STAR_CAP)
        elif char == "+":
            self.pos += 1
            node = Repeat(node, 1, STAR_CAP)
        elif char == "{":
            node = self.braces(node)
        # greedy or not, every match is equally valid to generate
        if self.peek() in ("?", "+") and isinstance(node, Repeat):
            self.pos += 1
        return node

    def braces(self, node: Node) -> Node:
        close = self.pattern.find("}", self.pos)
        if close < 0:
            raise self.fail("unterminated {")
        body = self.pattern[self.pos + 1 : close]
        self.pos = close + 1
        low_text, _, high_text = body.partition(",")
        if not low_text.isdigit():
            raise self.fail(f"unsupported repetition {{{body}}}")
        low = int(low_text)
        if "," not in body:
            return Repeat(node, low, low)
        if not high_text:
            return Repeat(node, low, low + STAR_CAP)
        if not high_text.isdigit():
            raise self.fail(f"unsupported repetition {{{body}}}")
        return Repeat(node, low, int(high_text))

    def atom(self) -> Node:
        char = self.pattern[self.pos]
        if char == "(":
            self.pos += 1
            if self.pattern.startswith("?:", self.pos):
                self.pos += 2
            elif self.peek() == "?":
                raise self.fail("lookaround and named groups are not supported")
            node = self.alternation()
            if self.peek() != ")":
                raise self.fail("unterminated (")
            self.pos += 1
            return node
        if char == "[":
            return self.char_class()
        if char == "\\":
            return self.escape()
        if char == ".":
            self.pos += 1
            return Chars(PRINTABLE)
        self.pos += 1
        return Chars(char)

    def escape(self) -> Chars:
        self.pos += 1
        char = self.peek()
        if char is None:
            raise self.fail("trailing backslash")
        self.pos += 1
        if char.isdigit():
            raise self.fail("backreferences are not supported")
        if char in ESCAPE_CLASSES:
            return Chars(ESCAPE_CLASSES[char])
        return Chars({"n": "\n", "t": "\t", "r": "\r"}.get(char, char))

    def char_class(self) -> Node:
        self.pos += 1
        negated = self.peek() == "^"
        if negated:
            self.pos += 1
        members = ""
        while (char := self.peek()) is not None and char != "]":
            if char == "\\":
                members += self.escape().options
                continue
            self.pos += 1
            if self.peek() == "-" and self.pattern[self.pos + 1 : self.pos + 2] not in (
                "",
                "]",
            ):
                end = self.pattern[self.pos + 1]
                self.pos += 2
                members += "".join(chr(code) for code in range(ord(char), ord(end) + 1))
            else:
                members += char
        if self.peek() != "]":
            raise self.fail("unterminated [")
        self.pos += 1
        options = "".join(dict.fromkeys(members))
        if negated:
            options = "".join(c for c in PRINTABLE if c not in options)
        if not options:
            raise self.fail("character class admits nothing")
        return Chars(options)


def parse_pattern(pattern: str) -> Node:
    return _Parser(pattern).parse()


def pattern_size(node: Node, cap: int) -> int:
    """How many strings the node can produce, saturating at ``cap``."""
    if isinstance(node, Chars):
        return min(len(node.options), cap)
    if isinstance(node, Alt):
        total = 0
        for option in node.options:
            total = min(total + pattern_size(option, cap), cap)
        return total
    if isinstance(node, Seq):
        total = 1
        for part in node.parts:
            total = min(total * pattern_size(part, cap), cap)
        return total
    inner = pattern_size(node.node, cap)
    total = 0
    for count in range(node.low, node.high + 1):
        total = min(total + inner**count, cap)
    return total


def pattern_values(node: Node) -> Iterator[str]:
    """Every string the node can produce, in a stable order. Only safe to drain
    when ``pattern_size`` says the language is small."""
    if isinstance(node, Chars):
        yield from node.options
    elif isinstance(node, Alt):
        for option in node.options:
            yield from pattern_values(option)
    elif isinstance(node, Seq):
        for combination in product(*(list(pattern_values(p)) for p in node.parts)):
            yield "".join(combination)
    else:
        inner = list(pattern_values(node.node))
        for count in range(node.low, node.high + 1):
            for combination in product(inner, repeat=count):
                yield "".join(combination)


def pattern_sample(node: Node) -> str:
    if isinstance(node, Chars):
        return random.choice(node.options)
    if isinstance(node, Alt):
        return pattern_sample(random.choice(node.options))
    if isinstance(node, Seq):
        return "".join(pattern_sample(part) for part in node.parts)
    return "".join(
        pattern_sample(node.node) for _ in range(random.randint(node.low, node.high))
    )


def mock_pattern(pattern: str, scale_factor: int, is_key: bool) -> list[str]:
    node = parse_pattern(pattern)
    if not is_key:
        return [pattern_sample(node) for _ in range(scale_factor)]
    # A key must not repeat. A language smaller than scale_factor caps the row
    # count, exactly as a small enum domain does; a large one is sampled until
    # it yields enough distinct values.
    if pattern_size(node, scale_factor + 1) <= scale_factor:
        return list(dict.fromkeys(pattern_values(node)))[:scale_factor]
    seen: dict[str, None] = {}
    for _ in range(scale_factor * 50):
        seen[pattern_sample(node)] = None
        if len(seen) == scale_factor:
            break
    return list(seen)
