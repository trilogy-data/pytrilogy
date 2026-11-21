"""Rewrite the removed `select ... by rollup|cube|grouping sets (...)` clause as
the call form: `rollup((select ...), keys)`.

    python local_scripts/migrate_by_rollup.py path [path ...]

`.preql` files are rewritten whole; in `.py` files every plain string literal
holding the clause is. A statement it cannot place (a merge, an f-string) is
left alone and counted.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

CLAUSE = re.compile(r"\bby\s+(rollup|cube|grouping\s+sets)\s*\(", re.IGNORECASE)
LEAD = re.compile(r"\s*(?:(?:with\s+\w+\s+as|rowset\s+\w+\s*<-)\s+)?", re.IGNORECASE)
COMMENTS = re.compile(r"\s*(?:(?:#|//)[^\n]*\n\s*)*")
UNSUPPORTED = re.compile(r"\b(merge|align|persist)\b", re.IGNORECASE)
BREAKS = re.compile(r"(?<=[ \n])")
NEWLINE = "\n"
BACKSLASH = "\\"


def close_paren(text: str, open_at: int) -> int:
    depth = 0
    for i in range(open_at, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced parens")


def statement_start(text: str, at: int) -> int:
    """After the nearest `;` or unmatched `(` before `at`."""
    depth = 0
    for i in range(at - 1, -1, -1):
        char = text[i]
        if char == ")":
            depth += 1
        elif char == "(":
            if depth == 0:
                return i + 1
            depth -= 1
        elif char == ";" and depth == 0:
            return i + 1
    return 0


def statement_end(text: str, at: int) -> int:
    """The next `;` or unmatched `)` from `at`, else the end of the text."""
    depth = 0
    for i in range(at, len(text)):
        char = text[i]
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                return i
            depth -= 1
        elif char == ";" and depth == 0:
            return i
    return len(text)


def migrate_one(text: str, match: re.Match[str]) -> str | None:
    name = re.sub(r"\s+", "_", match.group(1).lower())
    keys_open = match.end() - 1
    keys_close = close_paren(text, keys_open)
    keys = text[keys_open + 1 : keys_close].strip()
    start = statement_start(text, match.start())
    end = statement_end(text, keys_close + 1)
    comments = COMMENTS.match(text, start)
    assert comments is not None
    lead = LEAD.match(text, comments.end())
    assert lead is not None
    body = text[lead.end() : match.start()].rstrip().removesuffix(",").rstrip()
    tail = text[keys_close + 1 : end].strip()
    if UNSUPPORTED.search(body) or UNSUPPORTED.search(tail):
        return None
    multiline = NEWLINE in body or NEWLINE in tail
    gap = NEWLINE if multiline else " "
    arm = body + (gap + tail if tail else "")
    close = NEWLINE + ")" if multiline else ")"
    call = f"{name}(({arm}{close}" + (f", {keys}" if keys else "") + ")"
    return text[: comments.end()] + lead.group(0).lstrip() + call + text[end:]


def migrate_text(text: str) -> tuple[str, int]:
    """The text with every placeable clause rewritten, and how many were not."""
    skipped = 0
    at = 0
    while True:
        match = CLAUSE.search(text, at)
        if match is None:
            return text, skipped
        new = migrate_one(text, match)
        if new is None:
            skipped += 1
            at = match.end()
        else:
            text = new


def literal(value: str) -> str:
    body = value.replace(BACKSLASH, BACKSLASH * 2)
    body = body.replace(NEWLINE, BACKSLASH + "n").replace("\t", BACKSLASH + "t")
    if '"' in body:
        return "'" + body.replace("'", BACKSLASH + "'") + "'"
    return '"' + body + '"'


def quote(value: str, original: str, indent: int) -> str:
    """A literal for `value` in the style of the one it replaces: triple-quoted,
    or one string per source line of an implicit concatenation."""
    if original.startswith(('"""', "'''")) and BACKSLASH not in value:
        triple = "'''" if '"""' in value else '"""'
        return f"{triple}{value}{triple}"
    if NEWLINE not in original:
        return literal(value)
    pieces: list[str] = []
    line = ""
    for word in BREAKS.split(value):
        if line and (len(line) + len(word) > 78 or line.endswith(NEWLINE)):
            pieces.append(line)
            line = ""
        line += word
    pieces.append(line)
    return (NEWLINE + " " * indent).join(literal(piece) for piece in pieces if piece)


def migrate_python(source: str) -> tuple[str, int]:
    tree = ast.parse(source)
    offsets = [0]
    for line in source.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line.encode("utf-8")))
    raw = source.encode("utf-8")
    edits: list[tuple[int, int, str]] = []
    skipped = 0
    formatted = {
        id(v) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr) for v in n.values
    }
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        if id(node) in formatted or not CLAUSE.search(node.value):
            continue
        new, missed = migrate_text(node.value)
        skipped += missed
        if new == node.value or node.end_lineno is None:
            continue
        begin = offsets[node.lineno - 1] + node.col_offset
        end = offsets[node.end_lineno - 1] + (node.end_col_offset or 0)
        original = raw[begin:end].decode("utf-8")
        edits.append((begin, end, quote(new, original, node.col_offset)))
    for begin, end, text in sorted(edits, reverse=True):
        raw = raw[:begin] + text.encode("utf-8") + raw[end:]
    return raw.decode("utf-8"), skipped


def main(paths: list[str]) -> None:
    for name in paths:
        path = Path(name)
        raw = path.read_bytes().decode("utf-8")
        newline = "\r\n" if "\r\n" in raw else NEWLINE
        source = raw.replace("\r\n", NEWLINE)
        migrate = migrate_python if path.suffix == ".py" else migrate_text
        new, _ = migrate(source)
        if new != source:
            path.write_bytes(new.replace(NEWLINE, newline).encode("utf-8"))
        left = len(CLAUSE.findall(new))
        print(f"{name}: {'rewritten' if new != source else 'unchanged'}, {left} left")


if __name__ == "__main__":
    main(sys.argv[1:])
