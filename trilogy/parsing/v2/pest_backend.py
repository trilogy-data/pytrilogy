from __future__ import annotations

import re
from typing import Any

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import create_generic_syntax_error, create_syntax_error
from trilogy.parsing.v2.syntax import SyntaxDocument, syntax_document_from_parser

# Lark only captures comments via PARSE_COMMENT at three positions:
#   start: ( block | show_statement | PARSE_COMMENT )*
#   block: ... PARSE_COMMENT*
#   inline_property_list: inline_property ("," PARSE_COMMENT? inline_property)* ...
# Anywhere else, COMMENT is `%ignore`d and silently dropped. The pest tree must
# match: only attach comments inside these node types or to a gobbler sibling.
_COMMENT_GOBBLERS: frozenset[str] = frozenset(
    {"start", "block", "inline_property_list"}
)


class _SyntheticPestComment:
    __slots__ = (
        "type",
        "value",
        "line",
        "column",
        "end_line",
        "end_column",
        "start_pos",
        "end_pos",
    )

    def __init__(
        self,
        value: str,
        line: int,
        column: int,
        end_line: int,
        end_column: int,
        start_pos: int,
        end_pos: int,
    ) -> None:
        self.type = "PARSE_COMMENT"
        self.value = value
        self.line = line
        self.column = column
        self.end_line = end_line
        self.end_column = end_column
        self.start_pos = start_pos
        self.end_pos = end_pos

    @property
    def meta(self) -> "_SyntheticPestComment":
        return self


def _scan_comments(text: str, start: int, end: int) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    pos = start
    while pos < end:
        c = text[pos]
        if c.isspace():
            pos += 1
            continue
        if c == "#" or (c == "/" and pos + 1 < end and text[pos + 1] == "/"):
            cmt_start = pos
            nl = text.find("\n", pos, end)
            cmt_end = end if nl == -1 else nl + 1
            spans.append((cmt_start, cmt_end))
            pos = cmt_end
        elif c == "/" and pos + 1 < end and text[pos + 1] == "*":
            cmt_start = pos
            close = text.find("*/", pos + 2, end)
            cmt_end = end if close == -1 else close + 2
            spans.append((cmt_start, cmt_end))
            pos = cmt_end
        else:
            pos += 1
    return spans


def _make_comment(
    text: str, line_starts: list[int], start: int, end: int
) -> _SyntheticPestComment:
    from bisect import bisect_right

    def line_col(pos: int) -> tuple[int, int]:
        idx = bisect_right(line_starts, pos) - 1
        return idx + 1, pos - line_starts[idx] + 1

    line, col = line_col(start)
    end_line, end_col = line_col(end)
    return _SyntheticPestComment(
        value=text[start:end],
        line=line,
        column=col,
        end_line=end_line,
        end_column=end_col,
        start_pos=start,
        end_pos=end,
    )


def _walk_inject(node: Any, text: str, line_starts: list[int]) -> None:
    children = getattr(node, "children", None)
    if children is None:
        return
    snapshot = list(children)
    for child in snapshot:
        _walk_inject(child, text, line_starts)
    node_start = getattr(node, "start_pos", None)
    node_end = getattr(node, "end_pos", None)
    if node_start is None or node_end is None:
        return
    node_is_gobbler = getattr(node, "data", None) in _COMMENT_GOBBLERS
    new_children: list[Any] = []
    cursor = node_start
    prev_gobbler: Any = None
    for child in snapshot:
        cs = getattr(child, "start_pos", None)
        ce = getattr(child, "end_pos", None)
        if cs is None or ce is None:
            new_children.append(child)
            prev_gobbler = None
            continue
        gap = _scan_comments(text, cursor, cs)
        _emit_comments(
            gap, prev_gobbler, node_is_gobbler, new_children, text, line_starts
        )
        new_children.append(child)
        cursor = ce
        if getattr(child, "data", None) in _COMMENT_GOBBLERS:
            prev_gobbler = child
        else:
            prev_gobbler = None
    trailing = _scan_comments(text, cursor, node_end)
    _emit_comments(
        trailing, prev_gobbler, node_is_gobbler, new_children, text, line_starts
    )
    children[:] = new_children


def _emit_comments(
    spans: list[tuple[int, int]],
    prev_gobbler: Any,
    node_is_gobbler: bool,
    new_children: list[Any],
    text: str,
    line_starts: list[int],
) -> None:
    if not spans:
        return
    if prev_gobbler is not None:
        gc = getattr(prev_gobbler, "children", None)
        if gc is not None:
            for csp, cep in spans:
                gc.append(_make_comment(text, line_starts, csp, cep))
            return
    if node_is_gobbler:
        for csp, cep in spans:
            new_children.append(_make_comment(text, line_starts, csp, cep))


def _inject_pest_comments(tree: Any, text: str) -> None:
    if not text:
        return
    line_starts = [0]
    for i, ch in enumerate(text):
        if ch == "\n":
            line_starts.append(i + 1)
    _walk_inject(tree, text, line_starts)


_PEST_ERROR_POS_RE = re.compile(r"-->\s*(\d+):(\d+)")


def _pest_error_char_pos(raw_error: str, text: str) -> int:
    m = _PEST_ERROR_POS_RE.search(raw_error)
    if not m:
        return 0
    line = int(m.group(1))
    col = int(m.group(2))
    pos = 0
    for _ in range(line - 1):
        nl = text.find("\n", pos)
        if nl < 0:
            return len(text)
        pos = nl + 1
    return min(pos + col - 1, len(text))


def _pest_parses(text: str) -> bool:
    from _preql_import_resolver import parse_trilogy_syntax

    try:
        parse_trilogy_syntax(text)
        return True
    except ValueError:
        return False


def _diagnose_pest_error(text: str, raw_error: str) -> InvalidSyntaxException:
    # Replicates lark's rich error codes via source-level fixup probes.
    # Pest has no interactive-parser recovery, so instead of feeding synthetic
    # tokens we mutate the source text and reparse — same outcome, at the
    # cost of 1–3 extra pest parses on the error path (still sub-ms).
    pos = _pest_error_char_pos(raw_error, text)

    # 101: user typed `FROM` where trilogy doesn't accept it.
    if text[pos : pos + 4].upper() == "FROM":
        trailing = text[pos + 4 : pos + 5]
        if not trailing or not (trailing.isalnum() or trailing == "_"):
            return create_syntax_error(101, pos, text)

    # 202: trailing-terminator missing. Check only when the error position
    # is at or past the last non-whitespace character — otherwise we'd mask
    # real mid-stream failures by prematurely terminating the statement.
    if text[pos:].strip() == "" and _pest_parses(text + ";"):
        return create_syntax_error(202, pos, text)

    # 201: missing `as` before an alias identifier. Truncate the tail and
    # commit a probe alias so we only verify that *this* position could accept
    # AS + IDENTIFIER — mirrors lark's feed_token(AS) / feed_token(IDENTIFIER)
    # check. Inserting rather than truncating over-rejects chained aliases
    # like `SELECT a b c` where more than one alias is missing.
    if _pest_parses(text[:pos] + "as trilogy_alias_probe;"):
        return create_syntax_error(201, pos, text)

    return create_generic_syntax_error(raw_error, pos, text)


def parse_pest(text: str) -> SyntaxDocument:
    # ImportError from the lazy import bubbles up to parse_syntax so the dispatcher
    # can fall back to lark when the rust wheel isn't installed. Pest parse
    # failures arrive as pyo3 ValueError; we normalize them here and run the
    # rich-error probe so users see the same 101/201/202 codes as under lark.
    from _preql_import_resolver import parse_trilogy_syntax

    try:
        tree = parse_trilogy_syntax(text)
    except ValueError as e:
        raise _diagnose_pest_error(text, str(e)) from e
    _inject_pest_comments(tree, text)
    return syntax_document_from_parser(text=text, tree=tree)
