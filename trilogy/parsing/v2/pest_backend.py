from __future__ import annotations

import re
from bisect import bisect_right
from typing import Any

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import create_generic_syntax_error, create_syntax_error
from trilogy.parsing.v2.syntax import (
    LARK_NODE_KIND,
    LARK_TOKEN_KIND,
    SyntaxDocument,
    SyntaxElement,
    SyntaxMeta,
    SyntaxNode,
    SyntaxToken,
    SyntaxTokenKind,
)

# Lark only captures comments via PARSE_COMMENT at three positions:
#   start: ( block | show_statement | PARSE_COMMENT )*
#   block: ... PARSE_COMMENT*
#   inline_property_list: inline_property ("," PARSE_COMMENT? inline_property)* ...
# Anywhere else, COMMENT is `%ignore`d and silently dropped. Pest also drops
# comments at the grammar level, so the fused walker replicates lark's
# gobbler behavior by re-scanning source gaps and injecting synthetic COMMENT
# tokens into the nearest gobbler child or current gobbler node.
_COMMENT_GOBBLERS: frozenset[str] = frozenset(
    {"start", "block", "inline_property_list"}
)


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


def _compute_line_starts(text: str) -> list[int]:
    starts = [0]
    for i, ch in enumerate(text):
        if ch == "\n":
            starts.append(i + 1)
    return starts


def _make_comment_token(
    text: str, line_starts: list[int], start: int, end: int
) -> SyntaxToken:
    line_idx = bisect_right(line_starts, start) - 1
    end_line_idx = bisect_right(line_starts, end) - 1
    return SyntaxToken(
        name="PARSE_COMMENT",
        value=text[start:end],
        meta=SyntaxMeta(
            line=line_idx + 1,
            column=start - line_starts[line_idx] + 1,
            end_line=end_line_idx + 1,
            end_column=end - line_starts[end_line_idx] + 1,
            start_pos=start,
            end_pos=end,
        ),
        kind=SyntaxTokenKind.COMMENT,
    )


def _attach_comments(
    spans: list[tuple[int, int]],
    target_children: list[SyntaxElement] | None,
    host_is_gobbler: bool,
    current_children: list[SyntaxElement],
    text: str,
    line_starts: list[int],
) -> None:
    if target_children is not None:
        dest: list[SyntaxElement] = target_children
    elif host_is_gobbler:
        dest = current_children
    else:
        return
    for start, end in spans:
        dest.append(_make_comment_token(text, line_starts, start, end))


def _pest_to_syntax(
    element: Any,
    text: str,
    line_starts: list[int],
) -> SyntaxElement:
    """Single-pass walk: pest node/token -> SyntaxNode/SyntaxToken with
    comment tokens injected at gobbler boundaries.

    This fuses the old `_walk_inject` + `syntax_from_parser` passes. The
    recursion appends children to a mutable list so trailing comments in the
    parent scope can still be attached to a gobbler child after its recursive
    build returns.
    """
    data = getattr(element, "data", None)
    if data is None:
        token_type = element.type
        return SyntaxToken(
            name=token_type,
            value=element.value,
            meta=SyntaxMeta.from_parser_meta(element),
            kind=LARK_TOKEN_KIND.get(token_type),
        )

    children: list[SyntaxElement] = []
    node_start = getattr(element, "start_pos", None)
    node_end = getattr(element, "end_pos", None)
    have_range = node_start is not None and node_end is not None
    is_gobbler = data in _COMMENT_GOBBLERS
    cursor = node_start if node_start is not None else 0
    prev_gobbler_children: list[SyntaxElement] | None = None

    for pest_child in element.children:
        cs = getattr(pest_child, "start_pos", None)
        ce = getattr(pest_child, "end_pos", None)
        if have_range and cs is not None and cs > cursor:
            spans = _scan_comments(text, cursor, cs)
            if spans:
                _attach_comments(
                    spans,
                    prev_gobbler_children,
                    is_gobbler,
                    children,
                    text,
                    line_starts,
                )
        sub = _pest_to_syntax(pest_child, text, line_starts)
        children.append(sub)
        if ce is not None:
            cursor = ce
        child_data = getattr(pest_child, "data", None)
        if (
            isinstance(sub, SyntaxNode)
            and child_data is not None
            and child_data in _COMMENT_GOBBLERS
        ):
            prev_gobbler_children = sub.children
        else:
            prev_gobbler_children = None

    if have_range and node_end is not None and cursor < node_end:
        trailing = _scan_comments(text, cursor, node_end)
        if trailing:
            _attach_comments(
                trailing,
                prev_gobbler_children,
                is_gobbler,
                children,
                text,
                line_starts,
            )

    return SyntaxNode(
        name=data,
        children=children,
        meta=SyntaxMeta.from_parser_meta(element),
        kind=LARK_NODE_KIND.get(data),
    )


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
    line_starts = _compute_line_starts(text)
    syntax = _pest_to_syntax(tree, text, line_starts)
    if not isinstance(syntax, SyntaxNode):
        raise InvalidSyntaxException("pest root element is not a SyntaxNode")
    return SyntaxDocument(text=text, tree=syntax)
