from __future__ import annotations

import re
from bisect import bisect_right

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import create_generic_syntax_error, create_syntax_error
from trilogy.parsing.v2.syntax import (
    LARK_NODE_KIND,
    LARK_TOKEN_KIND,
    SyntaxDocument,
    SyntaxElement,
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


def _append_comment_tokens(
    dest: list[SyntaxElement],
    spans: list[tuple[int, int]],
    text: str,
    line_starts: list[int],
) -> None:
    for s, e in spans:
        line_idx = bisect_right(line_starts, s) - 1
        end_line_idx = bisect_right(line_starts, e) - 1
        dest.append(
            SyntaxToken(
                "PARSE_COMMENT",
                text[s:e],
                line_idx + 1,
                s - line_starts[line_idx] + 1,
                end_line_idx + 1,
                e - line_starts[end_line_idx] + 1,
                s,
                e,
                SyntaxTokenKind.COMMENT,
            )
        )


def _tuple_to_syntax(
    element: tuple,
    text: str,
    line_starts: list[int],
    _node_cls: type = SyntaxNode,
    _token_cls: type = SyntaxToken,
    _node_kind: dict = LARK_NODE_KIND,
    _token_kind: dict = LARK_TOKEN_KIND,
    _gobblers: frozenset = _COMMENT_GOBBLERS,
) -> SyntaxElement:
    """Single-pass walk over rust tuple output: builds `SyntaxNode`/`SyntaxToken`
    and injects comment tokens at gobbler boundaries.

    Tuple layout from `parse_trilogy_syntax_tuple`:
      node:  (name, children_tuple, line, col, end_line, end_col, start_pos, end_pos)
      token: (name, value_str,      line, col, end_line, end_col, start_pos, end_pos)
    Position fields are stored directly on SyntaxNode/SyntaxToken — there is
    no SyntaxMeta allocation. Default-arg bindings hoist class and dict
    lookups into fast locals.
    """
    name = element[0]
    second = element[1]
    if type(second) is str:
        return _token_cls(
            name,
            second,
            element[2],
            element[3],
            element[4],
            element[5],
            element[6],
            element[7],
            _token_kind.get(name),
        )

    children: list[SyntaxElement] = []
    node_start = element[6]
    node_end = element[7]
    is_gobbler = name in _gobblers
    cursor = node_start
    prev_gobbler_children: list[SyntaxElement] | None = None

    for child in second:
        cs = child[6]
        if cs > cursor:
            spans = _scan_comments(text, cursor, cs)
            if spans:
                if prev_gobbler_children is not None:
                    _append_comment_tokens(
                        prev_gobbler_children, spans, text, line_starts
                    )
                elif is_gobbler:
                    _append_comment_tokens(children, spans, text, line_starts)
        sub = _tuple_to_syntax(child, text, line_starts)
        children.append(sub)
        cursor = child[7]
        if type(child[1]) is tuple and child[0] in _gobblers:
            prev_gobbler_children = sub.children  # type: ignore[union-attr]
        else:
            prev_gobbler_children = None

    if cursor < node_end:
        trailing = _scan_comments(text, cursor, node_end)
        if trailing:
            if prev_gobbler_children is not None:
                _append_comment_tokens(
                    prev_gobbler_children, trailing, text, line_starts
                )
            elif is_gobbler:
                _append_comment_tokens(children, trailing, text, line_starts)

    return _node_cls(
        name,
        children,
        element[2],
        element[3],
        element[4],
        element[5],
        node_start,
        node_end,
        _node_kind.get(name),
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
    from _preql_import_resolver import parse_trilogy_syntax_tuple

    try:
        parse_trilogy_syntax_tuple(text)
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
    from _preql_import_resolver import parse_trilogy_syntax_tuple

    try:
        tree = parse_trilogy_syntax_tuple(text)
    except ValueError as e:
        raise _diagnose_pest_error(text, str(e)) from e
    line_starts = _compute_line_starts(text)
    syntax = _tuple_to_syntax(tree, text, line_starts)
    if not isinstance(syntax, SyntaxNode):
        raise InvalidSyntaxException("pest root element is not a SyntaxNode")
    return SyntaxDocument(text=text, tree=syntax)
