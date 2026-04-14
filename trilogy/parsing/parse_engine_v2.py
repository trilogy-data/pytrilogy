from __future__ import annotations

import re
from datetime import datetime
from logging import getLogger
from os.path import dirname, join
from pathlib import Path
from re import IGNORECASE
from typing import Any

from lark import Lark, Token
from lark.exceptions import (
    UnexpectedCharacters,
    UnexpectedEOF,
    UnexpectedInput,
    UnexpectedToken,
)

from trilogy.constants import CONFIG, ParserBackend, Parsing
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.hydration import HydrationContext, NativeHydrator
from trilogy.parsing.v2.syntax import (
    SyntaxDocument,
    SyntaxNode,
    SyntaxToken,
    syntax_document_from_parser,
)

perf_logger = getLogger("trilogy.parse.performance")

MAX_PARSE_DEPTH = 10

ERROR_CODES: dict[int, str] = {
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
}

DEFAULT_ERROR_SPAN: int = 30

# Lark only captures comments via PARSE_COMMENT at three positions:
#   start: ( block | show_statement | PARSE_COMMENT )*
#   block: ... PARSE_COMMENT*
#   inline_property_list: inline_property ("," PARSE_COMMENT? inline_property)* ...
# Anywhere else, COMMENT is `%ignore`d and silently dropped. The pest tree must
# match: only attach comments inside these node types or to a gobbler sibling.
_COMMENT_GOBBLERS: frozenset[str] = frozenset(
    {"start", "block", "inline_property_list"}
)

__all__ = [
    "PARSER",
    "SyntaxDocument",
    "SyntaxNode",
    "SyntaxToken",
    "TopLevelStatementParser",
    "parse_syntax",
    "parse_text",
    "parse_text_raw",
]


_PARSER: Lark | None = None


def _get_parser() -> Lark:
    global _PARSER
    if _PARSER is None:
        with open(join(dirname(__file__), "trilogy.lark"), "r") as f:
            _PARSER = Lark(
                f.read(),
                start="start",
                propagate_positions=True,
                g_regex_flags=IGNORECASE,
                parser="lalr",
                cache=True,
            )
    return _PARSER


class _LazyParser:
    def __getattr__(self, name: str) -> Any:
        return getattr(_get_parser(), name)


PARSER = _LazyParser()


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
            return _create_syntax_error(101, pos, text)

    # 202: trailing-terminator missing. Check only when the error position
    # is at or past the last non-whitespace character — otherwise we'd mask
    # real mid-stream failures by prematurely terminating the statement.
    if text[pos:].strip() == "" and _pest_parses(text + ";"):
        return _create_syntax_error(202, pos, text)

    # 201: missing `as` before an alias identifier. Truncate the tail and
    # commit a probe alias so we only verify that *this* position could accept
    # AS + IDENTIFIER — mirrors lark's feed_token(AS) / feed_token(IDENTIFIER)
    # check. Inserting rather than truncating over-rejects chained aliases
    # like `SELECT a b c` where more than one alias is missing.
    if _pest_parses(text[:pos] + "as trilogy_alias_probe;"):
        return _create_syntax_error(201, pos, text)

    return _create_generic_syntax_error(raw_error, pos, text)


def _parse_syntax_pest(text: str) -> SyntaxDocument:
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


def _parse_syntax_lark(text: str) -> SyntaxDocument:
    try:
        return syntax_document_from_parser(text=text, tree=_get_parser().parse(text))
    except UnexpectedToken as e:
        _handle_unexpected_token(e, text)
        raise  # unreachable — _handle_unexpected_token always raises
    except (UnexpectedCharacters, UnexpectedEOF, UnexpectedInput) as e:
        raise _create_generic_syntax_error(str(e), e.pos_in_stream or 0, text) from e


def parse_syntax(text: str) -> SyntaxDocument:
    if CONFIG.parser_backend == ParserBackend.PEST:
        return _parse_syntax_pest(text)
    return _parse_syntax_lark(text)


def parse_text_raw(text: str, environment: Environment | None = None) -> None:
    _ = environment
    parse_syntax(text)


class TopLevelStatementParser:
    def __init__(
        self,
        environment: Environment,
        parse_address: str | None = None,
        token_address: Path | str | None = None,
        parsed: dict[str, Any] | None = None,
        tokens: dict[Path | str, SyntaxDocument] | None = None,
        text_lookup: dict[Path | str, str] | None = None,
        environment_lookup: dict[str, Environment] | None = None,
        import_keys: list[str] | None = None,
        parse_config: Parsing | None = None,
        max_parse_depth: int = MAX_PARSE_DEPTH,
    ) -> None:
        _ = parsed, tokens
        self.hydrator = NativeHydrator(
            HydrationContext(
                environment=environment,
                parse_address=parse_address or "root",
                token_address=token_address or "root",
                parse_config=parse_config,
                max_parse_depth=max_parse_depth,
            )
        )
        if import_keys:
            self.hydrator.import_keys = import_keys
        if environment_lookup:
            self.hydrator.parsed_environments = environment_lookup
        if text_lookup:
            self.hydrator.text_lookup = text_lookup

    @property
    def environment(self) -> Environment:
        return self.hydrator.environment

    def parse(self, document: SyntaxDocument) -> list[Any]:
        return self.hydrator.parse(document)


def _create_syntax_error(
    code: int,
    pos: int,
    text: str,
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        f"Syntax [{code}]: "
        + ERROR_CODES[code]
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def _create_generic_syntax_error(
    message: str,
    pos: int,
    text: str,
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        message
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def _handle_unexpected_token(e: UnexpectedToken, text: str) -> None:
    pos = e.pos_in_stream or 0
    if e.interactive_parser.lexer_thread.state:
        last_token = e.interactive_parser.lexer_thread.state.last_token
    else:
        last_token = None

    parsed_tokens = [x.value for x in e.token_history if x] if e.token_history else []
    if parsed_tokens == ["FROM"]:
        raise _create_syntax_error(101, pos, text)

    if last_token and e.token.type == "$END":
        try:
            e.interactive_parser.feed_token(Token("_TERMINATOR", ";"))
            state = e.interactive_parser.lexer_thread.state
            if state and state.last_token:
                new_pos = state.last_token.end_pos or pos
            else:
                new_pos = pos
            raise _create_syntax_error(202, new_pos, text)
        except UnexpectedToken:
            pass

    try:
        e.interactive_parser.feed_token(Token("AS", "AS"))
        state = e.interactive_parser.lexer_thread.state
        if state and state.last_token:
            new_pos = state.last_token.end_pos or pos
        else:
            new_pos = pos
        e.interactive_parser.feed_token(Token("IDENTIFIER", e.token.value))
        raise _create_syntax_error(201, new_pos, text)
    except UnexpectedToken:
        pass

    raise _create_generic_syntax_error(str(e), pos, text)


def inject_context_maker(pos: int, text: str, span: int = 40) -> str:
    start = max(pos - span, 0)
    end = pos + span
    before = text[start:pos].rsplit("\n", 1)[-1]
    after = text[pos:end].split("\n", 1)[0]
    rcap = ""
    if after and not after[-1].isspace() and not (end > len(text)):
        rcap = "..."
    lcap = ""
    if start > 0 and not before[0].isspace():
        lcap = "..."
    lpad = " "
    rpad = " "
    if before.endswith(" "):
        lpad = ""
    if after.startswith(" "):
        rpad = ""
    return f"{lcap}{before}{lpad}???{rpad}{after}{rcap}"


def parse_text(
    text: str,
    environment: Environment | None = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
) -> tuple[Environment, list[Any]]:
    environment = environment or (
        Environment(working_path=root) if root else Environment()
    )
    parser = TopLevelStatementParser(
        environment=environment,
        import_keys=["root"],
        parse_config=parse_config or CONFIG.parsing,
    )
    start = datetime.now()

    try:
        document = parse_syntax(text)
        output = parser.parse(document)
        environment.concepts.fail_on_missing = True
        end = datetime.now()
        perf_logger.debug(
            f"Parse time: {end - start} for {len(text)} characters, {len(output)} objects"
        )
    except SyntaxError as e:
        raise InvalidSyntaxException(str(e)).with_traceback(e.__traceback__)

    return environment, output
