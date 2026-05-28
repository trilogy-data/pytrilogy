from __future__ import annotations

import re

from trilogy.core.exceptions import InvalidSyntaxException

ERROR_CODES: dict[int, str] = {
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    102: (
        "Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does "
        "not support subqueries — joins are auto-resolved from dotted paths. "
        "To filter on a value that lives on a related dimension, reference its "
        "dot-path directly. Example: instead of "
        "`where ss.store_id in (select store_id where store.state = 'TN')`, "
        "write `where ss.store.state = 'TN'`."
    ),
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    203: "Missing assignment operator '<-' and expression in derivation. Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`). Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
    211: "Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.",
}


_SUBSELECT_RE = re.compile(r"\(\s*(select|with)\b", re.IGNORECASE)


def detect_subselect(text: str, pos: int) -> int | None:
    """Locate a SQL-style subselect — `(select ...` or `(with ...` — that is
    still open at ``pos``. Returns the position of the opening paren, or None
    if no such pattern is in scope. Shared by both grammar backends.

    The pest backend reports the error position right at the offending
    keyword (e.g. on the `s` of `select`), so we scan a small window past
    ``pos`` as well and find the nearest enclosing match."""
    last = None
    scan_end = min(len(text), pos + 16)
    for m in _SUBSELECT_RE.finditer(text, 0, scan_end):
        if m.start() > pos:
            break
        last = m
    if last is None:
        return None
    open_paren = last.start()
    depth = 0
    closed_before_pos = False
    for i in range(open_paren, min(len(text), pos)):
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                closed_before_pos = True
                break
    if closed_before_pos:
        return None
    return open_paren


DEFAULT_ERROR_SPAN: int = 30


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


def create_syntax_error(code: int, pos: int, text: str) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        f"Syntax [{code}]: "
        + ERROR_CODES[code]
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def create_generic_syntax_error(
    message: str, pos: int, text: str
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        message
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )
