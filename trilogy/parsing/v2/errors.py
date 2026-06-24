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
    103: (
        "Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping "
        "is automatic by the non-aggregated fields in your SELECT. To aggregate "
        "at a different grain than the select, write `agg(x) by dim1, dim2` "
        "inline (e.g. `sum(sales.amount) by sales.store.id`)."
    ),
    104: (
        "Definition or statement after WHERE/SELECT? Concept definitions "
        "(`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and "
        "`import` are top-level statements and must appear BEFORE the "
        "`where`/`select` block — they cannot sit inside a query. Move this "
        "statement above your `where`, and make sure each statement ends with "
        "`;`. Example: put `auto x <- sum(sales.amount) by store.id;` above "
        "`where ... select ...`."
    ),
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    203: "Missing assignment operator '<-' and expression in derivation. Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`). Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
    211: "Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.",
    212: (
        "A `by <grain>` clause must attach directly to an aggregate, not to an "
        "expression that wraps one (e.g. `coalesce(...)`, `round(...)`, "
        "arithmetic). Move the grain inside, next to the aggregate — write "
        "`coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate "
        "first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`."
    ),
    220: (
        "Filter or stray clause after a `join`? A query-scoped join "
        "`inner|left|full join <a> = <b>` takes only key equalities — to join on "
        "multiple keys, chain `= c` (one equivalence group) or separate distinct "
        "groups with `and` (`a = b and c = d`); STACK another `join` clause for a "
        "different join type. Note `and` joins KEY EQUALITIES only, not filters. "
        "Joins go right after the `select` list (preferred, SQL-like) or before "
        "`select`; the order is `where` -> `select` <cols> -> join(s) -> `having` "
        "-> `order by` -> `limit`. Filter input rows in `where` (before `select`); "
        "filter a joined or aggregated RESULT in `having` (select the field, hide "
        "it with a leading `--`). Full reference: "
        "`trilogy agent-info syntax example query-structure`."
    ),
    221: (
        "Align groups are separated by `and`, not commas. Each `align` group is "
        "`<name>: <colA>, <colB>` (one column per merge arm); join multiple groups "
        "with `and` — e.g. `align item: a_item, b_item and store: a_store, b_store`. "
        "A comma here does not start a new group, so the previous group consumed this "
        "name as one of its columns."
    ),
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


_GROUP_BY_RE = re.compile(r"\bgroup\s+by\b", re.IGNORECASE)


def detect_group_by(text: str, pos: int) -> int | None:
    """Locate a SQL-style `GROUP BY` clause at ``pos``. Returns the position of
    the `group` keyword, or None. Both backends report the error right at the
    offending `group`, so scan a small window around ``pos``. Shared by both
    grammar backends."""
    window = text[max(0, pos - 2) : pos + 12]
    m = _GROUP_BY_RE.search(window)
    if m is None:
        return None
    return max(0, pos - 2) + m.start()


# Top-level statement keywords (followed by their name) that cannot appear
# inside a query body — the most common is `auto NAME <- ...` placed after the
# `where`. Both backends report the error at/just before the keyword.
_DEFINITION_HEAD_RE = re.compile(
    r"\b(auto|property|key|metric|rowset|def|datasource|import|persist)\b\s+[A-Za-z_.]",
    re.IGNORECASE,
)
# A query clause; once one opens, we are inside a query statement.
_QUERY_CLAUSE_RE = re.compile(
    r"\b(where|select|having|order|limit|group|merge|align)\b", re.IGNORECASE
)


def detect_definition_after_clause(text: str, pos: int) -> int | None:
    """Locate a top-level definition/statement keyword (`auto NAME`, `import …`,
    etc.) that sits *inside* an already-opened query (after a `where`/`select`
    in the same `;`-delimited statement). Returns the keyword position, or None.
    Shared by both grammar backends; both report the error at or just before the
    keyword, so scan a small window around ``pos``."""
    m = _DEFINITION_HEAD_RE.search(text, max(0, pos - 2), pos + 14)
    if m is None or m.start() > pos + 2:
        return None
    stmt_start = text.rfind(";", 0, m.start()) + 1
    if _QUERY_CLAUSE_RE.search(text, stmt_start, m.start()) is None:
        return None
    return m.start()


_JOIN_CLAUSE_RE = re.compile(
    r"\b(?:inner|left|right|full|cross)\s+join\b", re.IGNORECASE
)
_POST_JOIN_CONTINUATION_RE = re.compile(r"\b(?:and|or|where|having)\b", re.IGNORECASE)


def detect_clause_after_join(text: str, pos: int) -> int | None:
    """Locate a filter/WHERE continuation placed AFTER a query-scoped `join`
    clause (e.g. `... inner join a = b and c > 0 select ...`). A join may only
    be followed by another join or `select`; conditions belong in a single WHERE
    clause before the join. Returns the offending join clause's position, or
    None. Shared by both grammar backends."""
    stmt_start = text.rfind(";", 0, pos) + 1
    joins = list(_JOIN_CLAUSE_RE.finditer(text, stmt_start, pos))
    if not joins:
        return None
    # The failure may land ON the continuation keyword (`where`/`or`) or, now
    # that `and` separates distinct join groups, just AFTER an `and` that
    # introduced a filter rather than another key equality — so scan a little
    # before pos as well to catch the preceding `and`/`where`/`or`/`having`.
    window = text[max(stmt_start, pos - 6) : pos + 8]
    if _POST_JOIN_CONTINUATION_RE.search(window) is None:
        return None
    return joins[-1].start()


_ALIGN_RE = re.compile(r"\balign\b", re.IGNORECASE)
# A second align group introduced by a comma instead of `and`: `, <name> :`.
# Inside an align item commas only separate bare value identifiers, so a comma
# followed by `<ident>:` is always a misused group separator.
_ALIGN_COMMA_GROUP_RE = re.compile(r",\s*[a-zA-Z_]\w*\s*:")


def detect_align_missing_and(text: str, pos: int) -> int | None:
    """Locate a multi-select `align` group separated by a comma instead of `and`
    (`align a: x, y, b: p` should be `align a: x, y and b: p`). The first align
    item greedily eats `b` as a value, then fails at the `:`. Returns the
    offending comma's position, or None. Shared by both grammar backends."""
    stmt_start = text.rfind(";", 0, pos) + 1
    aligns = list(_ALIGN_RE.finditer(text, stmt_start, pos + 1))
    if not aligns:
        return None
    floor = aligns[-1].end()
    best: int | None = None
    for m in _ALIGN_COMMA_GROUP_RE.finditer(text, floor, pos + 8):
        # pest points just before the colon; keep the match nearest the failure.
        if m.end() >= pos - 2:
            best = m.start()
            break
    return best


_AGG_NAMES = (
    "count_distinct",
    "array_agg",
    "stddev",
    "variance",
    "bool_or",
    "bool_and",
    "count",
    "sum",
    "max",
    "min",
    "avg",
    "any",
)
_AGG_CALL_RE = re.compile(r"\b(?:" + "|".join(_AGG_NAMES) + r")\s*\(", re.IGNORECASE)
_BY_NEAR_RE = re.compile(r"\bby\b", re.IGNORECASE)


def detect_by_on_wrapped_aggregate(text: str, pos: int) -> int | None:
    """Locate a `by <grain>` clause attached to an expression that *wraps* an
    aggregate rather than to the aggregate itself — e.g.
    `coalesce(sum(x), 0) by store.id`. The grain may only follow a bare aggregate
    (`sum(x) by store.id`), so the parser chokes on the `by` once it sits after
    the wrapping call's `)`. Returns the offending `by` position, or None. Shared
    by both grammar backends; purely textual (no reparse)."""
    m = _BY_NEAR_RE.search(text, max(0, pos - 2), pos + 6)
    if m is None:
        return None
    by_pos = m.start()
    # The char immediately before `by` (skipping spaces) must close a call.
    i = by_pos - 1
    while i >= 0 and text[i].isspace():
        i -= 1
    if i < 0 or text[i] != ")":
        return None
    # Match that `)` back to its `(`.
    depth = 0
    j = i
    while j >= 0:
        if text[j] == ")":
            depth += 1
        elif text[j] == "(":
            depth -= 1
            if depth == 0:
                break
        j -= 1
    if j < 0:
        return None
    open_paren = j
    # Read the wrapping function name preceding `(`.
    k = open_paren - 1
    while k >= 0 and text[k].isspace():
        k -= 1
    name_end = k + 1
    while k >= 0 and (text[k].isalnum() or text[k] == "_"):
        k -= 1
    func = text[k + 1 : name_end].lower()
    # No wrapper name, or the call IS the aggregate (`sum(x) by ...` is valid) —
    # not this mistake.
    if not func or func in _AGG_NAMES:
        return None
    # The wrapped expression must actually contain an aggregate.
    if _AGG_CALL_RE.search(text, open_paren + 1, i) is None:
        return None
    return by_pos


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


_IDENT_RE = re.compile(r"[a-zA-Z_]\w*")
_FUNC_CALL_RE = re.compile(r"^([a-zA-Z_]\w*)\s*\((.*)\)$", re.DOTALL)


_SELECT_BOUNDARY_RE = re.compile(
    r"(where|having|order|group|limit|as)\b", re.IGNORECASE
)


def _unaliased_select_expr(text: str, pos: int) -> str | None:
    """Extract the select item that is missing its alias. The two backends
    report ``pos`` at different offsets (pest at the expr, lark past the next
    token), so anchor on ``pos`` only to pick the item, then bound it by
    structure: start at the nearest depth-0 comma / `select`, end at the first
    depth-0 clause boundary (`,`, `;`, or a where/having/order/group/limit/as
    keyword). Depth tracking survives commas inside `grouping_id(a, b)`."""
    head = text[:pos]
    sel = None
    for m in re.finditer(r"\bselect\b", head, re.IGNORECASE):
        sel = m
    floor = sel.end() if sel else 0
    start = floor
    depth = 0
    for i in range(pos - 1, floor - 1, -1):
        c = text[i]
        if c in ")]":
            depth += 1
        elif c in "([":
            depth -= 1
        elif c == "," and depth <= 0:
            start = i + 1
            break
    depth = 0
    end = len(text)
    j = start
    while j < len(text):
        c = text[j]
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif depth == 0:
            if c in ",;":
                end = j
                break
            at_word_start = j == start or (
                not text[j - 1].isalnum() and text[j - 1] != "_"
            )
            if at_word_start and _SELECT_BOUNDARY_RE.match(text, j):
                end = j
                break
        j += 1
    expr = text[start:end].strip().lstrip("-").strip()
    return expr or None


def _suggest_alias(expr: str) -> str:
    """A safe snake_case alias for an unaliased select expression. For
    `count(store_sales.ext_sales_price)` -> `ext_sales_price_count`; otherwise a
    sanitized fallback."""
    m = _FUNC_CALL_RE.match(expr)
    if m:
        func = m.group(1).lower()
        idents = _IDENT_RE.findall(m.group(2))
        if idents:
            return f"{idents[-1]}_{func}".lower()
        return func
    sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", expr).strip("_").lower()
    return sanitized[:40] or "value"


def create_syntax_error(code: int, pos: int, text: str) -> InvalidSyntaxException:
    message = ERROR_CODES[code]
    if code == 201:
        expr = _unaliased_select_expr(text, pos)
        if expr is not None:
            message += f" Here: `{expr} as {_suggest_alias(expr)}`"
    return InvalidSyntaxException(
        f"Syntax [{code}]: "
        + message
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
