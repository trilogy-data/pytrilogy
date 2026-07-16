from __future__ import annotations

import re

from trilogy.core.exceptions import InvalidSyntaxException

ERROR_CODES: dict[int, str] = {
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    102: (
        "Using a SQL-style CTE (`(with ... as ...)`) inside a query? Trilogy "
        "supports inline `(select ...)` subqueries (single aliased column), but "
        "not parenthesized `with` CTEs. Define a named `rowset <name> <- ...;` "
        "(or `with <name> as ...;`) as a top-level statement above the query and "
        "reference its output, or - to filter on a related dimension - use its "
        "dot-path directly (e.g. `where ss.store.state = 'TN'`)."
    ),
    103: (
        "Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping "
        "is automatic by the non-aggregated fields in your SELECT. To aggregate "
        "at a different grain than the select, write `agg(x) by dim1, dim2` "
        "inline (e.g. `sum(sales.amount) by sales.store.id`)."
    ),
    104: (
        "Definition or statement after WHERE/SELECT? Concept definitions "
        "(`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and "
        "`import` are top-level statements and must appear BEFORE the "
        "`where`/`select` block - they cannot sit inside a query. Move this "
        "statement above your `where`, and make sure each statement ends with "
        "`;`. Example: put `auto x <- sum(sales.amount) by store.id;` above "
        "`where ... select ...`."
    ),
    105: (
        "A `rowset`/`auto`/`metric`/`property` definition connects its name to "
        "its expression with `<-`, not `as` - write `rowset base <- select ...;` "
        "(a `rowset` may also use the `with base as select ...;` form). For "
        "`auto`/`metric`/`property` only `<-` is valid, e.g. "
        "`auto total <- sum(sales.amount);`."
    ),
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    203: "Missing assignment operator '<-' and expression in derivation. Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`). Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
    211: "Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.",
    212: (
        "A `by <grain>` clause must attach directly to an aggregate, not to an "
        "expression that wraps one (e.g. `coalesce(...)`, `round(...)`, "
        "arithmetic). Move the grain inside, next to the aggregate - write "
        "`coalesce(sum(x) by store.id, 0)` - or compute the grouped aggregate "
        "first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`."
    ),
    213: (
        "A `by <grain>` clause must follow an aggregate, but the expression "
        "before it has none. If the `by` sits inside an aggregate's "
        "parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. "
        "To take each distinct value once per grain, wrap it "
        "in `group(...)` - e.g. `group(item.current_price) by item.id, "
        "item.category`. For a reduction, use an aggregate: `sum(x) by ...`, "
        "`avg(x) by ...`, `max(x) by ...`."
    ),
    220: (
        "Filter or stray clause after a `join`? A query-scoped join "
        "`subset|union join <a> = <b>` takes only key equalities - to join on "
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
        "with `and` - e.g. `align item: a_item, b_item and store: a_store, b_store`. "
        "A comma here does not start a new group, so the previous group consumed this "
        "name as one of its columns."
    ),
    222: (
        "Missing `;` - a named definition must be terminated with a semicolon "
        "before the next statement. Terminate the `union(...) -> (...)` (or "
        "`with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after "
        "its `-> (...)` output signature, then start the consuming `select` on the "
        "next line. Example: `with u as union(...) -> (channel, np); select ...`."
    ),
    223: (
        "`*` is not a valid argument - Trilogy has no `*` row-marker, so "
        "`count(*)` / `sum(*)` don't parse. To count rows at the query grain, "
        "count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) "
        "- e.g. `count(store_sales.id)`; to count a related dimension's rows, "
        "count its key (`count(customer.id)`). It MUST be a key, and one that is "
        "not nullable: `count(x)` skips rows where `x` is NULL, so counting a "
        "nullable property (a name, a date, any optional field) silently "
        "undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: "
        "`count(grain(order_id, item.id))` counts order+item combinations, and "
        "`count_distinct(grain(first_name, last_name, sale_date))` counts distinct "
        "combinations - `grain()` is never NULL, so combinations with a missing "
        "member still count. For any other aggregate, pass the column you mean, e.g. "
        "`sum(store_sales.ext_sales_price)`."
    ),
    224: (
        "Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is "
        "already grouped by its non-aggregate columns, so listing the columns you "
        "want already returns distinct rows. Remove `distinct`: write "
        "`select s.channel, s.channel_dim_text_id` (not "
        "`select distinct s.channel, ...`)."
    ),
    225: (
        "Expected a join condition. A query-scoped `subset|union join` needs a "
        "key equality - write `subset join a.key = b.key` (or `union join a.key "
        "= b.key`). Chain more keys for a composite grain with `= c.key`, and "
        "separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). "
        "Both sides must be real fields or expressions - `...` is not a "
        "placeholder."
    ),
    226: (
        "Misplaced `subset|union join`. The key looks fine - the join is in the "
        "wrong PLACE. A query-scoped join is part of a `select` statement, not a "
        "standalone statement and not a pre-`where` clause. Put it right after "
        "the select list (preferred, SQL-like): `where <filters> select <cols> "
        "subset join a.key = b.key`. The clause order is `where` -> `select` "
        "<cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also "
        "sit between `where` and `select`, but never before `where` and never on "
        "its own. Full reference: `trilogy agent-info syntax example "
        "query-structure`."
    ),
    227: (
        "Named function `{name}` must be invoked with a leading `@` - write "
        "`@{name}(...)`. A user-defined (`def`) function is called with `@`; bare "
        "`{name}(...)` is read as a concept reference, not a call, so it fails at "
        "the `(`. Built-in functions (`sum(...)`, `count(...)`, `coalesce(...)`) "
        "need no `@`."
    ),
}


# A `by rollup/cube/grouping sets` clause placed *after* HAVING is a parse
# error — Trilogy orders the grouping clause *before* HAVING, matching SQL's
# `GROUP BY ... HAVING` (this was the reverse until 2026-07; older queries hit
# an opaque "expected order_by or limit"). Detect the legacy shape to add a
# targeted hint: `having` followed by a grouping clause within the same
# statement (no `;` between). A `by rollup/cube/grouping sets` lead can never
# be an aggregate's own `by` grain (those keywords are reserved), so the match
# is unambiguous. Advisory only — gated behind an actual syntax error.
_GROUPING_AFTER_HAVING = re.compile(
    r"\bhaving\b(?:(?!;).)*?\bby\s+(?:rollup|cube|grouping\s+sets)\b",
    re.IGNORECASE | re.DOTALL,
)


def detect_grouping_after_having(content: str) -> bool:
    return bool(_GROUPING_AFTER_HAVING.search(content))


_SUBSELECT_RE = re.compile(r"\(\s*(with)\b", re.IGNORECASE)


def detect_subselect(text: str, pos: int) -> int | None:
    """Locate a parenthesized SQL-style CTE — `(with ...` — that is still open
    at ``pos``. Returns the position of the opening paren, or None if no such
    pattern is in scope. Shared by both grammar backends.

    Inline `(select ...)` subqueries are supported now (desugared to an
    anonymous rowset), so only the unsupported `(with ... as ...)` CTE form is
    flagged here; a malformed `(select ...)` surfaces its own parse error.

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


_SELECT_DISTINCT_RE = re.compile(r"\bselect\s+(distinct)\b", re.IGNORECASE)


def detect_select_distinct(text: str, pos: int) -> int | None:
    """Locate a SQL-style ``SELECT DISTINCT`` (Trilogy groups by the non-aggregate
    select columns automatically, so distinctness is implicit). Both backends read
    ``distinct`` as a bare expression and fail just after it (on the next column,
    as a missing-alias [201]), so scan from the statement start to just past
    ``pos`` and take the nearest ``select distinct``. Returns the ``distinct``
    keyword position, or None. Shared by both grammar backends."""
    stmt_start = text.rfind(";", 0, pos) + 1
    match = None
    for cand in _SELECT_DISTINCT_RE.finditer(text, stmt_start, pos + 2):
        match = cand
    if match is None:
        return None
    return match.start(1)


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


# A `<-`-connected derivation (`rowset`/`auto`/`metric`/`property`) written with
# the SQL `as` connector instead — e.g. `rowset base as select ...`. These
# keywords bind their name with `<-` (a `rowset` may alternatively use the
# `with <name> as ...` form). Reserved keywords + `<name> as` is unambiguous.
_DERIVATION_AS_RE = re.compile(
    r"\b(auto|metric|property|rowset)\s+[A-Za-z_][\w.]*\s+(as)\b", re.IGNORECASE
)


def detect_derivation_as_connector(text: str, pos: int) -> int | None:
    """Locate a derivation (`rowset`/`auto`/`metric`/`property`) that uses the SQL
    `as` connector instead of `<-` — e.g. `rowset base as select ...`. Both
    backends reject the leading keyword itself, so ``pos`` sits at/near the
    statement start; scan forward from the statement start and require the
    keyword to be the statement's first token. Returns the offending `as`
    position, or None. Shared by both grammar backends; purely textual."""
    stmt_start = text.rfind(";", 0, pos + 1) + 1
    m = _DERIVATION_AS_RE.search(text, stmt_start)
    if m is None or text[stmt_start : m.start()].strip():
        return None
    return m.start(2)


_JOIN_CLAUSE_RE = re.compile(
    r"\b(?:inner|left|right|full|cross)\s+join\b", re.IGNORECASE
)
_POST_JOIN_CONTINUATION_RE = re.compile(r"\b(?:and|or|where|having)\b", re.IGNORECASE)
# A join key is an expression at the `sum_operator` level — below comparison —
# so any comparison/membership operator inside a join clause means a filter was
# misplaced there. When the offending key is itself an expression (`a + 1 = b`),
# the parser consumes the whole key and the failure lands ON the comparison
# operator, too far past the preceding `and` for the keyword window to catch.
_POST_JOIN_FILTER_OP_RE = re.compile(
    r"(>=|<=|!=|>|<|\bis\b|\bin\b|\bnot\b|\blike\b|\bilike\b|\bbetween\b)",
    re.IGNORECASE,
)


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
    if (
        _POST_JOIN_CONTINUATION_RE.search(window) is None
        and _POST_JOIN_FILTER_OP_RE.search(window) is None
    ):
        return None
    return joins[-1].start()


_QUERY_JOIN_RE = re.compile(
    r"\b(?:left|inner|full|right|cross|subset|union)\s+join\b", re.IGNORECASE
)
_SELECT_KW_RE = re.compile(r"\bselect\b", re.IGNORECASE)


def detect_join_missing_key(text: str, pos: int) -> int | None:
    """Locate a query-scoped join whose key expression is missing or malformed -
    e.g. `union join` with no key, `subset join a.id =` with no RHS, or a bare
    `...` placeholder. A join key parses as an expression (`sum_operator`), so
    both backends surface the opaque `expected sum_operator`; map it to a
    join-condition message. Returns the offending join clause's position, or
    None. Shared by both grammar backends."""
    stmt_start = text.rfind(";", 0, pos) + 1
    joins = list(_QUERY_JOIN_RE.finditer(text, stmt_start, pos + 1))
    if not joins:
        return None
    join = joins[-1]
    # A `select` between the join and the failure means the key already parsed
    # and the error is downstream (in the select) — not a missing join key.
    if _SELECT_KW_RE.search(text, join.end(), pos):
        return None
    return join.start()


_CLAUSE_BOUNDARY_RE = re.compile(
    r"\b(?:where|select|having|order|group|limit)\b|;", re.IGNORECASE
)
_WHERE_KW_RE = re.compile(r"\bwhere\b", re.IGNORECASE)


def misplaced_join_candidate(text: str, pos: int) -> tuple[int, str] | None:
    """Locate a query-scoped join that sits in an INVALID grammatical position —
    on its own as a standalone statement, or BEFORE the `where` — rather than
    after the select list (preferred) or between `where` and `select`. The
    grammar is `where? join_clause* select ... join_clause*`, so a join before
    the `where` (or with no `select` at all) fails even when its key is perfectly
    well-formed; without this it mis-reports as a malformed key (225).

    Returns `(join_start, clause_text)` — the caller confirms `clause_text` is a
    well-formed join in isolation via its own reparse probe before emitting 226;
    a malformed key falls through to 225. Returns None when the join is absent or
    already in a valid position. Shared by both grammar backends."""
    stmt_start = text.rfind(";", 0, pos) + 1
    joins = list(_QUERY_JOIN_RE.finditer(text, stmt_start, pos + 1))
    if not joins:
        return None
    join = joins[-1]
    stmt_end = text.find(";", join.end())
    if stmt_end == -1:
        stmt_end = len(text)
    select_after = _SELECT_KW_RE.search(text, join.end(), stmt_end)
    standalone = _SELECT_KW_RE.search(text, stmt_start, stmt_end) is None
    before_where = select_after is not None and bool(
        _WHERE_KW_RE.search(text[join.end() : select_after.start()])
    )
    if not (standalone or before_where):
        return None
    boundary = _CLAUSE_BOUNDARY_RE.search(text, join.end())
    clause_end = boundary.start() if boundary else stmt_end
    return join.start(), text[join.start() : clause_end].strip()


# A `def NAME(...)` (or `def table NAME(...)`) declaration — the named-function
# registry. Both forms are invoked with a leading `@` (`@NAME(...)`); omitting it
# reads the name as a bare concept reference and fails at the `(`.
_DEF_NAME_RE = re.compile(r"\bdef\s+(?:table\s+)?([A-Za-z_]\w*)", re.IGNORECASE)


def _named_functions(text: str, before: int) -> set[str]:
    return {m.group(1).lower() for m in _DEF_NAME_RE.finditer(text, 0, before)}


def detect_named_function_missing_at(text: str, pos: int) -> int | None:
    """Locate a user-defined (`def`) function invoked without its required `@`
    prefix — e.g. `identity(x)` where an earlier `def identity(...)` exists (the
    valid form is `@identity(x)`). Both backends report the failure at the `(`
    that follows the bare name, so find that `(`, read the identifier before it,
    and fire ONLY when the name matches a `def` declared earlier in the source —
    unknown names and built-ins keep their normal diagnostics. Returns the
    position of the function name, or None. Shared by both grammar backends;
    purely textual (no reparse)."""
    paren = None
    for i in range(max(0, pos - 1), min(len(text), pos + 2)):
        if text[i] == "(":
            paren = i
            break
    if paren is None:
        return None
    k = paren - 1
    while k >= 0 and text[k].isspace():
        k -= 1
    name_end = k + 1
    while k >= 0 and (text[k].isalnum() or text[k] == "_"):
        k -= 1
    name_start = k + 1
    name = text[name_start:name_end]
    if not name:
        return None
    # An `@` (skipping spaces) already prefixes the name → well-formed call.
    p = name_start - 1
    while p >= 0 and text[p].isspace():
        p -= 1
    if p >= 0 and text[p] == "@":
        return None
    if name.lower() not in _named_functions(text, name_start):
        return None
    return name_start


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
    # The call IS the aggregate (`sum(x) by ...`) — that's the valid form.
    if func in _AGG_NAMES:
        return None
    # Otherwise the wrapper is a non-aggregate function (`coalesce(...)`) OR a
    # bare parenthetical (`(sum(x) + 1)`, func == "") — either way the `by` is
    # misplaced, but only if an aggregate actually sits inside the wrapper.
    if _AGG_CALL_RE.search(text, open_paren + 1, i) is None:
        return None
    return by_pos


_BY_GROUPING_TAIL_RE = re.compile(r"(rollup|cube|grouping)\b", re.IGNORECASE)
_BY_EXPR_BOUNDARY_RE = re.compile(r"<-|\b(?:select|where|having)\b", re.IGNORECASE)


def _by_expr_start(text: str, end: int) -> int:
    """Start index of the expression term ending at ``end`` (inclusive), for
    scanning it for aggregates. Walk back to the statement `;`, then raise the
    floor to the nearest derivation arrow (`<-`) or `select`/`where`/`having`
    keyword, and stop at the first depth-0 comma or unmatched `(`/`[`."""
    floor = text.rfind(";", 0, end) + 1
    kw = None
    for kw in _BY_EXPR_BOUNDARY_RE.finditer(text, floor, end):
        pass
    if kw is not None:
        floor = kw.end()
    depth = 0
    i = end
    while i >= floor:
        c = text[i]
        if c in ")]":
            depth += 1
        elif c in "([":
            if depth == 0:
                return i + 1
            depth -= 1
        elif depth == 0 and c == ",":
            return i + 1
        i -= 1
    return floor


def detect_by_on_non_aggregate(text: str, pos: int) -> int | None:
    """Locate a `by <grain>` clause attached to an expression that contains NO
    aggregate at all — e.g. `item.current_price by item.id`. The grain may only
    follow an aggregate; to take each distinct value once per grain the
    expression must be wrapped in `group(...)`. Returns the offending `by`
    position, or None. Complements `detect_by_on_wrapped_aggregate` (which
    handles a non-aggregate wrapper that *contains* an aggregate). Shared by both
    grammar backends; purely textual (no reparse)."""
    m = _BY_NEAR_RE.search(text, max(0, pos - 2), pos + 6)
    if m is None:
        return None
    by_pos = m.start()
    # An expression must precede `by` (identifier char or closing paren/bracket).
    i = by_pos - 1
    while i >= 0 and text[i].isspace():
        i -= 1
    if i < 0 or not (text[i].isalnum() or text[i] in "_)]"):
        return None
    # `by rollup/cube/grouping (...)` is a valid SELECT-level grouping clause.
    after = by_pos + 2
    while after < len(text) and text[after].isspace():
        after += 1
    if _BY_GROUPING_TAIL_RE.match(text, after):
        return None
    # No aggregate anywhere in the preceding expression -> the misplaced-grain case.
    if _AGG_CALL_RE.search(text, _by_expr_start(text, i), by_pos) is not None:
        return None
    return by_pos


def detect_star_argument(text: str, pos: int) -> int | None:
    """Locate a `*` passed as the sole argument to a function call — the SQL
    `count(*)` idiom, which Trilogy doesn't support (there is no `*` row-marker).
    Returns the position of the wrapping function name, or None. Both backends
    report the failure right at the `*`, so scan a small window past ``pos`` to
    find it. Shared by both grammar backends; purely textual (no reparse)."""
    star = pos
    while star < len(text) and text[star] in " \t\r\n":
        star += 1
    if star >= len(text) or text[star] != "*":
        return None
    # A lone star: the next non-space char must close the call.
    j = star + 1
    while j < len(text) and text[j] in " \t\r\n":
        j += 1
    if j >= len(text) or text[j] != ")":
        return None
    # The char before the star must open the call.
    k = star - 1
    while k >= 0 and text[k] in " \t\r\n":
        k -= 1
    if k < 0 or text[k] != "(":
        return None
    # A function name must precede the `(`.
    k -= 1
    while k >= 0 and text[k] in " \t\r\n":
        k -= 1
    name_end = k + 1
    while k >= 0 and (text[k].isalnum() or text[k] == "_"):
        k -= 1
    if k + 1 == name_end:
        return None
    return k + 1


_TVF_SIGNATURE_RE = re.compile(r"->\s*\(")


def detect_missing_signature_semicolon(text: str, pos: int) -> int | None:
    """Locate a TVF output signature `-> (cols)` that was not terminated with a
    `;` before the next statement (e.g. `with u as union(...) -> (a, b) select
    ...`). Returns the position just after the signature's closing `)` (where the
    `;` belongs), or None. The two backends report the failure at different
    spots — pest on the signature's `)`, lark on the following statement keyword
    — so anchor on the nearest `-> (` at or before ``pos``. Shared by both
    backends; each confirms with a `;`-insertion reparse before surfacing."""
    sig = None
    for m in _TVF_SIGNATURE_RE.finditer(text, 0, pos + 2):
        sig = m
    if sig is None:
        return None
    open_paren = sig.end() - 1
    depth = 0
    close = None
    for i in range(open_paren, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                close = i
                break
    if close is None:
        return None
    # Next non-space char after the signature: a `;` means it IS terminated, so
    # this isn't the bug; end-of-input is the trailing-terminator case (202).
    nxt = close + 1
    while nxt < len(text) and text[nxt].isspace():
        nxt += 1
    if nxt >= len(text) or text[nxt] == ";":
        return None
    # The failure must sit within the signature or at the following token.
    if pos < open_paren or pos > nxt:
        return None
    return close + 1


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


def _self_contained_parens(inner: str) -> bool:
    depth = 0
    for c in inner:
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def suggest_select_alias(expr: str) -> str:
    """A safe snake_case alias for an unaliased select expression. For
    `count(store_sales.ext_sales_price)` -> `ext_sales_price_count`; otherwise a
    sanitized fallback. Also names anonymous select outputs, so the result is
    always a valid identifier."""
    m = _FUNC_CALL_RE.match(expr)
    # `sum(x) by (a, b)` also ends in `)` — only treat as a single function
    # call when the parens actually wrap the remainder.
    if m and not _self_contained_parens(m.group(2)):
        m = None
    if m:
        func = m.group(1).lower()
        idents = _IDENT_RE.findall(m.group(2))
        if idents:
            return f"{idents[-1]}_{func}".lower()
        return func
    sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", expr).strip("_").lower()
    sanitized = sanitized[:40] or "value"
    if not (sanitized[0].isalpha() or sanitized[0] == "_"):
        sanitized = f"value_{sanitized}"
    return sanitized


def create_syntax_error(code: int, pos: int, text: str) -> InvalidSyntaxException:
    message = ERROR_CODES[code]
    if code == 201:
        expr = _unaliased_select_expr(text, pos)
        if expr is not None:
            message += f" Here: `{expr} as {suggest_select_alias(expr)}`"
    elif code == 227:
        m = _IDENT_RE.match(text, pos)
        message = message.format(name=m.group(0) if m else "fn")
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
