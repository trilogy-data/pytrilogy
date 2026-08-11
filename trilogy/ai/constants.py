from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_FAMILIES, FUNCTION_REGISTRY, function_family

RULE_PROMPT = r"""# Trilogy Syntax

Trilogy statements define a semantic model or a query; collections of statements
are written as `.preql` files. A statement returns data (SELECT) or defines
concepts (key | property | auto | metric), datasources, or parameters.

import <model> as <alias>; makes a model's fields available. <model> is a DOT-SEPARATED MODULE PATH relative to the project root (where trilogy.toml lives), never a file path: the file `raw/store_sales.preql` is imported as `import raw.store_sales as ss;` — replace each `/` with `.`, drop the `.preql`, keep the folder segment. When an imported model imports others (a fact with imported dimensions), those are dot paths: `enroll.student.name`. Typical usage imports ONE fact and reaches dimensions through it; nested dimensions carry ALL values (order.customers includes customers without orders).

key | property | auto | metric; define new concepts. `auto x <- ...` is a DEFINITION, not a precomputed value: each reference expands and re-evaluates in the consuming query's scope.

parameter NAME TYPE [default <literal>]; — a runtime value supplied via `--param NAME=VALUE` (repeatable); append `?` to the type to allow null.

## Combining models

| Goal | Use |
|---|---|
| Typical query | no merge, no join: access all fields through dot-paths |
| Blend two models on shared keys inside one query | scoped `subset\|union join` |
| Make a connection universal to a whole file | `merge` |
| Stack subsets/channels as rows | `union(...)` |
| Rows in A but never in B (set difference) | `except(...)` |
| Rows present in every source (set intersection) | `intersect(...)` |

### Query-scoped join

A typical fact already has its dimensions merged in — no join needed. When blending fact models or rowset outputs, a join DECLARES how the key domains relate (allowing a path, not defining a join):

- `subset join a = b` — a's values are contained in b's (a ⊆ b); b is authoritative for the key.
- `union join a = b` — neither domain contains the other; the key is the coalesce of both sides and unmatched rows from BOTH sides are kept.

A union key-group must be entirely union (`union join a = b = c` chains one group); subset joins mix freely. Keys may be expressions (`union join a.id + 53 = b.id`), aggregates, or windows; only `=` is supported. Joins never drop nulls (NULL keys match null-safely) and are ALWAYS effectively full — no join EVER implicitly drops a row; there is NO inner join. For an intersection, add an explicit `where <optional-side attr> is not null`.

JOIN ON THE FULL GRAIN. `explore` prints each fact's grain as @<k1, k2>; a composite grain needs one join clause PER key (BOTH `union join a.order_number = b.order_number` AND `union join a.item.id = b.item.id`). Matching only one key of a multi-key grain causes duplication.

Joins go right after the select list. Full example: trilogy agent-info syntax example scoped-join.

merge <a> into ~<b>; is the persistent (whole-file) equivalent of `subset join a = b`; plain `merge a into b;` declares EXACT domain equivalence. Standalone statements; prefer a scoped join unless the connection is universal.

### union / except / intersect (row set operations)

union((armA), (armB), ...) -> (out1, out2, ...) row-stacks self-contained select arms positionally (SQL UNION ALL) into one named result, usable in a rowset (`with combined as union(...) -> (...)`). Each arm is an ordinary select and DEDUPLICATES to its own output grain before stacking — so aggregate INSIDE each arm (preferred), or carry each arm's grain key through as an extra output column; stacking raw measure rows and aggregating outside silently undersums. Example: trilogy agent-info syntax example union-stack-channels.

except((armA), (armB), ...) -> (...) and intersect(...) share union's arm shape but are SQL SET operators: output rows are DISTINCT, whole rows compare null-safely, and except subtracts later arms from the FIRST (arm order matters). Prefer except over multi-column `not in` for "in A but never in B". Example: trilogy agent-info syntax example except-intersect-setops.

## Query structure

Top-level statements first (each `;`-terminated), then a query with clauses in this EXACT order — all optional except select:

```
<top-level statements>;            # import / key / auto / rowset / merge / def / parameter
with <name> as                     # optional: name the select as a reusable rowset (CTE)
where   <row condition>            # 1. filters INPUT rows, BEFORE aggregation
select  <col>, <agg> as name,      # 2. projection — grouping is AUTOMATIC by the
                                   #    non-aggregated columns; never write GROUP BY
  subset|union join a = b (= c)?   # 3. blend models; one clause per key, right after the select list
by rollup|cube|grouping sets (...) # 4. optional multi-level grouping for the whole select
having  <result condition>         # 5. filters aggregated/joined RESULTS
order by <col> asc|desc            # 6. sort
limit   <n>;                       # 7. cap rows
```

A rowset (`with <name> as where ... select ...;`) is a standalone statement, evaluated in isolation — it does NOT respond to the consuming query's context. All its outputs are namespaced: output `abc.def` of rowset `foo` is referenced as `foo.abc.def`, and joined back like any concept (`subset join foo.key = other.key`). Alias every reused expression with `as`.

Full annotated example: trilogy agent-info syntax example query-structure.

### Not SQL — what to never write

- **No FROM, GROUP BY, DISTINCT, SELECT \*, or SQL-style set operators between selects.** Stack rows with `union(...)`; set difference/intersection with `except(...)`/`intersect(...)`; blend fact models with a scoped join.
- **Grouping is automatic** by the non-aggregated fields in the SELECT — never write GROUP BY. Aggregates inherit the select's grain wherever they appear (where/select/having). Override per-aggregate with `agg(x) by <dims>`; `by *` aggregates across all data (single row). To weight a related dimension property once per fact row, project it to the fact grain first: `auto row_birth_year <- group(enroll.student.birth_year) by enroll.student_id, enroll.course_id;` then aggregate that.
- **Output rows are deduplicated to the select grain.** To preserve legitimate duplicates (one row per matching fact), include the fact's grain keys in the select — hidden with the PREFIX `--` if they shouldn't appear in the output.
- **Never write `distinct`.** `count(<key>)` is already distinct because keys are unique; `count_distinct(<property>)` counts distinct values of a non-key property.
- **Count a COMPOSITE grain with `grain(...)`.** `count(grain(order_id, item.id))` counts order+item combinations; `grain(...)` is NEVER NULL. Never count one column of a multi-key grain — a coarser key counts its own distinct values and undercounts. `count_distinct` takes ONE argument; for multi-column distinct counts use `count(grain(a, b))`.
- **An aggregate over a group with no qualifying rows is NULL, never 0.** `sum(x ? cond) by k` yields NULL for groups where nothing matches, so `= 0` matches nothing. Test emptiness with `count(key ? cond) by k = 0` or wrap: `coalesce(sum(x ? cond), 0)`.
- **`count(1)` is invalid** — a constant does not identify rows. Use the row key, `count(grain(...))`, or `sum(1) by <grain>`.
- **One-column expression subqueries only.** `(select ...)` is allowed where a scalar or membership set is expected (exactly one projected column) — never as a SQL-style FROM-subquery. For related-entity filters prefer the dot-path: `where enrollments.student.state = 'TN'`, not a subselect.
- **`--` is a HIDDEN-field prefix, not a comment** (it still changes query structure). Comments use `#` only.
- **Aliases**: always use the full dot-path (`enroll.student.id`); alias every new expression with `as`; a select alias is usable in `having`/`order by` but NOT inside other select expressions or `where`, and must not rename a field back to an existing concept name.

## Filtering

`where` filters input rows before aggregation; `having` filters results after aggregation/windows. The inline filter `x ? cond` filters the immediate prior expression: `sum(credits ? enrolled = true)`.

Aggregates/windows in WHERE do not filter each other's inputs. Use inline filters, or a staged `then where` chain: each `then where` stage's aggregates compute over only the rows passing earlier stages (`where x = 5 then where sum(y) > 10` ≡ `where x = 5 and sum(y ? x = 5) > 10`). One rule: the SAME cross-row expression cannot gate two different stages — give one an inline filter.

Windows need all rows in their range: filter AFTER the window (in `having`) rather than in `where` when the window must see the full set.

## Membership (semijoins)

`in` / `not in` cross models without any declared relationship — the semijoin IS a scoped intersection. The right side is a concept or expression (never a subselect); build computed sets with an inline filter: `auto big_zip <- student.zip ? (count(student.id ? student.honors = true) by student.zip) > 10; ... where school.zip in big_zip`.

Membership is IDENTITY matching, not SQL three-valued logic: NULL matches NULL; the result is always TRUE or FALSE (safe as a projected flag); `not in` is the exact complement (a NULL in the set never empties the result). For strict SQL behavior, add `x is not null`.

A fact model contains ALL dimensional members, so `where students.id not in enroll.student_id` is typically a tautology — use `where enroll.id is null` instead. Tuple membership `(a, b) in (m.a, m.b)` requires both right-side concepts to resolve to ONE source (model or rowset); to test against a cross-model pair, stage it through a rowset first. Examples: existence-anti-join, staged-membership.

## Aggregation

An aggregate without an explicit `by` is RESPONSIVE — its grain is set where it is consumed, not where it is declared (`auto avg_credits <- avg(enroll.credits);` becomes per-department when selected with `enroll.department`). Pin a grain inline: `sum(metric) by dim1, dim2 as name`; `by` accepts bare identifiers or parenthesized expressions: `avg(price) by (substring(phone, 1, 2))`.

Multi-level grouping (`by rollup|cube|grouping sets (...)`) is a clause of the WHOLE select, placed before `having`: it computes every no-explicit-grain aggregate at every level in one pass (`by rollup (d1, d2)` → leaf, per-d1 subtotal, grand total). `grouping(<field>)` = 1 on rolled-up rows — use it to label levels rather than testing for NULL. Full detail: trilogy agent-info syntax example rollup.

## Window functions

Default to windows for self-referential queries (period-over-period, running totals, rank, share of group). SQL-style syntax: `rank(<key>) over (partition by <g> order by <expr> desc)` (`dense_rank`/`row_number` same shape; multi-key `rank(a, b)`); `sum(x) over (partition by g order by t)` for running totals; `lag(<field>, <offset>) over (...)` / `lead(...)` for offsets (default 1). `partition by` is optional and accepts expressions.

## Data types

Scalars: `string`, `bytes`, `bool`, `int`, `bigint`, `float`, `double`, `number`, `numeric(p,s)` (alias `decimal`; use for exact money), `date`, `datetime`, `timestamp`, `geography`, `any`. Composites: `array<T>`, `map<K, V>`, `struct<name: T, ...>`, `enum<T>[v1, v2, ...]`. Cast with `::type` or `cast(x as type)`: `"2020-01-01"::date`.

`?` after a type marks it nullable (`property user.middle_name string?;`). `~` prefixing a concept marks the reference as a PARTIAL subset of the concept's domain — on a select item (`~customer.id`) or in `merge a into ~b` (a ⊆ b); it tells the planner this side is not authoritative.

## Expression gotchas

- `x ? cond` binds to a primary — wrap arithmetic: `(a - b) ? cond`, NOT `a - b ? cond`.
- `a || b` is NULL if either side is NULL; `concat(...)` skips NULL arguments; `concat_ws(sep, ...)` joins with sep, skipping NULLs.
- Date parts are unquoted: `date_part(d, year)`, never `'year'`; prefer `year(d)`. `date_diff(start, end, unit)` = end − start (a shipping lag is `date_diff(sold_date, ship_date, day)`).
- Zero-argument functions still take parentheses: `current_date()`.
- Factor repeated calculations into a `def` macro (invoked `@name(...)`) or reusable `auto` concepts.
"""


def render_function(function_type: FunctionType, example: str | None = None):
    info = FUNCTION_REGISTRY[function_type]

    if info.arg_count == -1:
        # Infinite/variable number of arguments
        base = f"{function_type.value}(<arg1>, <arg2>, ..., <argN>)"
    elif info.arg_count == 0:
        # No arguments
        base = f"{function_type.value}()"
    else:
        # Fixed number of arguments
        base = f"{function_type.value}({', '.join([f'<arg{p}>' for p in range(1, info.arg_count + 1)])})"

    if example:
        base += f" e.g. {example}"
    return base


FUNCTION_EXAMPLES = {
    FunctionType.DATE_ADD: "date_add('2020-01-01'::date, month, 1)",
    FunctionType.DATE_DIFF: "date_diff('2020-01-01'::date, '2020-01-02'::date, day)",
    FunctionType.DATE_PART: "date_part('2020-01-01'::date, year)",
    FunctionType.DATE_SUB: "date_sub('2020-01-01'::date, day, 1)",
    FunctionType.DATE_TRUNCATE: "date_trunc('2020-01-01'::date, month)",
    FunctionType.CURRENT_TIMESTAMP: "now()",
}

# AST-internal / operator-duplicate function types that the agent should never
# call by name — they are noise in the reference. `union` is a relational TVF with
# its own `union((arm), ...) -> (...)` syntax (documented in RULE_PROMPT + the
# `union-stack-channels` example), NOT a plain function call, so it stays out of
# this list. Arithmetic is written with operators (`a + b`, not `add(a, b)`);
# member/index access with `.`/`[]`; parens/aliases/constants are surface syntax.
_AGENT_HIDDEN_FUNCTIONS = {
    FunctionType.NOOP,
    FunctionType.CUSTOM,
    FunctionType.UNION,
    FunctionType.RECURSE_EDGE,
    FunctionType.ALIAS,
    FunctionType.PARENTHETICAL,
    FunctionType.CONSTANT,
    FunctionType.TYPED_CONSTANT,
    FunctionType.BOOL,
    FunctionType.INDEX_ACCESS,
    FunctionType.MAP_ACCESS,
    FunctionType.ATTR_ACCESS,
    FunctionType.GROUP,
    FunctionType.ADD,
    FunctionType.SUBTRACT,
    FunctionType.MULTIPLY,
    FunctionType.DIVIDE,
}


_FAMILY_ORDER = [label for label, _ in FUNCTION_FAMILIES] + ["other"]


def _render_function_list(types) -> str:
    """Render the function reference as one labelled line per semantic family.
    Within a family, functions sharing an argument signature are consolidated
    (``sum|avg|min|max(<arg1>)``); families with mixed arities join their
    signature groups with `` ; ``. Functions carrying a worked example (the date
    functions) get their own line under their family so the example isn't lost.
    Family-level grouping keeps it compact without scrambling unrelated
    functions onto one line."""
    fam_sigs: dict[str, dict[str, list[str]]] = {}
    fam_examples: dict[str, list[str]] = {}
    seen: list[str] = []
    for v in types:
        fam = function_family(v)
        if fam not in fam_sigs:
            fam_sigs[fam], fam_examples[fam] = {}, []
            seen.append(fam)
        example = FUNCTION_EXAMPLES.get(v)
        if example:
            fam_examples[fam].append(f"  {render_function(v, example=example)}")
            continue
        sig = render_function(v)[len(v.value) :]  # the "(<arg1>, ...)" / "()" tail
        fam_sigs[fam].setdefault(sig, []).append(v.value)

    lines: list[str] = []
    for fam in sorted(seen, key=_FAMILY_ORDER.index):
        segs = [f"{'|'.join(names)}{sig}" for sig, names in fam_sigs[fam].items()]
        if segs:
            lines.append(f"{fam}: {' ; '.join(segs)}")
        lines.extend(fam_examples[fam])
    return "\n".join(lines)


FUNCTIONS = _render_function_list(
    v
    for _, v in FunctionType.__members__.items()
    if v in FUNCTION_REGISTRY and v not in _AGENT_HIDDEN_FUNCTIONS
)

AGGREGATE_FUNCTIONS = _render_function_list(
    v
    for _, v in FunctionType.__members__.items()
    if v in FunctionClass.AGGREGATE_FUNCTIONS.value
    and v in FUNCTION_REGISTRY
    and v not in _AGENT_HIDDEN_FUNCTIONS
)
