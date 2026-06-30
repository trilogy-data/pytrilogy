from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = r"""# Trilogy Syntax Guide

Trilogy statements define a semantic model or a query. Only selects return data.

import <model> as <alias>; — makes a model's fields available. When an imported model imports others (a fact table with imported dimensions); those are exposed as dot paths. `import enrollments as enroll;` enables accessing `enroll.student.name` if enroll imports students as studen. 
Typical usage only imports facts; dimensions will then be accessed through the nested import. Nested dimensions have all values; order.customers returns all customers, even those without orders. 

key | property | auto | metric — define new concepts in your script. These concepts (auto x <- ...) are definitions, NOT precomputed values: each reference expands in a query and re-evaluates in the referencing query's scope. 

parameter NAME TYPE [default <literal>]; — declares a runtime value supplied via trilogy run <file>.preql --param NAME=VALUE (repeat --param for several). Reference it like any field. Without default, required at run time.

## Combining models

| Goal | Use |
|---|---|
| Typical query | no select, no merge, all fields accessed through dot-paths |
| Blend two models on shared keys inside one query | scoped `inner\|left\|full join` (the default) |
| Make a connection universal to all queries in a file | `merge` |
| Stack subsets/channels as rows | `union(...)` |

### Query-scoped join (the default)

inner|left|full join <a> = <b> [= <c>] blends two models inside one SELECT. Place it right after the select list (the SQL-like spot); Semantics match SQL: inner asserts strict equivalence (drops unmatched rows); left makes the right side optional/nullable; full keeps unmatched rows from both sides. Right unsupported; just flip to a left. A full key-group must be entirely full (no mixing with inner/left on the same key; full join a = b = c chains one all-full group); inner and left mix freely. Chain = c to pull additional concepts into a join. Each key may be any expression, not just a field — join on a computed/offset key (`inner join a.id + 53 = b.id`), an aggregate, or a window; only `=` equality is supported.

Joins indicate that concepts are *the same*; it is a conceptual operation not a field operation. inner join a=b means that a is null and b is not null is tautologically always false.

Joins do NOT drop nulls. Joins will merge null values across dimension keys. To filter out nulls, explicitly use not-null conditions.

Join on the full grain. When blending two FACT models, write one join clause per key in their shared grain. trilogy explore prints each fact's grain as @<k1, k2> (e.g. @<order_number, item.id>); a composite grain needs BOTH inner join a.order_number = b.order_number AND inner join a.item.id = b.item.id. Matching only one key of a multi-key grain fans out and double-counts — a top cause of wrong results.

Full example: trilogy agent-info syntax example scoped-join.

merge (model-level)

merge <a> into <b> is the persistent equivalent of a join (whole query/file). Prefer a scoped join; use merge only when the connection is universal. merge a into ~b marks b the superset, so a is a partial subset (brought-in side nullable, like left join); plain merge a into b asserts strict equivalence (like inner join). One merge per shared concept.

union (row stacking)

union((armA), (armB), ...) -> (out1, out2, ...) row-stacks self-contained select arms positionally (SQL UNION ALL) into one named result. Arms match by column position (same count/order/types as outputs) and may contain full trilogy select statements (with their own filters + local joins). Usable in a rowset — with combined as union(...) -> (...) with outputs using standard rowset namespaceing <rowset_name>.<path>.

Full example: trilogy agent-info syntax example union-stack-channels.

## SELECT statements

```
<WITH NAME>?      # name the select to use later
WHERE?            # filter rows BEFORE aggregation
SELECT
  <EXPR> [AS <ALIAS>], ...
  INNER|LEFT|FULL JOIN <a> = <b> [= <c>] ...   # one or more join concepts beyond model defaults
HAVING?           # filter AFTER aggregation
ORDER BY?
LIMIT?
```

A CTE/Rowset - a named output - is defined by a select with a preceding `WITH <name> as`; reference it later as `<name>.<field>` or in a join as `<name>.<key> = other.<key>`. These are standalone statements, not part of a select.  

A rowset creates a "new" model with all concepts namespaced; `abc.def` output in a rowset called `foo` is
referenced as `foo.abc.def`. Use joins to merge the rowset outputs back into the global namespace if needed.

with funky as
where customer = 'funky_monkey'
SELECT
    order.id,
;

Is accessed as `select funk.order.id`;

Full annotated example: `trilogy agent-info syntax example query-structure`.

### Not SQL — what to never write

- **No FROM, GROUP BY, DISTINCT, SELECT \*, or SQL-style set operators.** To stack rows use `union(...)`; to blend fact models use a scoped join.
- **Grouping is automatic** by the non-aggregated fields in the SELECT — never write GROUP BY. Aggregates inherit the grain of the select output list automatically, in where/select/having. Use explicit grain agg(x) by <dims> as needed to override the default. Use `by *` to aggregate to a global scalar.
- **Never write `distinct`.** `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count distinct values of a non-key property.
- **No subselects.** "Filter the fact by an attribute of a related entity" means reach across the import chain with a dot-path in WHERE:
  - Wrong: `where enrollments.student_id in (select student_id where student.state = 'TN')`
  - Right: `where enrollments.student.state = 'TN'`
- **-- is a HIDDEN field not a comment; it still changes query structure. Use # for comments
- Since there are no underlying tables, `sum(1)`/`count(1)` is only meaningful grouped by a grain field (e.g. `sum(1) by x as count`).

### Fields and aliases

- Always use the full path (`enroll.student.id`) for a field; namespacing matters.
- Every new expression in the select output must be aliased with `as` (e.g. `sum(births) as all_births`).
- Aliases cannot appear inside calculations or in WHERE/HAVING/ORDER clauses: `sum(credits) as total_credits` is valid; `(sum(credits) as total_credits) + 1 as credits_plus_one` is not. Never alias a field to an existing name.
- Use a context dependent reasonable `LIMIT` on final queries if unspecified (data for charts typically must be complete)
## Filtering

WHERE filters rows BEFORE aggregates and window functions; HAVING after. The inline filter x ? cond filters one expression's input (e.g. sum(x ? x > 0)).

WHERE conditions push into aggregates/windows in the select, NOT into aggregates/windows written in WHERE itself. where x = 3 and sum(x.y) > 10 sums over ALL x. Either inline-filter (where x = 3 and sum(x.y ? x = 3) > 10) or filter in HAVING:
```
where thing.key = 3
select 
    thing.prop,
    --sum(thing.val) as total_val
having 
    total_val > 10
```
HAVING filters after any where conditions are output, making it useful for window functions and other cases. You can hide concepts in the output projection to make them reusable in HAVING.
```
select 
    student.state, 
    --sum(enroll.credits) as total_credits, 
having 
    total_credits > 1000 
    and enroll.year = 2020
```

HAVING/WHERE aggregates inherit the output grain; a bare sum(x)/avg(x) there is the CURRENT group's value, not a global total.
 Pin a different grain explicitly: by * is global (one value over all rows); by <dims> fixes a coarser grain. E.g. "a student's credits exceed 0.0001 of the global total":
```
auto grand_total <- sum(enroll.credits) by *;

select 
    student.id, 
    --sum(enroll.credits) as student_total
having 
    student_total > 0.0001 * grand_total
```

Aggregates in WHERE are not filtered by other items in the where clause, and inherit the select grain.
To filter a aggregate in the where pre-aggregation, use inline condition (? ) inside the aggregate. 
Use an explicit grain (such as `*` for a global total) to avoid the default select grain. 
```
where enroll.year = 2020
  and course.credits > 1.2 * avg(course.credits ? explicit_other_condition) by course.department
  and course.creds > 1.5 * avg(course.credits) by *
select course.name, course.credits
```

## SemiJoins

Semijoins are unique in that they do not require an explicit relationship to cross models, as the semijoin *is* a scoped intersection.

Membership in a computed set (SQL IN (subquery)): define the set as a derived concept (filter with ?), then test in against that concept. 
The right side is a concept or expression, not subselect. (in fact either side can be an expression) membership compares the left expression against every value of the right concept (a semi-join over a value set):
```
auto big_zip <- student.zip ? (count(student.id ? student.honors = true) by student.zip) > 10;
# schools whose 2-digit zip-prefix matches a high-honors-student zip:
where substring(school.zip, 1, 2) in substring(big_zip, 1, 2)
```

 A fact model contains the full set of dimensional members (all students appear in `fact.students`), so:
- No matching record (anti-join): `where students.id not in enroll.student_id` is typically a tautology — `enroll.student_id` references the student table and contains all students. Use e.g. `where enroll.id is null select enroll.student.id` instead.
- Has a matching record (semi-join): `where students.id in ([some other student_list])` effectively filters across models that are not explicitly merged but share an ID (e.g. IDs from an external source).

## Aggregation and grouping

- Aggregates group at the query's automatic grain by default; override one aggregate's grain with inline grouping: `sum(metric) by dim1, dim2 as sum_by_dim1_dim2`.
- The `by` clause accepts bare identifiers (`by dim1, dim2`) OR arbitrary expressions wrapped in parens — function calls, casts, arithmetic: `avg(price) by (substring(phone, 1, 2))`.
- **Multi-level grouping** (ROLLUP / CUBE / GROUPING SETS) is a property of the WHOLE select — a clause after the select list (before `order by`/`limit`) that computes the query at multiple grain levels in one pass. It applies to EVERY aggregate in the select that has no explicit `by` grain, so there is exactly one consistent grouping:
  - `select d1, d2, agg(<expr>) as a by rollup (d1, d2)` → grouping sets `(d1, d2)`, `(d1)`, `()` — standard SQL ROLLUP, useful for subtotals + grand total.
  - `select d1, d2, agg(<expr>) as a by cube (d1, d2)` → every subset of the grouping keys.
  - `select d1, d2, agg(<expr>) as a by grouping sets ((d1, d2), (d1), ())` → arbitrary explicit combinations; parens around each set; `()` is the grand total.
  - `by rollup ()` (empty) rolls up over the select's own automatic grain.
  - Because it is select-level, multiple measures just share it — no per-aggregate repetition or macro needed:

    ```
    select enroll.department, enroll.year,
        avg(enroll.credits::numeric(12,2)) as agg1,
        avg(enroll.grade_points::numeric(12,2)) as agg2
    by rollup (enroll.department, enroll.year);
    ```
  - A composite measure works the same — both operands roll up together because the clause covers the whole select: `sum(a) - sum(b) as net by rollup (d1, d2)`.
  - `grouping(<field>)` returns 1 when the field has been rolled up at that row, 0 otherwise — use it (or a sum like `grouping(a) + grouping(b)`) to compute the hierarchy level. It needs a `by rollup`/`cube`/`grouping sets` clause on the select. Detecting rollup rows by output NULL only works when the source has no real NULLs in those columns; when in doubt, prefer `grouping()`.

## Window functions

Default to windows for **self-referential queries**; relating a row to other rows of the same set (period-over-period, previous/next value, running total, share of a group total, rank). Syntax is SQL-style:

- Ranking: `rank(<key>) over (partition by <group> order by <expr> desc)` — e.g. `rank(name) over (partition by state order by sum(births) desc) as top_name`. `partition by` is optional (omit for one global window). `dense_rank`/`row_number` take the same shape.
- Multi-key ranking: `rank(a, b) over (...)` — all comma-separated args are equal-status grain keys (used when ranking rollup output whose grain spans multiple columns).
- `partition by` accepts arbitrary expressions, not just identifiers: `partition by upper(student.state), case when student.gpa >= 3.5 then 1 else 0 end`.
- Aggregates as windows: `sum(x) over (partition by g order by t)` for running totals. Without `order by`, a partitioned aggregate collapses to a plain grouped aggregate; write `sum(x) by g` directly instead.
- `lag(<field>, <offset>) over (partition by <g> order by <expr>)` fetches the value `<offset>` rows back; `lead(...)` fetches it ahead. Offset defaults to 1. Examples: `lag(amount, 2) over (order by date asc) as prev_amount`; next-year same week = `lead(weekly, 53) over (order by week_seq asc) as next_year`.

## Expressions and miscellany

- **Operator precedence** (highest binds first; use `(...)` to override):
  1. Primaries: literal, identifier, function call, parenthetical `(...)`, member access (`.`, `[]`, `::` cast).
  2. Inline filter `x ? cond` — `?` takes a primary on the left, so wrap arithmetic in parens: `(a - b) ? cond`, NOT `a - b ? cond` (which binds `?` to `b` alone).
  3. Multiplicative: `*`, `/`, `%`.
  4. Additive / string concat: `+`, `-`, `||`.
  5. Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `not like`, `not ilike`, `between ... and ...`, `in (...)`, `not in (...)`, `is null`.
  6. Logical `and`.
  7. Logical `or`.
- Cast with `::type`, e.g. `"2020-01-01"::date`.
- Date parts have no quotes: `date_part(enroll.date, year)`, never `date_part(enroll.date, 'year')`. Prefer idiomatic function forms when available: `year(enroll.date)`.
- All functions take parentheses; zero-argument functions use empty ones (`current_date()`).
- Comments use `#` only, per line. -- is NOT a comment.
- When several columns share the same calculation, factor it into a `def` macro (invoked with `@name(...)`); for complex logic, break the query into reusable concept declarations.

## Worked examples

**Reusable concepts, filtered aggregates, and dual ranks.** For names with more than 10 births in Vermont ever, find the top 10 names by total births across the US in the 1940s and 1950s for Idaho, along with their Vermont births and ranks within Idaho and nationally:

```
# break up a query by defining reusable components
auto all_births <- sum(births);

# force an explicit grain rather than the select's implicit one:
# births by name, ignoring state
auto births_by_name_usa_wide <- sum(births) by name;

# push filters into aggregates — especially useful for WHERE filtering
auto vermont_births <- sum(births ? state = 'VT');

# use a rowset to define a reusable CTE
with target_names as 
where year between 1940 and 1950
select
    name,
    state,
    all_births,
    vermont_births
;

where 
 target_names.vermont_births > 10
  and target_names.state = 'ID'
select
    target_names.name,
    target_names.state,
    target_names.all_births,
    target_names.vermont_births,
    rank(target_names.name) over (partition by target_names.state order by target_names.all_births desc) as state_rank,
    rank(target_names.name) over (order by births_by_name_usa_wide desc) as all_rank
having
    all_rank < 11
order by
    all_rank asc
limit 5;
```

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


# Coarse semantic families so the reference reads as a handful of grouped lines
# rather than one scrambled mega-line. A function falls into the first family it
# matches; anything unlisted lands in "other".
_FUNCTION_FAMILIES: list[tuple[str, frozenset[str]]] = [
    (
        "aggregate",
        frozenset(
            {
                "count",
                "count_distinct",
                "sum",
                "max",
                "min",
                "avg",
                "stddev",
                "variance",
                "array_agg",
                "bool_or",
                "bool_and",
                "any",
                "grouping",
                "grouping_id",
            }
        ),
    ),
    (
        "string",
        frozenset(
            {
                "lower",
                "upper",
                "ltrim",
                "rtrim",
                "trim",
                "hex",
                "concat",
                "split",
                "strpos",
                "contains",
                "len",
                "substring",
                "replace",
                "regexp_contains",
                "regexp_extract",
                "regexp_replace",
                "format_time",
                "parse_time",
            }
        ),
    ),
    (
        "date/time",
        frozenset(
            {
                "date",
                "datetime",
                "timestamp",
                "second",
                "minute",
                "hour",
                "day",
                "day_of_week",
                "week",
                "month",
                "quarter",
                "year",
                "month_name",
                "day_name",
                "unix_to_timestamp",
                "date_part",
                "date_truncate",
                "date_add",
                "date_sub",
                "date_diff",
                "date_spine",
                "current_date",
                "current_datetime",
                "current_timestamp",
            }
        ),
    ),
    (
        "array/map/struct",
        frozenset(
            {
                "unnest",
                "array",
                "array_distinct",
                "array_sum",
                "array_sort",
                "array_to_string",
                "array_transform",
                "array_filter",
                "generate_array",
                "map_keys",
                "map_values",
                "struct",
            }
        ),
    ),
    (
        "math",
        frozenset(
            {
                "abs",
                "sqrt",
                "random",
                "floor",
                "ceil",
                "round",
                "mod",
                "log",
                "power",
                "hash",
            }
        ),
    ),
    (
        "conditional/cast",
        frozenset(
            {
                "case",
                "simple_case",
                "coalesce",
                "nullif",
                "isnull",
                "greatest",
                "least",
                "cast",
            }
        ),
    ),
    (
        "geo",
        frozenset(
            {
                "geo_from_text",
                "geo_x",
                "geo_y",
                "geo_centroid",
                "geo_point",
                "geo_distance",
                "geo_transform",
            }
        ),
    ),
]
_FAMILY_ORDER = [label for label, _ in _FUNCTION_FAMILIES] + ["other"]


def _function_family(name: str) -> str:
    for label, members in _FUNCTION_FAMILIES:
        if name in members:
            return label
    return "other"


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
        fam = _function_family(v.value)
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
