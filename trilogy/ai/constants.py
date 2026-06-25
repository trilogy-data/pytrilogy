from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = r"""# Trilogy Syntax Guide

Trilogy statements define either a semantic model or a query. If a user asks for data, they want a SELECT.

Semantic model statements

import <model> as <alias>; — makes a model's fields available. Imports chain: when an imported model imports others (e.g. a fact table foreign-key-linked to its dimensions), reach those by path — after import enrollments as enroll;, write enroll.student.name. Import only the model(s) you take measures from; never separately import a model already reachable by chaining (a separate import is a disconnected copy that will not join). A field belongs to exactly one model — never invent intermediate nesting: enroll.credits, never enroll.student.credits.

key | property | auto | metric — define fields locally; outputs appear in the available fields, so editing these is rarely needed unless requested. Predefined concepts (auto x <- ...) are definitions, NOT precomputed values: each reference expands inline (like a macro) and re-evaluates in the referencing query's scope — so the query's WHERE filters the rows feeding a referenced aggregate.

parameter NAME TYPE [default <literal>]; — declares a runtime value supplied via trilogy run <file>.preql --param NAME=VALUE (repeat --param for several). Reference it like any field. Without default, --param is required at run time.

datasource — maps fields to a SQL table; left side is the SQL column, right side the field name.

Models include facts + dimensions. Nullability and fanout are handled automatically, defaulting to preserving data: order.customers returns all customers, even those without orders. Add not null conditions to filter.
## Combining models


| Goal | Use |
|---|---|
| Typical query | no select, no merge, access joins through dot-paths |
| Blend two models on shared keys inside one query | scoped `inner\|left\|full join` (the default) |
| Make a connection universal to all queries in a file | `merge` |
| Stack subsets/channels as rows | `union(...)` |

### Query-scoped join (the default)

inner|left|full join <a> = <b> [= <c>] blends two models inside one SELECT. Place it right after the select list (the SQL-like spot); Semantics match SQL: inner asserts strict equivalence (drops unmatched rows); left makes the right side optional/nullable; full keeps unmatched rows from both sides. right is unsupported — swap operands. A full key-group must be entirely full (no mixing with inner/left on the same key; full join a = b = c chains one all-full group); inner and left mix freely. Chain = c to pull additional concepts into a join. Each key may be any expression, not just a field — join on a computed/offset key (`inner join a.week_seq + 53 = b.week_seq`), an aggregate, or a window; only `=` equality is supported.

Join on the full grain. When blending two FACT models, write one join clause per key in their shared grain. trilogy explore prints each fact's grain as @<k1, k2> (e.g. @<order_number, item.id>); a composite grain needs BOTH inner join a.order_number = b.order_number AND inner join a.item.id = b.item.id. Matching only one key of a multi-key grain fans out and double-counts — a top cause of wrong results.

Full example: trilogy agent-info syntax example scoped-join.

merge (model-level)

merge <a> into <b> is the persistent equivalent of a join (whole query/file). Prefer a scoped join; use merge only when the connection is universal. merge a into ~b marks b the superset, so a is a partial subset (brought-in side nullable, like left join); plain merge a into b asserts strict equivalence (like inner join). One merge per shared concept.

union (row stacking)

union((armA), (armB), ...) -> (out1, out2, ...) row-stacks self-contained select arms positionally (SQL UNION ALL) into one named result. Arms match by column position (same count/order/types as outputs) and may contain full SQL including subfilters and joins. Usable in a rowset — with combined as union(...) -> (...) with outputs using standard rowset namespaceing <rowset_name>.<path>.

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

A named query - a rowset is defined by a select with a preceding `WITH <name>`; reference it later as `<name>.<field>` or in a join as `<name>.<key> = other.<key>`. These are standalone statements, not part of a select.  
A select without WITH is an anonymous query whose outputs are not reusable by name. 

A rowset creates a "new" model; use joins to merge the rowset outputs back into the global namespace if needed.


Full annotated example: `trilogy agent-info syntax example query-structure`.

### Not SQL — what to never write

- **No FROM, GROUP BY, DISTINCT, SELECT \*, or SQL-style set operators.** To stack rows use `union(...)`; to blend fact models use a scoped join.
- **Grouping is automatic** by the non-aggregated fields in the SELECT — never write GROUP BY.
- **Never write `distinct`.** `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count distinct values of a non-key property.
- **No subselects.** "Filter the fact by an attribute of a related entity" → reach across the import chain with a dot-path in WHERE:
  - Wrong: `where enrollments.student_id in (select student_id where student.state = 'TN')`
  - Right: `where enrollments.student.state = 'TN'`
- Since there are no underlying tables, `sum(1)`/`count(1)` is only meaningful grouped by a grain field (e.g. `sum(1) by x as count`).

### Fields and aliases

- All fields exist in a global namespace; always use the full path (`enroll.student.id`).
- Every new expression in the select output must be aliased with `as` (e.g. `sum(births) as all_births`).
- Aliases cannot appear inside calculations or in WHERE/HAVING/ORDER clauses: `sum(credits) as total_credits` is valid; `(sum(credits) as total_credits) + 1 as credits_plus_one` is not. Never alias a field to an existing name.
- Always use a reasonable `LIMIT` on final queries if unspecified, unless the request is a time series or line chart.

## Filtering

WHERE filters rows BEFORE aggregates and window functions; HAVING after. The inline filter x ? cond filters one expression's input (e.g. sum(x ? x > 0)).

WHERE pushdown scoping. WHERE conditions push into aggregates/windows in the select, NOT into aggregates/windows written in WHERE itself. where x = 3 and sum(x.y) > 10 sums over ALL x. Either inline-filter (where x = 3 and sum(x.y ? x = 3) > 10) or filter in HAVING:
```
where x = 3
select --sum(x.y) as total_y
having total_y > 10
```
HAVING references the projection only. Select what you filter on; hide it with a leading -- to keep it out of the output. Hide-and-HAVING a dimension (rather than moving it to WHERE) whenever WHERE would change an aggregate's or window's input — e.g. filtering to one year AFTER a lead/lag over the full series:
```
select student.state, --sum(enroll.credits) as total_credits, --enroll.year
having total_credits > 1000 and enroll.year = 2020
```

HAVING aggregates inherit the output grain — a bare sum(x)/avg(x) there is the CURRENT group's value, not a global total. Pin a different grain explicitly: by * is global (one value over all rows); by <dims> fixes a coarser grain. E.g. "a student's credits exceed 0.0001 of the global total":
```
auto grand_total <- sum(enroll.credits) by *;
select student.id, --sum(enroll.credits) as student_total
having student_total > 0.0001 * grand_total
```

Aggregates in WHERE. To filter rows by an aggregate over pre-filter inputs, write the aggregate directly in WHERE with inline grouping agg(x) by grain (add an inline ? condition if needed):
```
where enroll.year = 2020
  and course.credits > 1.2 * avg(course.credits ? explicit_other_condition) by course.department
select course.name, course.credits
```

Membership in a computed set (SQL IN (subquery)): define the set as a derived concept (filter with ?), then test in against that concept. The right side is a concept, not a literal list — no (select ...). Both sides may be expressions; membership compares the left expression against every value of the right concept (a semi-join over a value set):
```
auto big_zip <- student.zip ? (count(student.id ? student.honors = true) by student.zip) > 10;
# schools whose 2-digit zip-prefix matches a high-honors-student zip:
where substring(school.zip, 1, 2) in substring(big_zip, 1, 2)
```

**Anti-join / semi-join.** A fact model contains the full set of dimensional members (all students appear in `fact.students`), so:
- No matching record (anti-join): `where students.id not in enroll.student_id` is typically a tautology — `enroll.student_id` references the student table and contains all students. Use e.g. `where enroll.id is null select enroll.student.id` instead.
- Has a matching record (semi-join): `where students.id in ([some other student_list])` effectively filters across models that are not explicitly merged but share an ID (e.g. IDs from an external source).

## Aggregation and grouping

- Aggregates group at the query's automatic grain by default; override with inline grouping: `sum(metric) by dim1, dim2 as sum_by_dim1_dim2`.
- The `by` clause accepts bare identifiers (`by dim1, dim2`) OR arbitrary expressions wrapped in parens — function calls, casts, arithmetic: `avg(price) by (substring(phone, 1, 2))`.
- **Multi-level grouping** (ROLLUP / CUBE / GROUPING SETS) attaches to an aggregate's `by` clause and computes it at multiple grain levels in one pass:
  - `agg(<expr>) by rollup d1, d2` → grouping sets `(d1, d2)`, `(d1)`, `()` — standard SQL ROLLUP, useful for subtotals + grand total.
  - `agg(<expr>) by cube d1, d2` → every subset of the grouping keys.
  - `agg(<expr>) by grouping sets (d1, d2), (d1), ()` → arbitrary explicit combinations; parens around each set; `()` is the grand total.
  - The clause attaches to ONE aggregate. When several aggregates need the same expansion, wrap them in a `def` macro (or repeat the spec for each):

    ```
    def rollup_avg(metric) -> avg(metric::numeric(12,2)) by rollup enroll.department, enroll.year;
    select enroll.department, enroll.year,
        @rollup_avg(enroll.credits) as agg1,
        @rollup_avg(enroll.grade_points) as agg2;
    ```
  - `grouping(<field>)` returns 1 when the field has been rolled up at that row, 0 otherwise — use it (or a sum like `grouping(a) + grouping(b)`) to compute the hierarchy level. Detecting rollup rows by output NULL only works when the source has no real NULLs in those columns; when in doubt, prefer `grouping()`.

## Window functions

Default to windows for **self-referential queries** — relating a row to other rows of the same set (period-over-period, previous/next value, running total, share of a group total, rank). Syntax is SQL-style:

- Ranking: `rank(<key>) over (partition by <group> order by <expr> desc)` — e.g. `rank(name) over (partition by state order by sum(births) desc) as top_name`. `partition by` is optional (omit for one global window). `dense_rank`/`row_number` take the same shape.
- Multi-key ranking: `rank(a, b) over (...)` — all comma-separated args are equal-status grain keys (used when ranking rollup output whose grain spans multiple columns).
- `partition by` accepts arbitrary expressions, not just identifiers: `partition by upper(student.state), case when student.gpa >= 3.5 then 1 else 0 end`.
- Aggregates as windows: `sum(x) over (partition by g order by t)` for running totals. Without `order by`, a partitioned aggregate collapses to a plain grouped aggregate — write `sum(x) by g` directly instead.
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
- Comments use `#` only, per line.
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

**Aggregate-in-WHERE plus HAVING.** For students with significant enrollments between 2000 and 2002, find those with average daily enrollments > 10 between 2002 and January 31, 2010:

```
where enroll.date between '2002-01-01'::datetime and '2010-01-31'::datetime
  and count(enroll.id ? year(enroll.date::datetime) between 2000 and 2002) by student.name > 1000
select
    student.name,
    count(enroll.id) as total_enrollments,
    total_enrollments / date_diff(min(enroll.date.date), max(enroll.date.date), DAY) as average_daily_enrollments
having
    average_daily_enrollments > 10
order by
    total_enrollments desc;
```

**Shared rollup spec via `def`.** When several columns share the same calculation, factor it out:

```
def by_geo(metric) -> avg(metric::numeric(12,2))
    by rollup student.country, student.state, student.county;

select
    student.country,
    student.state,
    student.county,
    @by_geo(enroll.credits)      as avg_credits,
    @by_geo(enroll.grade_points) as avg_grade_points
limit 100;
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

FUNCTIONS = "\n".join(
    [
        render_function(v, example=FUNCTION_EXAMPLES.get(v))
        for x, v in FunctionType.__members__.items()
        if v in FUNCTION_REGISTRY and v not in _AGENT_HIDDEN_FUNCTIONS
    ]
)

AGGREGATE_FUNCTIONS = "\n".join(
    [
        render_function(v, example=FUNCTION_EXAMPLES.get(v))
        for _, v in FunctionType.__members__.items()
        if v in FunctionClass.AGGREGATE_FUNCTIONS.value
        and v in FUNCTION_REGISTRY
        and v not in _AGENT_HIDDEN_FUNCTIONS
    ]
)
