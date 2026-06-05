from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = """Trilogy statements define a semantic model or query. If a user is asking for data, they want a SELECT.
Semantic model statements:
- import <> imports a model to reuse; its fields become available. Imports CHAIN: when an imported model itself imports others (e.g. a fact table foreign-key-linked to its dimensions), reach those by chaining the path — after `import enrollments as enroll;` write `enroll.student.name`. Import only the model(s) you take measures from; do NOT separately import a model already reachable by chaining (a separate import is a disconnected copy that will not join). A field belongs to exactly one model — never invent intermediate nesting: it is `enroll.credits`, never `enroll.student.credits`.
- merge <a> into <b> links a concept from one model to another so the models that own them join. In general the two sides cover different-sized sets — merge the subset into the superset and mark the superset target with `~`: `merge attendance.student.id into ~enrollments.student.id;`. A plain `merge a into b` (no `~`) asserts strict equivalence — the two concepts are exactly the same set. When a query needs two separate models that share a concept (e.g. two fact tables each linked to `student`), import both and merge their shared concept before the query — one merge per concept. Use `merge` in the query; never edit model files to wire a join.
- key|property|auto|metric defines fields locally. The output will also be visible in fields available to use, so you generally don't need to edit these unless requested.
- `parameter NAME TYPE [default <literal>];` declares a runtime parameter — a value supplied at execution time by `trilogy run <file>.preql --param NAME=VALUE` (multiple `--param` flags allowed). Reference it as a regular field anywhere a value is valid. Without a `default`, the file requires `--param` at run-time; add `default <literal>` to make local validation work without flags.
- datasource statements define a datasource, which is a mapping of fields to a SQL database table. The left side is the SQL column name, the right side is the field name.

SELECT RULES:
- No FROM, JOIN, GROUP BY, SUB SELECTS, DISTINCT, UNION, or SELECT *. The most common SUB SELECTS misuse is `IN (select ...)` to filter on a related dimension — that does not work. Instead, reference the dimension by its dotted path; Trilogy auto-resolves the join:
  * Wrong (SQL-style subselect): `where enrollments.student_id in (select student_id where student.state = 'TN')`
  * Right (dot-path on the related dim): `where enrollments.student.state = 'TN'`
  This pattern generalises: any "filter the fact by some attribute of a related entity" → reach across the import chain (`fact.dim.attr`) and put it in WHERE.
- Existence / non-existence across two models that are NOT directly connected (no shared dotted path): import both, then test a linking key with `in` / `not in` against the OTHER model's key. This is the anti-join / semi-join — it is column-level (`key not in other.key`), NOT a subquery, and needs no merge. Use it for "rows with (no) matching record in another model":
  * No matching record (anti-join): `import students as students; import enrollments as enroll; where students.id not in enroll.student_id` → students who never enrolled in a course.
  * Has a matching record (semi-join): `where students.id in enroll.student_id` → students who have enrolled.
- Membership in a COMPUTED set of values (the "filter X to rows whose value is in a set produced by another query") — is equivalent to a SQL `IN (subquery)`: define the set as a derived concept (filter it with `?`), then test membership with `in` against that concept. The right side is a concept, not a literal list — no `(select ...)`. Both sides may be expressions:
  * Build the set, then filter: `auto big_zip <- student.zip ? (count(student.id ? student.honors = true) by student.zip) > 10;` then `where substring(school.zip, 1, 2) in substring(big_zip, 1, 2)` → schools whose 2-digit zip-prefix matches a high-honors-student zip. The membership compares the LEFT expression against every value of the RIGHT concept (a semi-join over a value set), so mind the grain of each side — `substring(...)` on both sides matches prefixes, not full values.
- Never write the `distinct` keyword. `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count the distinct values of a non-key property.
- All fields exist in a global namespace; field paths look like `enroll.student.id`. Always use the full path. NEVER include a from clause.
- If a field has a grain defined, and that grain is not in the query output, aggregate it to get desired result.
- Newly created fields at the output of the select must be aliased with as (e.g. `sum(births) as all_births`).
- Aliases cannot happen inside calculations or in the where/having/order clause. Never alias fields to an existing name. 'sum(credits) as total_credits' is valid, but '(sum(credits) as total_credits) +1 as credits_plus_one' is not.
- Automatic group by. NEVER include the GROUP BY clause for a select. Grouping is automatic by non-aggregated fields in the SELECT clause.
- You CAN dynamically group inline to get groups at different grains - ex:  `sum(metric) by dim1, dim2 as sum_by_dim1_dm2` for alternate grouping. If you are grouping a defined aggregate
- The `by` clause accepts bare identifiers (`by dim1, dim2`) OR arbitrary expressions wrapped in parens (`by (substring(phone, 1, 2), upper(name))`). Use parens whenever a `by` entry is anything other than a simple identifier — function calls, casts, arithmetic, etc. — e.g. `avg(price) by (substring(phone, 1, 2))`. Without the parens the parser will reject the expression form.
- Count must specify a field (no `count(*)`) Counts are automatically deduplicated for keys. A count of a property counts the key. Use count_distinct for unique property members; do not use it on keys as it is identical to count.
- Since there are no underlying tables, sum/count 1 is only meaningful when grouped by a grain field (e.g. `sum(1) by x as count`).
- Use the where clause to filter *before* query computation (aggregates and windows), the *having* for after. Use hidden fields in the select to be able to filter on them in having without showing them in the output.
- Predefined concepts (`auto x <- ...`) are definitions, NOT precomputed values: each reference expands inline (like a macro) and is re-evaluated in the referencing query's scope. So the query's WHERE filters the rows feeding a referenced aggregate in the select.
- Filtering on aggregates:
  - Use `field ? condition` for inline filters (e.g. `sum(x ? x > 0)`).
  * WHERE conditions are pushed before aggregation calculation for aggregates in the select. Where conditions DO NOT
    apply to other aggregates in the WHERE CLAUSE.
  * HAVING can ONLY reference fields that appear in the SELECT projection — aggregates OR plain dimensions. Select what you filter on; hide it with a leading `--` when you don't want it in the output. Hide-and-HAVING a dimension (rather than moving it to WHERE) whenever WHERE would change an aggregate's or window's input — e.g. filtering one year AFTER a `lead/lag` over the full series:
      select student.state, --sum(enroll.credits) as total_credits, --enroll.year
      having total_credits > 1000 and enroll.year = 2020
  * HAVING evaluates in the post-aggregation OUTPUT context, so an aggregate referenced there inherits the SELECT's output grain — a bare `sum(x)`/`avg(x)` in HAVING is the CURRENT group's value, NOT a global total. To compare against a total at a DIFFERENT grain, PIN the grain explicitly: `by *` keeps it global (one value over all rows); `by <dims>` fixes a coarser grain. e.g. "a student's credits exceed 0.0001 of the global total":
      auto grand_total <- sum(enroll.credits) by *;
      select student.id, --sum(enroll.credits) as student_total
      having student_total > 0.0001 * grand_total
  * To filter rows by an aggregate condition based on inputs before filtering, write the aggregate directly in WHERE using inline grouping `agg(x) by grain`:
      Use an inline condition if you need to filter inside those.
      where enroll.year = 2020 and course.credits > 1.2 * avg(course.credits ? explicit_other_condition) by course.department
      select course.name, course.credits
- Operator precedence (highest binds first; use `(...)` to override):
  1. Primaries: literal, identifier, function call, parenthetical `(...)`, member access (`.`, `[]`, `::` cast).
  2. Inline filter `x ? cond` — `?` takes a primary on the left, so wrap any arithmetic in parens: `(a - b) ? cond`, NOT `a - b ? cond` (the latter binds `?` to `b` alone).
  3. Multiplicative: `*`, `/`, `%`.
  4. Additive / string concat: `+`, `-`, `||`.
  5. Comparison`=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `not like`, `not ilike`, `between … and …`, `in (…)`, `not in (…)`, `is null`.
  6. Logical `and`.
  7. Logical `or`.
- Always use a reasonable `LIMIT` for final queries if unspecified and the request is not for a time series or line chart.
- Self-referential queries — relating a row to OTHER rows of the same set (period-over-period, previous/next value, running total, share of a group total, rank): Default to WINDOW function, NOT a re-grained `by (key +/- N)` aggregate. Two aggregates defined at different derived grains will not join back and silently produce NULL; the window carries the related value onto the current row instead (same week PRIOR year = `lag(metric, 53) over (order by week_seq)`; same week NEXT year = `lead(metric, 53) over (order by week_seq)` — `lag` looks back, `lead` looks ahead).
- Window functions use SQL-style syntax:
  * Ranking: `rank(<key>) over (partition by <group> order by <expr> desc)` — e.g. `rank(name) over (partition by state order by sum(births) desc) as top_name`. `partition by` is OPTIONAL (omit for a single global window). `dense_rank`/`row_number` take the same shape.
  * Multi-key ranking: `rank(a, b) over (...)` — all comma-separated args are equal-status grain keys (used when ranking ROLLUP output where the grain spans multiple columns).
  * `partition by` accepts arbitrary expressions, not just identifiers: `partition by upper(student.state), case when student.gpa >= 3.5 then 1 else 0 end`.
  * Aggregates as windows: `sum(x) over (partition by g order by t)` for running totals. Without `order by`, a partitioned aggregate collapses to a plain grouped aggregate — write `sum(x) by g` directly instead of `sum(x) over (partition by g)`.
  * lag/lead: `lag(<field>, <offset>) over (partition by <g> order by <expr>)` fetches the value <offset> rows BACK; `lead(<field>, <offset>) over (...)` fetches it <offset> rows AHEAD. Offset is optional and defaults to 1. Examples: prior row = `lag(amount, 2) over (order by date asc) as prev_amount`; next-year same week = `lead(weekly, 53) over (order by week_seq asc) as next_year`.
- All functions have parenthese (e.g. `sum(births)`, `date_part('year', enroll.date)`). For no arguments, use empty parentheses (e.g. `current_date()`).
- Multi-level grouping (ROLLUP / CUBE / GROUPING SETS) attaches to an aggregate with a `by` clause and computes that aggregate at multiple grain levels in one pass:
  * `agg(<expr>) by rollup d1, d2` → grouping sets `(d1, d2)`, `(d1)`, `()`. Standard SQL ROLLUP semantics, useful for subtotals + grand total.
  * `agg(<expr>) by cube d1, d2` → every subset of the grouping keys.
  * `agg(<expr>) by grouping sets (d1, d2), (d1), ()` → arbitrary, explicit grouping combinations. Parens around each set; `()` is the grand total.
  * The `by rollup|cube|grouping sets ...` clause attaches to ONE aggregate. When several aggregates need the same expansion, wrap them in a `def` macro so the rollup spec stays consistent:
        def rollup_avg(metric) -> avg(metric::numeric(12,2)) by rollup enroll.department, enroll.year;
        select enroll.department, enroll.year, @rollup_avg(enroll.credits) as agg1, @rollup_avg(enroll.grade_points) as agg2;
  * `grouping(<field>)` returns 1 when the field has been rolled up at that row, 0 otherwise — use it (or its sum, e.g. `grouping(a) + grouping(b)`) to compute the hierarchy level. Detection by output NULL works only when the source has no real NULLs in the rolled columns; when in doubt, prefer `grouping()`.
- Use `::type` casting, e.g., `"2020-01-01"::date`.
- Date_parts have no quotes; use `date_part(enroll.date, year)` instead of `date_part(enroll.date, 'year')`. Prefer idiomatic function casts (year(enroll.date) instead of date_part(enroll.date, year)) when possible.
- Comments use `#` only, per line.
- For complex logic, break down your query into concept declarations that can be reused
- Two example queries (more patterns — ROLLUP/grouping, ranking-within-rollup, multi-select align — are in `trilogy agent-info syntax`):

Query 1: For names with more than 10 births in vermont ever, find the top 10 names by total births
across the US in the 1940s and 1950s for Idaho, along with their Vermont births and ranks within Idaho
and nationally.
```
# break up a query by defining resusable components
auto all_births <- sum(births);

# can force an aggregate rather than getting the implicit
# aggregate of the select, so here we get briths by name, no state
auto births_by_name_usa_wide <- sum(births) by name;

# can push filters into aggregates, especially
# useful for where filtering.
auto vermont_births <- sum(births ? state = 'VT');

where year between 1940 and 1950
and vermont_births>10
and state = 'ID'
SELECT
      name,
      state,
      all_births,
      vermont_births,
      rank(name) over (partition by state order by all_births desc) AS state_rank,
      rank(name) over (order by births_by_name_usa_wide desc) AS all_rank
  having
      all_rank<11

  order by
    all_rank asc
    limit 5;
```

Query 2: for students with significant enrollments between 2000 and 2002, find the
students with average daily enrollments >10 between 2002 and january 31 2010
```
where enroll.date between '2002-01-01'::datetime and '2010-01-31'::datetime
and count(enroll.id ? year(enroll.date::datetime) between 2000 and 2002) by student.name > 1000
  select
      student.name,
      count(enroll.id) AS total_enrollments,
      total_enrollments / date_diff(min(enroll.date.date), max(enroll.date.date), DAY) AS average_daily_enrollments
 having
    average_daily_enrollments > 10
  order by
    total_enrollments desc;
```

When several columns have the same calculation factor it into a `def` function
to keep queries concise.
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
# call by name — they are noise in the reference and (e.g. `union`) tempt agents
# into complex constructs not worth using yet. Arithmetic is written with
# operators (`a + b`, not `add(a, b)`); member/index access with `.`/`[]`;
# parentheses/aliases/constants are surface syntax, not callable functions.
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
