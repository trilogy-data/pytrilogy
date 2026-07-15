from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_FAMILIES, FUNCTION_REGISTRY, function_family

RULE_PROMPT = r"""# Trilogy Syntax Guide

Trilogy statements define a semantic model or a query. Only selects return data.

import <model> as <alias>; — makes a model's fields available. When an imported model imports others (a fact table with imported dimensions); those are exposed as dot paths. `import enrollments as enroll;` enables accessing `enroll.student.name` if enroll imports students as studen. 
Typical usage only imports facts; dimensions will then be accessed through the nested import. Nested dimensions have all values; order.customers returns all customers, even those without orders. 

key | property | auto | metric — define new concepts in your script. These concepts (auto x <- ...) are definitions, NOT precomputed values: each reference expands in a query and re-evaluates in the referencing query's scope. 

parameter NAME TYPE [default <literal>]; — declares a runtime value supplied via trilogy run <file>.preql --param NAME=VALUE (repeat --param for several). Reference it like any field. Without default, required at run time. TYPE is any data type (see Data types); append `?` to allow null (`parameter cutoff date?;`).

## Combining models

| Goal | Use |
|---|---|
| Typical query | no select, no merge, no join, all fields accessed through dot-paths |
| Blend two models on shared keys inside one query | scoped `subset\|union join` (the default) |
| Make a connection universal to all queries in a file | `merge` |
| Stack subsets/channels as rows | `union(...)` |
| Rows in A but never in B (set difference) | `except(...)` |
| Rows present in every source (set intersection) | `intersect(...)` |

### Query-scoped join (the default)

A typical fact will have dimensions already merged in, and no joins are needed.

In complex cases - primarily joining fact models or a rowset output - you will want to use a query scoped join to tell the planner which keys it can join on; you are not explicitly defining a join, just allowing a path
to traverse the models to be used in the query generation.

subset|union join <a> = <b> [= <c>] blends concepts or expressions (from any source - models, rowsets etc) inside one SELECT by DECLARING how their value domains relate. A join declares DOMAIN knowledge, never row intent:

- `subset join a = b` declares a's values are contained in b's (a ⊆ b). b is authoritative for the key; a's rows all find a partner.
- `union join a = b` declares neither domain contains the other; the key is the coalesce of both sides and unmatched rows from both sides are kept.

Rendering is always row-preserving: no join ever silently drops a row. The optimizer narrows to directional/INNER joins only when provably row-identical (an unfiltered authoritative side). Row restriction is ALWAYS an explicit predicate: to get an intersection (customers who have orders, items in both years), add explicit conditions — `where <optional side attr> is not null` on each side you require. A one-sided `is not null` keeps the other side's exclusive rows.

There is no 'inner' join. Is explicit conditions (is not null, etc) to restrict results.

Place it right after the select list (the SQL-like spot).

A union key-group must be entirely union (no mixing with subset on the same key; union join a = b = c chains one group); subset joins mix freely.

Chain = c to pull additional concepts into a join. Joins can be on expressions: a computed/offset key (`union join a.id + 53 = b.id`), an aggregate, or a window; only `=` equality is supported.

Joins do NOT drop nulls. NULL is not a value: nullability never affects the declared domain relation, and NULL keys match null-safely. To filter out nulls, explicitly use not-null conditions.

Join on the full grain. When blending two FACT models, write one join clause per key in their shared grain.
trilogy explore prints each fact's grain as @<k1, k2> (e.g. @<order_number, item.id>); a composite grain needs BOTH union join a.order_number = b.order_number AND union join a.item.id = b.item.id.
Matching only one key of a multi-key grain may cause duplication.

Joins do *not restrict results*; they only enable traversal across models with those keys. To filter, explicit add a condition on the element to restrict to. 

To restrict returned values, explicitly add conditions. union join x=y does not restrict the result to where x=y unless that condition is defined in the where clause.

Full example: trilogy agent-info syntax example scoped-join.

merge (model-level)

merge <a> into <b> is the persistent equivalent of a join (whole query/file). Prefer a scoped join; use merge only when the connection is universal. merge a into ~b declares a SUBSET domain (a ⊆ b, like subset join a = b); plain merge a into b declares EQUAL domains — the strongest claim, letting the planner treat either side as authoritative and narrow joins to INNER. A lying declaration (data outside the declared domain) is an author error: the narrowed join drops the violating rows. One merge per shared concept.

union (row stacking)

union((armA), (armB), ...) -> (out1, out2, ...) row-stacks self-contained select arms positionally (SQL UNION ALL) into one named result. Arms match by column position (same count/order/types as outputs) and may contain full trilogy select statements (with their own filters + local joins). Usable in a rowset — with combined as union(...) -> (...) with outputs using standard rowset namespaceing <rowset_name>.<path>.

Each arm is an ordinary select and DEDUPLICATES to its own output grain BEFORE stacking (cross-arm duplicates still stack). Stacking raw measure rows and aggregating OUTSIDE the union silently undersums whenever two source rows in one arm project the same tuple (two same-priced sales in one week). Either aggregate INSIDE each arm (preferred: `sum(x) as v` per the arm's dims) or pull each arm's grain key through as an extra output column so every fact row stays distinct; downstream selects can simply ignore the key column.

Full example: trilogy agent-info syntax example union-stack-channels.

except / intersect (set operations)

except((armA), (armB), ...) -> (...) and intersect(...) share union's arm shape but are SQL SET operators: output rows are DISTINCT, whole rows compare null-safely (NULL matches NULL, same identity semantics as `in`/`not in` membership), and except subtracts later arms from the FIRST, left to right (arm order matters). Prefer except over multi-column `not in` when you also want the DISTINCT output rows ("combinations in A that never appear in B"); aggregate the outputs directly for a distinct-combination count.

Full example: trilogy agent-info syntax example except-intersect-setops.

## SELECT statements

```
<WITH NAME>?      # name the select to use later
WHERE?            # filters data BEFORE it reaches aggregates or windows
SELECT            # Defines the output grain for aggregates
  <EXPR> [AS <ALIAS>], ...
  SUBSET|UNION JOIN <a> = <b> [= <c>] ...   # one or more join concepts beyond model defaults (LEFT/FULL legacy aliases)
BY ROLLUP|CUBE|GROUPING SETS?  # multi-level grouping for all aggregates in the select
HAVING?           # filters final projection (after any grouping clause, as in SQL)
ORDER BY?
LIMIT?
```

If unspecified (no BY) aggregates always group to the grain of dimensions in the select, no matter where they appear in the query.

A CTE/Rowset - a named output - is defined by a select with a preceding `WITH <name> as`; reference it later as `<name>.<field>` or in a join as `<name>.<key> = other.<key>`. 
These are standalone statements, not part of a select, and create their own local outputs.

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

- **No FROM, GROUP BY, DISTINCT, SELECT \*, or SQL-style set operators between selects.** To stack rows use `union(...)`; for set difference/intersection use `except(...)`/`intersect(...)`; to blend fact models use a scoped join.
- **Grouping is automatic** by the non-aggregated fields in the SELECT — never write GROUP BY. Aggregates inherit the grain of the select output list automatically, in where/select/having. 
   Use explicit grain agg(x) by <dims> as needed to override the default. 
   Use `by *` to aggregate across all data (a single row output).
   auto avg_credits_at_query_grain <- avg(enroll.credits); # responsive to the consuming query's grain
   auto avg_credits_single_row <- avg(enroll.credits) by *; # explicit single-row grain, regardless of query grain
   To weight a related dimension property once per fact row before aggregating it, first project it to the fact grain with `group`: `auto row_birth_year <- group(enroll.student.birth_year) by enroll.student_id, enroll.course_id; auto avg_birth_year <- avg(row_birth_year);`. Directly averaging the dimension property can retain its native entity grain instead of fact-row weighting.
- **Output rows are deduplicated to the select grain.** To preserve legitimate duplicate rows, such as one output row per matching fact when the projected columns repeat, include the fact's grain keys in the select, hidden with the PREFIX `--` if they shouldn't appear in the output.
- **Never write `distinct`.** `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count distinct values of a non-key property.
- **Count a COMPOSITE grain with `grain(...)`.** When the thing you are counting is identified by several keys together, name them: `count(grain(order_id, item.id))` counts order+item combinations; `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations. `grain(...)` is NEVER NULL, so a combination with a missing member still counts - unlike `count(<nullable property>)`, which skips those rows and silently undercounts. Never pick one column of a multi-key grain and count it: a coarser key counts its own distinct values, and a nullable one drops rows.
- **One-column expression subqueries are supported.** Use `(select ...)` only where a scalar value or membership set is expected, and project exactly one column. This does not add SQL-style `FROM (SELECT ...)` table subqueries. For ordinary related-entity filters, prefer the direct dot-path:
  - Wrong: `where enrollments.student_id in (select student_id where student.state = 'TN')`
  - Right: `where enrollments.student.state = 'TN'`
- **-- is a HIDDEN field not a comment; it still changes query structure. Use # for comments
- **`count(1)` is invalid because a constant does not identify rows.** Count a concept with the intended identity, or use `count(grain(key1, key2))`; for conditional counts use `count(grain(key1, key2) ? condition)`. `sum(1)` remains available when a numeric row flag is explicitly intended.
- **Counting fact rows without a single unique row key: use `count(grain(<the fact's keys>))`** - e.g. `count(grain(fact.order_id, fact.item_id))`. (The older two-stage workaround - materialize a flag at the complete fact grain with `auto line_flag <- sum(case when fact.qualifies then 1 else 0 end) by fact.order_id, fact.item_id;` then `sum(line_flag)` - still works, but `grain()` says it directly.) Writing `sum(1)` only at the outer report grain does not preserve repeated fact lines.

### Fields and aliases
- Always use the full path (`enroll.student.id`) for a field; namespacing matters.
- Every new expression in the select output must be aliased with `as` (e.g. `sum(births) as all_births`).
- Aliases cannot appear inside calculations or in WHERE/HAVING/ORDER clauses: `sum(credits) as total_credits` is valid; `(sum(credits) as total_credits) + 1 as credits_plus_one` is not. Never alias a field to an existing name.
- Use a context dependent reasonable `LIMIT` on final queries if unspecified (data for charts typically must be complete)

## Filtering Data
```
WHERE <filter> 
SELECT
...
HAVING # filters data AFTER it has been aggregated or windowed
```
INLINE filter x ? cond <- filters the immediate prior expression (e.g. sum(x ? x > 0) is sum (x where x is more than 0)).

Note that aggregates/windows in WHERE do not filter the inputs to each other. You must use inline filters if you want a where clause aggregate/window to be filtered.

where student.state = 'TN' #  filters ALL Data to the state
select 
    student.id, 
    --sum(enroll.credits ? student.enrolled = True) as student_total # filters JUST the input to the sum
having 
    student_total > 0.0001 * grand_total # filters the final output rows
```

IMPORTANT: BE CAREFUL with window functions and the where clause - you must have all the rows required for the window range reach the window. Filtering in the HAVING
is often useful here. 

## SemiJoins

Semijoins are unique in that they do not require an explicit relationship to cross models, as the semijoin *is* a scoped intersection.

Membership in a computed set (SQL IN (subquery)): define the set as a derived concept (filter with ?), then test in against that concept.
The right side is a concept or expression, not subselect. (in fact either side can be an expression) membership compares the left expression against every value of the right concept (a semi-join over a value set):
```
auto big_zip <- student.zip ? (count(student.id ? student.honors = true) by student.zip) > 10;
# schools whose 2-digit zip-prefix matches a high-honors-student zip:
where substring(school.zip, 1, 2) in substring(big_zip, 1, 2)
```

Membership semantics (`in` / `not in`, scalar and tuple alike) are IDENTITY matching, not SQL three-valued logic:
- NULL matches NULL: a NULL key is in a set that contains a NULL, and `x in (1, null)` is true for NULL x.
- Total: membership is always TRUE or FALSE (never NULL), so it is safe as a projected boolean flag.
- `not in` is the EXACT complement of `in` — every row lands in exactly one side. There is no SQL NOT-IN footgun (a NULL in the set does not empty the result), and NULL-keyed rows are never silently dropped from either side.
- To reproduce strict SQL behavior, exclude NULLs explicitly: `where x is not null and x in some.set`.

 A fact model contains the full set of dimensional members (all students appear in `fact.students`), so:
- No matching record (anti-join): `where students.id not in enroll.student_id` is typically a tautology — `enroll.student_id` references the student table and contains all students. Use e.g. `where enroll.id is null select enroll.student.id` instead.
- Has a matching record (semi-join): `where students.id in ([some other student_list])` effectively filters across models that are not explicitly merged but share an ID (e.g. IDs from an external source).

## Aggregation and grouping

- Aggregates group at the query's automatic grain by default; 
- An aggregate without an explicit `by` grain is responsive, including when defined as an `auto` concept. Its grain is determined where it is consumed, not where it is declared: `auto avg_credits <- avg(enroll.credits);` becomes per-department when selected with `enroll.department`.
- override one aggregate's grain with inline grouping: `sum(metric) by dim1, dim2 as sum_by_dim1_dim2`.
- The `by` clause accepts bare identifiers (`by dim1, dim2`) OR arbitrary expressions wrapped in parens — function calls, casts, arithmetic: `avg(price) by (substring(phone, 1, 2))`.
- **Multi-level grouping** (ROLLUP / CUBE / GROUPING SETS) is a property of the WHOLE select — a clause after the select list (before `having`/`order by`/`limit`) that computes the query at multiple grain levels in one pass. It applies to EVERY aggregate in the select that has no explicit `by` grain, so there is exactly one consistent grouping:
  - `select d1, d2, agg(<expr>) as a by rollup (d1, d2)` → grouping sets `(d1, d2)`, `(d1)`, `()` — standard SQL ROLLUP, useful for subtotals + grand total.
  - `select d1, d2, agg(<expr>) as a by cube (d1, d2)` → every subset of the grouping keys.
  - `select d1, d2, agg(<expr>) as a by grouping sets ((d1, d2), (d1), ())` → arbitrary explicit combinations; parens around each set; `()` is the grand total.
  - `by rollup ()` (empty) rolls up over the select's own automatic grain.
  - A `having` clause comes AFTER the grouping clause — same relative order as SQL's `GROUP BY ... HAVING`, and it filters every grouping level (subtotals and grand total included): `select d1, sum(x) as t by rollup (d1) having t > 100 order by d1;`
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

## Data types

Types appear in concept/property declarations, `parameter` declarations, `type` aliases, and casts. A concept's type is written right after its name: `property user.email string;`, `key order.id int;`.

Scalar types: `string`, `bytes`, `bool`, `int`, `bigint`, `float`, `double`, `number`, `numeric(<precision>,<scale>)` (alias `decimal(p,s)`, e.g. `numeric(12,2)` for exact money), `date`, `datetime`, `timestamp`, `geography`, `any`.

Composite types:
- `array<T>` (alias `list<T>`) — an ordered list, e.g. `array<int>`.
- `map<K, V>` — e.g. `map<string, int>`.
- `struct<name1: T1, name2: T2, ...>` — a named record; access fields with `.`.
- `enum<T>[v1, v2, ...]` — a T constrained to the listed literals, e.g. `enum<string>['open', 'closed']`.

Cast a value with `::type` or `cast(<expr> as <type>)`, e.g. `"2020-01-01"::date`, `cast(credits as numeric(12,2))`.

### `?` — nullable, `~` — subset binding

Two type/concept modifiers use punctuation:

- **`?` after a type marks the concept (or parameter) as nullable** — its values may be NULL: `property user.middle_name string?;`, `parameter as_of date?;`. Without `?` a concept is non-nullable. 
- **`~` prefixing a concept marks it as a binding of a SUBSET of that concept's values** — i.e. this reference does not carry the full domain, only a partial set. 
It appears as a prefix on a select item (`~customer.id`) to flag the value as partial, and as `merge a into ~b` to declare a ⊆ b (the subset form of `merge`, equivalent to `subset join a = b`). A `~`-stamped value tells the planner not to treat this side as authoritative/complete for its concept.

## Expressions and miscellany

- **Operator precedence** (highest binds first; use `(...)` to override):
  1. Primaries: literal, identifier, function call, parenthetical `(...)`, member access (`.`, `[]`, `::` cast).
  2. Inline filter `x ? cond` — `?` takes a primary on the left, so wrap arithmetic in parens: `(a - b) ? cond`, NOT `a - b ? cond` (which binds `?` to `b` alone).
  3. Multiplicative: `*`, `/`, `%`.
  4. Additive / string concat: `+`, `-`, `||`.
  5. Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `not like`, `not ilike`, `between ... and ...`, `in (...)`, `not in (...)`, `is null`.
- String concatenation NULL semantics (all backends): `a || b` is NULL if either side is NULL; `concat(a, b, ...)` skips NULL arguments (all-NULL gives `''`); `concat_ws(sep, a, b, ...)` joins with `sep`, skipping NULL arguments and their separators (empty strings are kept).
  6. Logical `and`.
  7. Logical `or`.
- Cast with `::type`, e.g. `"2020-01-01"::date`.
- Date parts have no quotes: `date_part(enroll.date, year)`, never `date_part(enroll.date, 'year')`. Prefer idiomatic function forms when available: `year(enroll.date)`.
- `date_diff(start_date, end_date, unit)` computes `end_date - start_date`. Argument order matters: `date_diff('2020-01-01'::date, '2020-01-02'::date, day)` returns `1`; a shipping lag is `date_diff(sold_date, ship_date, day)`.
- All functions take parentheses; zero-argument functions use empty ones (`current_date()`).
- Comments use `#` only, per line. -- is NOT a comment.
- When several columns share the same calculation, factor it into a `def` macro (invoked with `@name(...)`); for complex logic, break the query into reusable concept declarations.

## Worked examples

**Reusable concepts, filtered aggregates, and dual ranks.** 
For names with more than 10 births in Vermont ever, find the top 10 names by total births across the US in the 1940s and 1950s for Idaho, along with their Vermont births and ranks within Idaho and nationally:

```
# break up a query by defining reusable components
# these are macros; they will be evaluated in select context
# and respond to that context
auto all_births <- sum(births);

# a rowset binds all_data.total_births;
# it is isolated and if reference din a query will *not* respond to local context
# it will be evaluated as the output of this select
with all_data as select sum(births) as total_births;

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
