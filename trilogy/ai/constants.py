from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = """Trilogy statements define a semantic model or query. If a user is asking for data, they want a SELECT.
Semantic model statements:
- import <> imports a model to reuse; its fields become available. Imports CHAIN: when an imported model itself imports others (e.g. a fact table foreign-key-linked to its dimensions), reach those by chaining the path — after `import orders as orders;` write `orders.customer.name`. Import only the model(s) you take measures from; do NOT separately import a model already reachable by chaining (a separate import is a disconnected copy that will not join). A field belongs to exactly one model — never invent intermediate nesting: it is `orders.amount`, never `orders.date.amount`.
- merge <a> into <b> links a concept from one model to another so the models that own them join. In general the two sides cover different-sized sets — merge the subset into the superset and mark the superset target with `~`: `merge inventory.item.id into ~store_sales.item.id;`. A plain `merge a into b` (no `~`) asserts strict equivalence — the two concepts are exactly the same set. When a query needs two separate models that share a concept (e.g. two fact tables each linked to `item`), import both and merge their shared concept before the query — one merge per concept. Use `merge` in the query; never edit model files to wire a join.
- key|property|auto|metric defines fields locally. The output will also be visible in fields available to use, so you generally don't need to edit these unless requested.
- `parameter NAME TYPE [default <literal>];` declares a runtime parameter — a value supplied at execution time by `trilogy run <file>.preql --param NAME=VALUE` (multiple `--param` flags allowed). Reference it as a regular field anywhere a value is valid. Without a `default`, the file requires `--param` at run-time; add `default <literal>` to make local validation work without flags.
- datasource statements define a datasource, which is a mapping of fields to a SQL database table. The left side is the SQL column name, the right side is the field name.

SELECT RULES:
- No FROM, JOIN, GROUP BY, SUB SELECTS, DISTINCT, UNION, or SELECT *. The most common SUB SELECTS misuse is `IN (select ...)` to filter on a related dimension — that does not work. Instead, reference the dimension by its dotted path; Trilogy auto-resolves the join:
  * Wrong (SQL-style subselect): `where store_sales.store_id in (select store_id where store.state = 'TN')`
  * Right (dot-path on the related dim): `where store_sales.store.state = 'TN'`
  This pattern generalises: any "filter the fact by some attribute of a related entity" → reach across the import chain (`fact.dim.attr`) and put it in WHERE.
- Existence / non-existence across two models that are NOT directly connected (no shared dotted path): import both, then test a linking key with `in` / `not in` against the OTHER model's key. This is the anti-join / semi-join — it is column-level (`key not in other.key`), NOT a subquery, and needs no merge. Use it for "rows with (no) matching record in another model":
  * No matching record (anti-join): `import customers as c; import orders as o; where c.id not in o.customer_id` → customers who never placed an order.
  * Has a matching record (semi-join): `where c.id in o.customer_id` → customers who have placed an order.
  Prefer this over an aggregate hack like `sum(o.amount) by c.id is null` — a nullable measure can be null even when a matching row exists, so it silently mis-classifies.
- Membership in a COMPUTED set of values (the "filter X to rows whose value is in a set produced by another query") — the closest thing to a SQL `IN (subquery)`: define the set as a derived concept (filter it with `?`), then test membership with `in` against that concept. The right side is a concept, not a literal list — no `(select ...)`. Both sides may be expressions:
  * Build the set, then filter: `auto big_zip <- customer.address.zip ? (count(customer.id ? customer.preferred_cust_flag = 'Y') by customer.address.zip) > 10;` then `where substring(store.zip, 1, 2) in substring(big_zip, 1, 2)` → stores whose 2-digit zip-prefix matches a high-preferred-customer zip. The membership compares the LEFT expression against every value of the RIGHT concept (a semi-join over a value set), so mind the grain of each side — `substring(...)` on both sides matches prefixes, not full values.
- Never write the `distinct` keyword. `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count the distinct values of a non-key property.
- All fields exist in a global namespace; field paths look like `order.product.id`. Always use the full path. NEVER include a from clause.
- If a field has a grain defined, and that grain is not in the query output, aggregate it to get desired result. 
- Newly created fields at the output of the select must be aliased with as (e.g. `sum(births) as all_births`). 
- Aliases cannot happen inside calculations or in the where/having/order clause. Never alias fields with existing names. 'sum(revenue) as total_revenue' is valid, but '(sum(births) as total_revenue) +1 as revenue_plus_one' is not.
- Automatic groups. NEVER include the GROUP BY clause for a select. Grouping is automatic by non-aggregated fields in the SELECT clause.
- You CAN dynamically group inline to get groups at different grains - ex:  `sum(metric) by dim1, dim2 as sum_by_dim1_dm2` for alternate grouping. If you are grouping a defined aggregate
- The `by` clause accepts bare identifiers (`by dim1, dim2`) OR arbitrary expressions wrapped in parens (`by (substring(phone, 1, 2), upper(name))`). Use parens whenever a `by` entry is anything other than a simple identifier — function calls, casts, arithmetic, etc. — e.g. `avg(price) by (substring(phone, 1, 2))`. Without the parens the parser will reject the expression form.
- Histograms / bucket-of-aggregates (counting entities by a per-entity metric): define the per-key metric with `by`, then SELECT it alongside `count(<other_key>)` — the outer select buckets by the metric and counts how many entities fall into each bucket. Wrap with `coalesce(..., 0)` to include entities whose underlying rows are missing (the left-join equivalent — entities with zero matching child rows stay in the histogram). Example: bucket customers by how many of their orders match a predicate, including customers with zero matches:
      auto orders_per_customer <- count(orders.id ? not orders.comment like '%X%') by customer.id;
      select
          coalesce(orders_per_customer, 0) as order_count,
          count(customer.id) as customer_count
      order by customer_count desc, order_count desc;
  Do NOT instead write a per-customer SELECT (one row per customer) — that's the input to the bucketing, not the output.
- Count must specify a field (no `count(*)`) Counts are automatically deduplicated for keys. A count of a property counts the key. Use count_distinct for unique property members; do not use it on keys as it is identical to count.
- Since there are no underlying tables, sum/count of a constant should always specify a grain field (e.g. `sum(1) by x as count`). 
- Filtering on aggregates:
  - Use `field ? condition` for inline filters (e.g. `sum(x ? x > 0)`).
  * WHERE conditions are pushed before aggregation calculation for aggregates in the select. Where conditions DO NOT
    apply to other aggregates in the WHERE CLAUSE. 
  * HAVING can ONLY reference fields that appear in the SELECT projection — aggregates OR plain dimensions. Select what you filter on; hide it with a leading `--` when you don't want it in the output. Hide-and-HAVING a dimension (rather than moving it to WHERE) whenever WHERE would change an aggregate's or window's input — e.g. filtering one year AFTER a `lead/lag` over the full series:
      select customer.state, --sum(sales.amount) as total_sales, --store.id
      having total_sales > 1000 and store.id = 5
  * Nested aggregate — compare a per-entity total to the GROUP AVERAGE of those totals (a common "above 1.2x the group norm" ask). Define each grain with its own `by`, then filter in HAVING. Both derived metrics are selected hidden (`--`) so HAVING can reference them while the output stays just the id:
      auto cust_store_total <- sum(sales.amount) by sales.customer.id, sales.store.id;
      auto store_avg <- avg(cust_store_total) by sales.store.id;
      select sales.customer.id, --cust_store_total, --store_avg
      having cust_store_total > 1.2 * store_avg
  * To filter rows by an aggregate condition that is NOT in the output, write the aggregate directly in WHERE using inline grouping `agg(x) by grain`:
      Remember that other where conditions are not pushed through an aggregate in the where; use an inline condition if you
      need to filter inside those. 
      where store=1 and item.price > 1.2 * avg(item.price ? explicit_other_condition) by item.category
      select item.name, item.price
- Condition scoping: WHERE conditions DO NOT filter each other, and they DO NOT scope aggregates inside other conditions. Each aggregate computes over its own input, independent of sibling WHERE clauses. To scope an aggregate's input, push the filter INSIDE the aggregate with `?`:
  * Wrong (the `region = 'EUROPE'` clause does NOT restrict the `min(price)` in the other clause; min is over ALL rows):
      where region = 'EUROPE'
        and price = min(price) by part_id
  * Right (the `?` filter restricts the min's input to EUROPE rows per part):
      where region = 'EUROPE'
        and price = min(price ? region = 'EUROPE') by part_id
    Quick rules:
    - Global filters show in WHERE
    - Filtering on aggregates post-global filters can be done through the having clause with a hidden aggregate
    in select
    - OR in WHERE with `?` inline filter syntax
- Operator precedence (highest binds first; use `(...)` to override):
  1. Primaries: literal, identifier, function call, parenthetical `(...)`, member access (`.`, `[]`, `::` cast).
  2. Inline filter `x ? cond` — `?` takes a primary on the left, so wrap any arithmetic in parens: `(a - b) ? cond`, NOT `a - b ? cond` (the latter binds `?` to `b` alone).
  3. Multiplicative: `*`, `/`, `%`.
  4. Additive / string concat: `+`, `-`, `||`.
  5. Comparison (one per pair, NOT chainable — write `a < b and b < c`, not `a < b < c`): `=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `between … and …`, `in (…)`, `not in (…)`, `is null`.
  6. Logical `and`.
  7. Logical `or`.
- Always use a reasonable `LIMIT` for final queries unless the request is for a time series or line chart.
- Self-referential queries — relating a row to OTHER rows of the same set (period-over-period, previous/next value, running total, share of a group total, rank): Trilogy has no self-joins or subqueries, so reach for a WINDOW function, NOT a re-grained `by (key +/- N)` aggregate. Two aggregates defined at different derived grains will not join back and silently produce NULL; the window carries the related value onto the current row instead (e.g. same week last year = `lag(metric, 53) over (order by week_seq)`).
- Window functions use SQL-style syntax — the canonical form Trilogy parses, renders, and round-trips:
  * Ranking: `rank(<key>) over (partition by <group> order by <expr> desc)` — e.g. `rank(name) over (partition by state order by sum(births) desc) as top_name`. `partition by` is OPTIONAL (omit for a single global window). `dense_rank`/`row_number` take the same shape.
  * Multi-key ranking: `rank(a, b) over (...)` — all comma-separated args are equal-status grain keys (used when ranking ROLLUP output where the grain spans multiple columns).
  * `partition by` accepts arbitrary expressions, not just identifiers: `partition by upper(country), case when region = 'EU' then 1 else 0 end`.
  * Aggregates as windows: `sum(x) over (partition by g order by t)` for running totals. Without `order by`, a partitioned aggregate collapses to a plain grouped aggregate — write `sum(x) by g` directly instead of `sum(x) over (partition by g)`.
  * lag/lead: `lag(<field>, <offset>) over (partition by <g> order by <expr>)`. Offset is optional and defaults to 1. Example: `lag(amount, 2) over (order by date asc) as prev_amount`.
- Functions. All function names have parenthese (e.g. `sum(births)`, `date_part('year', dep_time)`). For no arguments, use empty parentheses (e.g. `current_date()`).
- Multi-level grouping (ROLLUP / CUBE / GROUPING SETS) attaches to an aggregate with a `by` clause and computes that aggregate at multiple grain levels in one pass:
  * `agg(<expr>) by rollup d1, d2` → grouping sets `(d1, d2)`, `(d1)`, `()`. Standard SQL ROLLUP semantics, useful for subtotals + grand total.
  * `agg(<expr>) by cube d1, d2` → every subset of the grouping keys.
  * `agg(<expr>) by grouping sets (d1, d2), (d1), ()` → arbitrary, explicit grouping combinations. Parens around each set; `()` is the grand total.
  * The `by rollup|cube|grouping sets ...` clause attaches to ONE aggregate. When several aggregates need the same expansion, wrap them in a `def` macro so the rollup spec stays consistent:
        def rollup_avg(metric) -> avg(metric::numeric(12,2)) by rollup item.category, item.class;
        select item.category, item.class, @rollup_avg(quantity) as agg1, @rollup_avg(price) as agg2;
  * `grouping(<field>)` returns 1 when the field has been rolled up at that row, 0 otherwise — use it (or its sum, e.g. `grouping(a) + grouping(b)`) to compute the hierarchy level. Detection by output NULL works only when the source has no real NULLs in the rolled columns; when in doubt, prefer `grouping()`.
- Use `::type` casting, e.g., `"2020-01-01"::date`.
- Date_parts have no quotes; use `date_part(order_date, year)` instead of `date_part(order_date, 'year')`. Prefer idiomatic function casts (year(order_date) instead of date_part(order_date, year)) when possible.
- Comments use `#` only, per line.
- For complex logic, break down your query into concept declarations that can be resued
- Three example queries:

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
    
Query 2: for carriers with significant flights between 2000 and 2002, find the 
carriers with average daily flights >10 between 2002 and january 31 2010
``
where dep_time between '2002-01-01'::datetime and '2010-01-31'::datetime
and count(id2 ? year(dep_time::datetime) between 2000 and 2002) by carrier.name > 1000
  select
      carrier.name,
      count(id2) AS total_flights,
      total_flights / date_diff(min(dep_time.date), max(dep_time.date), DAY) AS average_daily_flights
 having
    average_daily_flights > 10
  order by
    total_flights desc;"

Query 3: store sales totals by item category and class, with subtotals for each
category and a grand total. The ROLLUP expands one aggregate into three grain
levels in one pass; `grouping()` tags which level each row is at so we can sort
totals above their children.
```
where store_sales.date.year = 2001
select
    store_sales.item.category,
    store_sales.item.class,
    sum(store_sales.net_profit) by rollup store_sales.item.category, store_sales.item.class as profit,
    grouping(store_sales.item.category) by rollup store_sales.item.category, store_sales.item.class as g_cat,
    grouping(store_sales.item.class)    by rollup store_sales.item.category, store_sales.item.class as g_class,
    g_cat + g_class as level  # 0 = leaf, 1 = category subtotal, 2 = grand total
order by
    level asc,
    profit desc
limit 100;
```

When several columns have the same calculation factor it into a `def` function
to keep queries concise.
```
def by_geo(metric) -> avg(metric::numeric(12,2))
    by rollup customer.address.country, customer.address.state, customer.address.county;

select
    customer.address.country,
    customer.address.state,
    customer.address.county,
    @by_geo(sales.quantity)   as avg_qty,
    @by_geo(sales.list_price) as avg_price
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
