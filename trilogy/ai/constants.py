from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = """Trilogy statements define a semantic model or query. If a user is asking for data, they want a SELECT.
Semantic model statements:
- import <> imports a model to reuse; its fields become available. Imports CHAIN: when an imported model itself imports others (e.g. a fact table foreign-key-linked to its dimensions), reach those by chaining the path — after `import orders as orders;` write `orders.customer.name`. Import only the model(s) you take measures from; do NOT separately import a model already reachable by chaining (a separate import is a disconnected copy that will not join). A field belongs to exactly one model — never invent intermediate nesting: it is `orders.amount`, never `orders.date.amount`.
- merge <a> into <b> links a concept from one model to another so the models that own them join. In general the two sides cover different-sized sets — merge the subset into the superset and mark the superset target with `~`: `merge inventory.item.id into ~store_sales.item.id;`. A plain `merge a into b` (no `~`) asserts strict equivalence — the two concepts are exactly the same set. When a query needs two separate models that share a concept (e.g. two fact tables each linked to `item`), import both and merge their shared concept before the query — one merge per concept. Use `merge` in the query; never edit model files to wire a join.
- key|property|auto|metric defines fields locally. The output will also be visible in fields available to use, so you generally don't need to edit these unless requested.
- datasource statements define a datasource, which is a mapping of fields to a SQL database table. The left side is the SQL column name, the right side is the field name.

SELECT RULES:
- No FROM, JOIN, GROUP BY, SUB SELECTS, DISTINCT, UNION, or SELECT *.
- Never write the `distinct` keyword. `count(<key>)` is already distinct because keys are unique; use `count_distinct(<property>)` to count the distinct values of a non-key property.
- All fields exist in a global namespace; field paths look like `order.product.id`. Always use the full path. NEVER include a from clause.
- If a field has a grain defined, and that grain is not in the query output, aggregate it to get desired result. 
- If a field has a 'alias_for' defined, it is shorthand for that calculation. Use the field name instead of the calculation in your query to be concise. 
- Newly created fields at the output of the select must be aliased with as (e.g. `sum(births) as all_births`). 
- Aliases cannot happen inside calculations or in the where/having/order clause. Never alias fields with existing names. 'sum(revenue) as total_revenue' is valid, but '(sum(births) as total_revenue) +1 as revenue_plus_one' is not.
- Implicit grouping: NEVER include a group by clause. Grouping is by non-aggregated fields in the SELECT clause.
- You can dynamically group inline to get groups at different grains - ex:  `sum(metric) by dim1, dim2 as sum_by_dim1_dm2` for alternate grouping. If you are grouping a defined aggregate
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
  * HAVING filters on an aggregate that IS in the SELECT output. HAVING can ONLY reference fields that appear in the SELECT projection — select the aggregate with an alias, then reference that alias:
      select customer.state, sum(sales.amount) as total_sales
      having total_sales > 1000
  * To filter rows by an aggregate condition that is NOT in the output, write the aggregate directly in WHERE using inline grouping `agg(x) by grain`:
      where item.price > 1.2 * avg(item.price) by item.category
      select item.name, item.price
- Use `field ? condition` for inline filters (e.g. `sum(x ? x > 0)`).
- Condition scoping: WHERE conditions DO NOT filter each other, and they DO NOT scope aggregates inside other conditions. Each aggregate computes over its own input, independent of sibling WHERE clauses. To scope an aggregate's input, push the filter INSIDE the aggregate with `?`:
  * Wrong (the `region = 'EUROPE'` clause does NOT restrict the `min(price)` in the other clause; min is over ALL rows):
      where region = 'EUROPE'
        and price = min(price) by part_id
  * Right (the `?` filter restricts the min's input to EUROPE rows per part):
      where region = 'EUROPE'
        and price = min(price ? region = 'EUROPE') by part_id
  This applies to aggregates anywhere — WHERE, HAVING, SELECT projections. If an aggregate should see only a subset, that subset goes inside `?`, never as a sibling WHERE condition.
- Operator precedence (highest binds first; use `(...)` to override):
  1. Primaries: literal, identifier, function call, parenthetical `(...)`, member access (`.`, `[]`, `::` cast).
  2. Inline filter `x ? cond` — `?` takes a primary on the left, so wrap any arithmetic in parens: `(a - b) ? cond`, NOT `a - b ? cond` (the latter binds `?` to `b` alone).
  3. Multiplicative: `*`, `/`, `%`.
  4. Additive / string concat: `+`, `-`, `||`.
  5. Comparison (one per pair, NOT chainable — write `a < b and b < c`, not `a < b < c`): `=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `between … and …`, `in (…)`, `not in (…)`, `is null`.
  6. Logical `and`.
  7. Logical `or`.
- Always use a reasonable `LIMIT` for final queries unless the request is for a time series or line chart.
- Window functions: `rank entity [optional over group] by field desc` (e.g. `rank name over state by sum(births) desc as top_name`) Do not use parentheses for over.
- Functions. All function names have parenthese (e.g. `sum(births)`, `date_part('year', dep_time)`). For no arguments, use empty parentheses (e.g. `current_date()`).
- For lag/lead, offset is first: lag/lead offset field order by expr asc/desc.
- For lag/lead with a window clause: lag/lead offset field by window_clause order by expr asc/desc.
- Use `::type` casting, e.g., `"2020-01-01"::date`.
- Date_parts have no quotes; use `date_part(order_date, year)` instead of `date_part(order_date, 'year')`. Prefer idiomatic function casts (year(order_date) instead of date_part(order_date, year)) when possible.
- Comments use `#` only, per line.
- Two example queries: "where year between 1940 and 1950
  select
      name,
      state,
      sum(births) AS all_births,
      sum(births ? state = 'VT') AS vermont_births,
      rank name over state by all_births desc AS state_rank,
      rank name by sum(births) by name desc AS all_rank
  having 
      all_rank<11
      and state = 'ID'
  order by 
    all_rank asc
    limit 5;", "where dep_time between '2002-01-01'::datetime and '2010-01-31'::datetime
  select
      carrier.name,
      count(id2) AS total_flights,
      total_flights / date_diff(min(dep_time.date), max(dep_time.date), DAY) AS average_daily_flights
  order by 
    total_flights desc;"""


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

FUNCTIONS = "\n".join(
    [
        render_function(v, example=FUNCTION_EXAMPLES.get(v))
        for x, v in FunctionType.__members__.items()
        if v in FUNCTION_REGISTRY
    ]
)

AGGREGATE_FUNCTIONS = [
    x
    for x, info in FunctionType.__members__.items()
    if x in FunctionClass.AGGREGATE_FUNCTIONS.value
]
