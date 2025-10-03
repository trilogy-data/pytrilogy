from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.core.functions import FUNCTION_REGISTRY

RULE_PROMPT = """Trilogy statements define a semantic model or query. If a user is asking for data, they want a SELECT.
Semantic model statements:
- import <> imports a model to reuse. The output of imports will be visible in fields available to use.
- key|property|auto|metric defines fields locally. The output will also be visible in fields available to use, so you generally don't need to edit these unless requested.
- datasource statements define a datasource, which is a mapping of fields to a SQL database table. The left side is the SQL column name, the right side is the field name.

SELECT RULES:
- No FROM, JOIN, GROUP BY, SUB SELECTS, DISTINCT, UNION, or SELECT *.
- All fields exist in a global namespace; field paths look like `order.product.id`. Always use the full path. NEVER include a from clause.
- If a field has a grain defined, and that grain is not in the query output, aggregate it to get desired result. 
- If a field has a 'alias_for' defined, it is shorthand for that calculation. Use the field name instead of the calculation in your query to be concise. 
- Newly created fields at the output of the select must be aliased with as (e.g. `sum(births) as all_births`). 
- Aliases cannot happen inside calculations or in the where/having/order clause. Never alias fields with existing names. 'sum(revenue) as total_revenue' is valid, but '(sum(births) as total_revenue) +1 as revenue_plus_one' is not.
- Implicit grouping: NEVER include a group by clause. Grouping is by non-aggregated fields in the SELECT clause.
- You can dynamically group inline to get groups at different grains - ex:  `sum(metric) by dim1, dim2 as sum_by_dim1_dm2` for alternate grouping. If you are grouping a defined aggregate
- Count must specify a field (no `count(*)`) Counts are automatically deduplicated. Do not ever use DISTINCT.
- Since there are no underlying tables, sum/count of a constant should always specify a grain field (e.g. `sum(1) by x as count`). 
- Aggregates in SELECT must be filtered via HAVING. Use WHERE for pre-aggregation filters.
- Use `field ? condition` for inline filters (e.g. `sum(x ? x > 0)`).
- Always use a reasonable `LIMIT` for final queries unless the request is for a time series or line chart.
- Window functions: `rank entity [optional over group] by field desc` (e.g. `rank name over state by sum(births) desc as top_name`) Do not use parentheses for over.
- Functions. All function names have parenthese (e.g. `sum(births)`, `date_part('year', dep_time)`). For no arguments, use empty parentheses (e.g. `current_date()`).
- For lag/lead, offset is first: lag/lead offset field order by expr asc/desc.
- For lag/lead with a window clause: lag/lead offset field by window_clause order by expr asc/desc.
- Use `::type` casting, e.g., `"2020-01-01"::date`.
- Date_parts have no quotes; use `date_part(order_date, year)` instead of `date_part(order_date, 'year')`.
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
