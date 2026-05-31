# Trilogy failure analysis — 20260531-151900

- Run `20260531-151859_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 181 | failed: 25 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 13 | 52% |
| `other` | 9 | 36% |
| `syntax-missing-alias` | 2 | 8% |
| `type-error` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy file write query92.preql --content import raw.web_sales as web_sales;

# Filter to manufacturer 350 and date window
where web_sales.item.manufacturer…* avg_discount_by_item);

select
    total_ext_discount as total_extended_discount_amount
order by
    total_extended_discount_amount desc
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...r item across the date window ??? auto avg_discount_by_item <- a...

  Write stats: received 662 chars / 662 bytes; tail: …'\\norder by\\n
  total_extended_discount_amount desc\\nlimit 100;'.
  ```
- `trilogy file write query92.preql --content import raw.web_sales as web_sales;

# Average discount per item across the date window
auto avg_discount_by_item <…* avg_discount_by_item);

select
    total_ext_discount as total_extended_discount_amount
order by
    total_extended_discount_amount desc
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...ceeds 1.3x the item's average ??? auto total_ext_discount <- sum...

  Write stats: received 662 chars / 662 bytes; tail: …'\\norder by\\n
  total_extended_discount_amount desc\\nlimit 100;'.
  ```
- `trilogy file write query94.preql --content import raw.web_sales as web_sales;

# Filter to qualifying web sales lines
where web_sales.ship_date.date between …,
    sum(web_sales.ext_ship_cost) as total_extended_shipping_cost,
    sum(web_sales.net_profit) as total_net_profit
order by order_count
limit 100;`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...evel aggregates for filtering ??? auto order_warehouse_count <-

  Write stats: received 818 chars / 818 bytes; tail: …'_profit) as
  total_net_profit\\norder by order_count\\nlimit 100;'.
  ```
- `trilogy file write query95.preql --content import raw.web_sales as web_sales;

# Filter conditions on the web_sales data
where web_sales.ship_date.date betwe…um(web_sales.net_profit) as total_net_profit
having order_count > 0
  and warehouse_count > 1
  and has_return = true
order by order_count
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...nes from different warehouses ??? auto warehouse_count <- count_...

  Write stats: received 1075 chars / 1075 bytes; tail: …' > 1\\n  and has_return
  = true\\norder by order_count\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can … count((store_set_cust, store_set_item) ? (store_set_cust in catalog.bill_customer.id and store_set_item in catalog.item.id)) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 10, column 44.
  Expected one of:
          * SELECT

  Location:
  ...illing_customer.id is not null ??? ;  # Define store set: distinc...

  Write stats: received 1188 chars / 1188 bytes; tail: …' store_set_item in
  catalog.item.id)) as both_sets\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can …
    count(catalog_pair ? catalog_pair not in store_pair) as catalog_only,
    count(store_pair ? store_pair in catalog_pair) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 9, column 94.
  Expected one of:
          * COMMA

  Location:
  ...g, ',', store.item.id::string) ??? ) ? store.date.year = 2000 and...

  Write stats: received 922 chars / 922 bytes; tail: …'e_pair ? store_pair in
  catalog_pair) as both_sets\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can …
    count(catalog_pair ? catalog_pair not in store_pair) as catalog_only,
    count(store_pair ? store_pair in catalog_pair) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 9, column 78.
  Expected one of:
          * COMMA

  Location:
  ...stomer.id, ',', store.item.id) ??? ) ? store.date.year = 2000 and...

  Write stats: received 890 chars / 890 bytes; tail: …'e_pair ? store_pair in
  catalog_pair) as both_sets\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can …
    count(catalog_pair ? catalog_pair not in store_pair) as catalog_only,
    count(store_pair ? store_pair in catalog_pair) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 9, column 78.
  Expected one of:
          * COMMA

  Location:
  ...stomer.id, ',', store.item.id) ??? );  # Define catalog set: (bil...

  Write stats: received 888 chars / 888 bytes; tail: …'e_pair ? store_pair in
  catalog_pair) as both_sets\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can …
    count(catalog_pair ? catalog_pair not in store_pair) as catalog_only,
    count(store_pair ? store_pair in catalog_pair) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'store.item.id') at line 9,
  column 52.
  Expected one of:
          * HASH_TYPE
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...sh(store.billing_customer.id, ??? store.item.id);  # Define cata...

  Write stats: received 862 chars / 862 bytes; tail: …'e_pair ? store_pair in
  catalog_pair) as both_sets\\nlimit 100;'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;

# Merge the shared concepts so they can …
    count(catalog_pair ? catalog_pair not in store_pair) as catalog_only,
    count(store_pair ? store_pair in catalog_pair) as both_sets
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 9, column 76.
  Expected one of:
          * COMMA

  Location:
  ...r.id * 1000000 + store.item.id ??? );  # Define catalog set: (bil...

  Write stats: received 884 chars / 884 bytes; tail: …'e_pair ? store_pair in
  catalog_pair) as both_sets\\nlimit 100;'.
  ```
- `trilogy run query97.preql`

  ```text
  --> 10:5
     |
  10 |     ? store.date.year = 2000 and store.billing_customer.id is not null;
     |     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...0 + store.item.id::bigint     ??? ? store.date.year = 2000 and s...
  ```
- `trilogy file write query98.preql --content import raw.physical_sales as store_sales;

# Filter by item category and sale date
where store_sales.item.category…y asc nulls first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class_sales asc nulls first;`

  ```text
  refused to write 'query98.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_total_ext_price <- s...

  Write stats: received 1030 chars / 1030 bytes; tail: …'ion asc nulls first,\\n
  pct_of_class_sales asc nulls first;'.
  ```
- `trilogy file write query99.preql --content import raw.catalog_sales as catalog_sales;

# Only sales where warehouse, shipping mode, and call center are all r…gt_120
order by warehouse_name20 asc nulls first,
         ship_mode_type asc nulls first,
         call_center_name_lower asc nulls first
limit 100;`

  ```text
  refused to write 'query99.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('NULL', 'null')]

  Location:
  ...r shipping lag (days_to_ship) ??? auto lag_bucket <- case(     c...

  Write stats: received 1655 chars / 1655 bytes; tail: …',\\n
  call_center_name_lower asc nulls first\\nlimit 100;'.
  ```

### `other`

- `trilogy run query94.preql`

  ```text
  (_duckdb.BinderException) Binder Error: No function matches
  the given name and argument types 'bool_or(INTEGER)'. You might need to add
  explicit type casts.
          Candidate functions:
          bool_or(BOOLEAN) -> BOOLEAN


  LINE 47:     bool_or(CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else...
               ^
  [SQL:
  WITH
  questionable as (
  SELECT
      "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
      "web_sales_web_sales"."WS_WAREHOUSE_SK" as "web_sales_warehouse_id"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      "web_s
  …
  y"."web_sales_order_number"
  WHERE
      "uneven"."_virt_agg_count_distinct_3536447574926385" > 1

  GROUP BY
      1),
  young as (
  SELECT
      count("concerned"."web_sales_order_number") as "order_count"
  FROM
      "concerned")
  SELECT
      coalesce("young"."order_count",0) as "order_count",
      "sweltering"."total_extended_shipping_cost" as
  "total_extended_shipping_cost",
      "sweltering"."total_net_profit" as "total_net_profit"
  FROM
      "young"
      FULL JOIN "sweltering" on 1=1
  ORDER BY
      coalesce("young"."order_count",0) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query94.preql`

  ```text
  Cannot compare BOOL
  (sum(ref:web_sales.is_returned)<['ref:web_sales.order_number']>) and INTEGER
  (0) of different types with operator = in
  sum(ref:web_sales.is_returned)<['ref:web_sales.order_number']> = 0
  ```
- `trilogy run query94.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column
  web_sales_web_site_company_name must appear in the GROUP BY clause or be used
  in an aggregate function
  [SQL:
  WITH
  questionable as (
  SELECT
      "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
      "web_sales_web_sales"."WS_WAREHOUSE_SK" as "web_sales_warehouse_id"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  juicy as (
  SELECT
      "web_sales_ship_address_customer_address"."CA_STATE" as
  "web_sales_ship_address_state",
      "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
      "web_s
  …
  es_web_site_company_name" = 'pri' and
  bool_and(coalesce("cooperative"."web_sales_is_returned",False) = False)
  ),
  concerned as (
  SELECT
      count("vacuous"."web_sales_order_number") as "order_count"
  FROM
      "vacuous")
  SELECT
      coalesce("concerned"."order_count",0) as "order_count",
      "uneven"."total_extended_shipping_cost" as "total_extended_shipping_cost",
      "uneven"."total_net_profit" as "total_net_profit"
  FROM
      "uneven"
      FULL JOIN "concerned" on 1=1
  ORDER BY
      coalesce("concerned"."order_count",0) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query94.preql`

  ```text
  Cannot compare BOOL (ref:web_sales.is_returned) and INTEGER
  (1) of different types with operator = in ref:web_sales.is_returned = 1
  ```
- `trilogy run query95.preql`

  ```text
  HAVING references 'local.warehouse_count',
  'local.has_return', which are not in the SELECT projection (line 15). Add them
  to SELECT, each prefixed with `--` so they stay out of the output rows — keep
  your HAVING as-is:
      select <your existing columns>, --local.warehouse_count, --local.has_return
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query95.preql`

  ```text
  (_duckdb.BinderException) Binder Error: No function matches
  the given name and argument types 'bool_or(INTEGER)'. You might need to add
  explicit type casts.
          Candidate functions:
          bool_or(BOOLEAN) -> BOOLEAN


  LINE 20:     bool_or(CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else...
               ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "web_sales_web_sales"."WS_EXT_SHIP_COST" as "web_sales_ext_ship_cost",
      "web_sales_web_sales"."WS_NET_PROFIT" as "web_sales_net_profit",
      "web_sales_web_sales"."WS_ORDER_NUMBER" as "web_sales_order_number",
      "web_sales_web_sales"."WS_WAR
  …
   "order_count",
      "concerned"."total_extended_ship_cost" as "total_extended_ship_cost",
      "concerned"."total_net_profit" as "total_net_profit"
  FROM
      "concerned"
      INNER JOIN "yummy" on "concerned"."has_return" is not distinct from
  "yummy"."has_return" AND "concerned"."warehouse_count" =
  "yummy"."warehouse_count"
  WHERE
      coalesce("concerned"."warehouse_count","yummy"."warehouse_count") > 1 and
  coalesce("concerned"."has_return","yummy"."has_return") = True

  ORDER BY
      coalesce("yummy"."order_count",0) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query97.preql`

  ```text
  Filter item with non ref content (ref:local.store_cust,
  ref:local.store_item) (<class 'trilogy.core.models.core.TupleWrapper'>) not yet
  supported
  ```
- `trilogy run query97.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "thoughtful" not found!
  Candidate tables: "juicy"

  LINE 104:     CASE WHEN "thoughtful"."store_cust" in (select
  juicy."catalog_cust...
                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "catalog_bill_customer_id",
      "store_store_sales"."SS_SOLD_DATE_SK" as "store_date_id"
  FROM
      "store_sales" as "store_store_sales"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1,
      2),
  sparkling as (
  SELECT
      "catalog_catalog_sales"."CS_BILL_CUSTOMER_SK" as
  "catal
  …
  og_item" from juicy where juicy."catalog_item" is
  not null) THEN "thoughtful"."store_cust" ELSE NULL END as
  "_virt_filter_store_cust_5360552361966666"
  FROM
      "juicy"
  GROUP BY
      1,
      2,
      3,
      "juicy"."catalog_bill_customer_id")
  SELECT
      count("sweltering"."_virt_filter_store_cust_5360552361966666") as
  "store_only",
      count("sweltering"."_virt_filter_catalog_cust_6528389102306989") as
  "catalog_only",
      count("sweltering"."_virt_filter_store_cust_5666476168746148") as
  "both_sets"
  FROM
      "sweltering"
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:store select max(store.billing_customer.id) as max_cust, max(store.item.id) as max_item limit 1;`

  ```text
  Unable to import
  'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\2026053
  1-151859_enriched\workspace\_worker_3\raw\store_sales.preql': [Errno 2] No such
  file or directory:
  'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpcds_agent\\results\
  \20260531-151859_enriched\\workspace\\_worker_3\\raw\\store_sales.preql'.
  ```

### `syntax-missing-alias`

- `trilogy file write query94.preql --content import raw.web_sales as web_sales;

select
    count(web_sales.order_number) as order_count,
    sum(web_sales.ext…warehouse.id) by web_sales.order_number > 1
  and sum(cast(web_sales.is_returned, int)) by web_sales.order_number = 0
order by order_count
limit 100;`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `int)) by web_sales.order_number = 0
  order by order_count
  limit 100; as int_by_web_sales_order_number_0_order_by`
  Location:
  ...um(cast(web_sales.is_returned, ??? int)) by web_sales.order_numb...

  Write stats: received 558 chars / 558 bytes; tail: …'y web_sales.order_number =
  0\\norder by order_count\\nlimit 100;'.
  ```
- `trilogy file write query94.preql --content import raw.web_sales as web_sales;

select
    count(web_sales.order_number) as order_count,
    sum(web_sales.ext…warehouse.id) by web_sales.order_number > 1
  and sum(cast(web_sales.is_returned, int)) by web_sales.order_number = 0
order by order_count
limit 100;`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `int)) by web_sales.order_number = 0
  order by order_count
  limit 100; as int_by_web_sales_order_number_0_order_by`
  Location:
  ...um(cast(web_sales.is_returned, ??? int)) by web_sales.order_numb...

  Write stats: received 558 chars / 558 bytes; tail: …'y web_sales.order_number =
  0\\norder by order_count\\nlimit 100;'.
  ```

### `type-error`

- `trilogy run query97.preql`

  ```text
  Invalid argument type 'INTEGER' passed into CONCAT function
  in position 1 from concept: store.billing_customer.id. Valid: 'STRING'.
  ```
