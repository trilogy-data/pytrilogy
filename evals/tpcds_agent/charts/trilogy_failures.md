# Trilogy failure analysis — 20260531-151900

- Run `20260531-151859_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 127 | failed: 8 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 7 | 88% |
| `syntax-missing-alias` | 1 | 12% |

## Detail

### `syntax-parse`

- `trilogy file write query92.preql --content import raw.web_sales as web_sales;

# Filter date window and manufacturer
where web_sales.sold_date.date between '…eb_sales.ext_discount_amt > 1.3 * avg_ext_discount_by_item) as total_extended_discount_amount
order by total_extended_discount_amount desc
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...(within the same date window) ??? auto avg_ext_discount_by_item

  Write stats: received 684 chars / 684 bytes; tail: …'ount\\norder by
  total_extended_discount_amount desc\\nlimit 100;'.
  ```
- `trilogy file write query93.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

# Find store_sales rows that ha… and store_returns.reason.desc = 'reason 28') as total_adjusted_amount
order by total_adjusted_amount nulls first, customer_id nulls first
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...* store_sales.sales_price     ??? where store_sales.item.item_sk...

  Write stats: received 1016 chars / 1016 bytes; tail: …'usted_amount nulls
  first, customer_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales.preql:ss --import raw/catalog_sales.preql:cs select concat(cast(ss.customer.customer_sk, string), '-', cast(ss.item.item_sk, string)) limit 3;`

  ```text
  --> 3:43
    |
  3 | select concat(cast(ss.customer.customer_sk, string), '-',
  cast(ss.item.item_sk, string)) limit 3;
    |                                           ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t(cast(ss.customer.customer_sk ??? , string), '-', cast(ss.item.i...
  ```
- `trilogy run --import raw/store_sales.preql:ss --import raw/catalog_sales.preql:cs select concat(cast(ss.customer.customer_sk, string)::string, '-', cast(ss.item.item_sk, string)::string) limit 3;`

  ```text
  --> 3:43
    |
  3 | select concat(cast(ss.customer.customer_sk, string)::string, '-',
  cast(ss.item.item_sk, string)::string) limit 3;
    |                                           ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t(cast(ss.customer.customer_sk ??? , string)::string, '-', cast(s...
  ```
- `trilogy run --import raw/store_sales.preql:ss --import raw/catalog_sales.preql:cs select count_distinct(concat(ss.customer.customer_sk::string, '-', ss.item.item_sk::string) ? ss.date_dim.year=2000 and ss.customer.customer_sk is not null) as store_combos count 5;`

  ```text
  --> 3:174
    |
  3 | select count_distinct(concat(ss.customer.customer_sk::string, '-',
  ss.item.item_sk::string) ? ss.date_dim.year=2000 and ss.customer.customer_sk is
  not null) as store_combos count 5;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   is not null) as store_combos ??? count 5;
  ```
- `trilogy file write query98.preql --content import raw.store_sales as store_sales;

# Filter for the date range and categories
where store_sales.date_dim.date…   store_sales.item.category,
    store_sales.item.class,
    store_sales.item.item_id,
    store_sales.item.item_desc,
    pct_of_class
nulls first;`

  ```text
  refused to write 'query98.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...em total extended sales price ??? auto item_total <- sum(store_s...

  Write stats: received 1178 chars / 1178 bytes; tail: …'
  store_sales.item.item_desc,\\n    pct_of_class\\nnulls first;'.
  ```
- `trilogy file write query98.preql --content import raw.store_sales as store_sales;
auto item_total <- sum(store_sales.ext_sales_price) by store_sales.item.ite…category asc,
    store_sales.item.class asc,
    store_sales.item.item_id asc,
    store_sales.item.item_desc asc,
    pct_of_class asc
nulls first;`

  ```text
  refused to write 'query98.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 5, column 61.
  Expected one of:
          * SELECT

  Location:
  ...in ('Sports', 'Books', 'Home') ??? ; select     store_sales.item....

  Write stats: received 865 chars / 865 bytes; tail: …'_sales.item.item_desc
  asc,\\n    pct_of_class asc\\nnulls first;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales.preql:ss --import raw/catalog_sales.preql:cs select concat(ss.customer.customer_sk::string, '-', ss.item.item_sk::string) limit 3;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `concat(ss.customer.customer_sk::string,
  '-', ss.item.item_sk::string) as string_concat`
  Location:
  ...'-', ss.item.item_sk::string) ??? limit 3;
  ```
