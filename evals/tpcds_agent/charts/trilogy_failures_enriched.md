# Trilogy failure analysis — 20260720-020432

- Run `20260720-020432_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 52 | failed: 6 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 3 | 50% |
| `other` | 2 | 33% |
| `syntax-missing-alias` | 1 | 17% |

## Detail

### `syntax-parse`

- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# First, define the set of qualifying stor…ullif(avg(cs.quantity), 0) as cs_qty_cv
order by ss.item.id asc nulls first, ss.item.desc asc nulls first, ss.store.state asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...r.sk = cs.billing_customer.sk ??? union join ss.item.sk = cs.ite...
  ```
- `trilogy file write answer_3825713089.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;

# Store sales in April 2001
where…it) as catalog_sale_net_profit
having
    store.item.id is not null
order by
    item_code,
    item_desc,
    store_code,
    store_name
limit 100;
`

  ```text
  refused to write 'answer_3825713089.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...= catalog.billing_customer.sk ??? union join store.item.sk = cat...
  ```
- `trilogy file write answer_3825713089.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Store sales in April 2001 that were ret…_net_profit is not null
order by
    store_data.item_code,
    store_data.item_desc,
    store_data.store_code,
    store_data.store_name
limit 100;
`

  ```text
  refused to write 'answer_3825713089.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_data as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...rn date in April-October 2001 ??? rowset store_data as where ss....
  ```

### `other`

- `trilogy run answer_1809796058.preql`

  ```text
  Syntax error in answer_1809796058.preql: 2 undefined concept references; fix all before re-running:
    - return_date.year (line 7, col 7, in WHERE); did you mean: s.return_date.year, return_date.month_of_year, s.sale_date.year, s.return_customer.first_sales_date.year, s.return_customer.first_shipto_date.year, s.customer.first_sales_date.year?
    - return_date.month_of_year (line 8, col 7, in WHERE); did you mean: s.return_date.month_of_year, return_date.year, s.sale_date.month_of_year, s.return_customer.first_sales_date.month_of_year, s.return_customer.first_shipto_date.month_of_year, s.customer.first_sales_date.month_of_year?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 9 column 14 (char 353). Re-issue the call with valid JSON arguments.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_2869182220.preql --content import raw.store_sales as ss;
import raw.customer as c;

# For each store return, find customers whose c…        -- hidden grain field
subset join c.current_demographics.sk = ss.return_customer_demographic.sk
order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden grain field
      -- ss.item.sk as hidden_grain_field_ss_item_sk`
  Location:
   as ticket_number,  -- hidden ??? grain field     -- ss.item.sk
  ```
