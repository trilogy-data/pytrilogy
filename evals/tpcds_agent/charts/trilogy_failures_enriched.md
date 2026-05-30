# Trilogy failure analysis — 20260530-151525

- Run `20260530-151525` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 805 | failed: 73 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 24 | 33% |
| `syntax-parse` | 23 | 32% |
| `undefined-concept` | 11 | 15% |
| `cli-misuse` | 8 | 11% |
| `syntax-missing-alias` | 4 | 5% |
| `type-error` | 2 | 3% |
| `join-resolution` | 1 | 1% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.date:d select d.year, d.week_seq where d.year in (2000,2001,2002) and d.day_of_week=0 order by year, week_seq limit 10;`

  ```text
  ORDER BY references 'local.year', which is not in the SELECT
  projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.year order by local.year
  asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 34: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_billing_customer_address_customer_address"."CA_ZIP" as
  "store_sales_billing_customer_address_zip",
      count("store_sales_billing_customer_customers"."C_CUSTOMER_SK") as
  "
  …
  tore_sales_store_store"."S_ZIP" in (select
  quizzical."_virt_func_split_4785012549328100" from quizzical where
  quizzical."_virt_func_split_4785012549328100" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "concerned"."store_sales_store_name" as "store_sales_store_name",
      sum("concerned"."store_sales_net_profit") as "total_net_profit"
  FROM
      "concerned"
  GROUP BY
      1
  ORDER BY
      "concerned"."store_sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 34: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_billing_customer_address_customer_address"."CA_ZIP" as
  "store_sales_billing_customer_address_zip",
      count("store_sales_billing_customer_customers"."C_CUSTOMER_SK") as
  "
  …
  tore_sales_store_store"."S_ZIP" in (select
  quizzical."_virt_func_split_4785012549328100" from quizzical where
  quizzical."_virt_func_split_4785012549328100" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "concerned"."store_sales_store_name" as "store_sales_store_name",
      sum("concerned"."store_sales_net_profit") as "total_net_profit"
  FROM
      "concerned"
  GROUP BY
      1
  ORDER BY
      "concerned"."store_sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 36: ...prefix" is not null) and "store_sales_store_store"."S_ZIP" in
  (select quizzical."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_billing_customer_address_customer_address"."CA_ZIP" as
  "store_sales_billing_customer_address_zip",
      count("store_sales_billing_customer_customers"."C_CUSTOMER_SK") as
  "preferred_by_zip"
  FROM

  …
  tore_sales_store_store"."S_ZIP" in (select
  quizzical."_virt_func_split_4785012549328100" from quizzical where
  quizzical."_virt_func_split_4785012549328100" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "concerned"."store_sales_store_name" as "store_sales_store_name",
      sum("concerned"."store_sales_net_profit") as "total_net_profit"
  FROM
      "concerned"
  GROUP BY
      1
  ORDER BY
      "concerned"."store_sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql`

  ```text
  This script requires parameter "zips" to be set in
  environment.
  ```
- `trilogy run --import raw.physical_sales:store_sales --param zips=24128,76232,65084 select store_sales.store.name, store_sales.store.zip, store_sales.date.yea…e.year = 1998 and store_sales.date.quarter = 2 and store_sales.store.zip in split('24128,76232,65084', ',') order by store_sales.store.name limit 10;`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 18: ...te_date"."D_QOY" = 2 and "store_sales_store_store"."S_ZIP" in
  (select quizzical."_virt_func_split_3106282253538863...
                                                                         ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      STRING_SPLIT( '24128,76232,65084' , ',' ) as
  "_virt_func_split_3106282253538863"
  )
  SELECT
      "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
      "store_sales_store_store"."S_ZIP" as "sto
  …
  es_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on
  "store_sales_store_sales"."SS_STORE_SK" =
  "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 1998 and "store_sales_date_date"."D_QOY"
  = 2 and "store_sales_store_store"."S_ZIP" in (select
  quizzical."_virt_func_split_3106282253538863" from quizzical where
  quizzical."_virt_func_split_3106282253538863" is not null)

  GROUP BY
      1,
      2,
      3,
      4
  ORDER BY
      "store_sales_store_store"."S_STORE_NAME" asc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.physical_sales:store_sales --param zips=24128,76232,65084 select store_sales.store.name, store_sales.store.zip, store_sales.date.yea… = 1998 and store_sales.date.quarter = 2 and substring(store_sales.store.zip, 1, 2) in split('24128', ',' ) order by store_sales.store.name limit 10;`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 18: ..." = 2 and SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in
  (select quizzical."_virt_func_split_7516790893574274...
                                                                         ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      STRING_SPLIT( '24128' , ',' ) as "_virt_func_split_7516790893574274"
  )
  SELECT
      "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
      "store_sales_store_store"."S_ZIP" as "store_sales_stor
  …
  D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on
  "store_sales_store_sales"."SS_STORE_SK" =
  "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 1998 and "store_sales_date_date"."D_QOY"
  = 2 and SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in (select
  quizzical."_virt_func_split_7516790893574274" from quizzical where
  quizzical."_virt_func_split_7516790893574274" is not null)

  GROUP BY
      1,
      2,
      3,
      4
  ORDER BY
      "store_sales_store_store"."S_STORE_NAME" asc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 216 (char 215). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq as ws, all_sales.date.day_of_week as dow, sum(all_sales.ext_sales_price) as daily…les_channel in ('WEB','CATALOG') and all_sales.date.year=2001 group by all_sales.date.week_seq, all_sales.date.day_of_week order by ws, dow limit 20;`

  ```text
  --> 2:200
    |
  2 | select all_sales.date.week_seq as ws, all_sales.date.day_of_week as dow,
  sum(all_sales.ext_sales_price) as daily_sales where all_sales.sales_channel in
  ('WEB','CATALOG') and all_sales.date.year=2001 group by
  all_sales.date.week_seq, all_sales.date.day_of_week order by ws, dow limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   and all_sales.date.year=2001 ??? group by all_sales.date.week_s...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year as yr, min(all_sales.date.week_seq) as min_ws, max(all_sales.date.week_seq) as max_ws, count(all_sales.date.week_seq) as num_weeks where all_sales.date.year in (2001,2002) group_by all_sales.date.year order by yr;`

  ```text
  --> 2:200
    |
  2 | select all_sales.date.year as yr, min(all_sales.date.week_seq) as min_ws,
  max(all_sales.date.week_seq) as max_ws, count(all_sales.date.week_seq) as
  num_weeks where all_sales.date.year in (2001,2002) group_by all_sales.date.year
  order by yr;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ales.date.year in (2001,2002) ??? group_by all_sales.date.year o...
  ```
- `trilogy file write query06.preql --content import raw.physical_sales as store_sales;

# Filter to store sales in January 2001
where store_sales.date.year = 2…ling_customer.id) as customer_count
having
    customer_count >= 10
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 12, column 1.
  Expected one of:
          * SELECT

  Location:
   states with >= 10 customers  ??? auto avg_price_by_cat <- avg(i...

  Write stats: received 765 chars / 765 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql -e -c import raw.physical_sales as store_sales;

# Filter to store sales in January 2001
where store_sales.date.year = 2001
…ling_customer.id) as customer_count
having
    customer_count >= 10
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...verage current price of items ??? auto avg_price_by_cat <- avg(i...

  Write stats: received 586 chars / 586 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;
import raw.customer as customer;

# declare the parameter
parameter zips…_prefixes
    )
select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 9, column 78.
  Expected one of:
          * _TERMINATOR

  Location:
  ...mer.preferred_cust_flag = 'Y' ??? by customer.address.zip; auto

  Write stats: received 1447 chars / 1447 bytes; tail: …'al_net_profit\\norder
  by\\n    store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Preferred customers with their address ZIPs
#…s, 1, 2)
    )

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ore_sales.store.zip, 1, 2) in ??? (         select substring(zip...

  Write stats: received 1174 chars / 1174 bytes; tail: …'al_net_profit\\norder
  by\\n    store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Preferred customers with their address ZIPs
#…d, 1, 2)
    )

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   and substring(zips, 1, 2) in ??? (         select substring(zip...

  Write stats: received 950 chars / 950 bytes; tail: …'al_net_profit\\norder
  by\\n    store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --param zips=24128,76232,65084 select store_sales.store.name, store_sales.store.zip, store_sales.date.year, store_sales.date.quarter, sum(store_s…es.date.year = 1998 and store_sales.date.quarter = 2 group by store_sales.store.name, store_sales.store.zip order by store_sales.store.name limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...t_profit) as total_net_profit ??? from raw.physical_sales as sto...
  ```
- `trilogy run --import raw.customer:customer select customer.address.zip, count(customer.id ? customer.preferred_cust_flag = 'Y') as pref_cnt by customer.address.zip order by pref_cnt desc limit 10;`

  ```text
  --> 2:98
    |
  2 | select customer.address.zip, count(customer.id ?
  customer.preferred_cust_flag = 'Y') as pref_cnt by customer.address.zip order
  by pref_cnt desc limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ..._cust_flag = 'Y') as pref_cnt ??? by customer.address.zip order
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty, avg(store_sales.ext_discount_amount) as avg_disc, avg(store_sales.net_paid) as avg_paid group by store_sales.ticket_number limit 10;`

  ```text
  --> 2:164
    |
  2 | select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty,
  avg(store_sales.ext_discount_amount) as avg_disc, avg(store_sales.net_paid) as
  avg_paid group by store_sales.ticket_number limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.net_paid) as avg_paid ??? group by store_sales.ticket_nu...
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty, avg(store_sales.ext_discount_amount) as avg_disc, avg(store_sales.net_paid) as avg_paid by store_sales.ticket_number limit 10;`

  ```text
  --> 2:164
    |
  2 | select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty,
  avg(store_sales.ext_discount_amount) as avg_disc, avg(store_sales.net_paid) as
  avg_paid by store_sales.ticket_number limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.net_paid) as avg_paid ??? by store_sales.ticket_number l...
  ```
- `trilogy run --import raw.physical_sales:store_sales select sum(store_sales.quantity) as ticket_qty by store_sales.ticket_number limit 5;`

  ```text
  --> 2:48
    |
  2 | select sum(store_sales.quantity) as ticket_qty by store_sales.ticket_number
  limit 5;
    |                                                ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...sales.quantity) as ticket_qty ??? by store_sales.ticket_number l...
  ```
- `trilogy run --import raw.physical_sales:store_sales select sum(store_sales.quantity) as ticket_qty by store_sales.ticket_number limit 5;`

  ```text
  --> 2:48
    |
  2 | select sum(store_sales.quantity) as ticket_qty by store_sales.ticket_number
  limit 5;
    |                                                ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...sales.quantity) as ticket_qty ??? by store_sales.ticket_number l...
  ```
- `trilogy run --import raw.physical_sales:store_sales select min(ticket_qty) as min_qty, max(ticket_qty) as max_qty from (select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty limit 50000);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...y, max(ticket_qty) as max_qty ??? from (select store_sales.ticke...
  ```
- `trilogy run --import raw.physical_sales:store_sales select min(store_sales.ticket_number) as min_tkt, max(store_sales.ticket_number) as max_tkt, min(ticket_qty) as min_tkt_qty, max(ticket_qty) as max_tkt_qty from (select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty limit 24000);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ax(ticket_qty) as max_tkt_qty ??? from (select store_sales.ticke...
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.ticket_number ? ticket_qty between 1 and 20) as cnt from (select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty limit 24000);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ..._qty between 1 and 20) as cnt ??? from (select store_sales.ticke...
  ```
- `trilogy run --import raw.physical_sales:store_sales select min(ticket_qty) as min_qty, max(ticket_qty) as max_qty from (select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty limit 24000);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...y, max(ticket_qty) as max_qty ??? from (select store_sales.ticke...
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.ticket_number ? ticket_qty between 1 and 20) as cnt, count(store_sales.ticket_number ? ticket_qty between 21 and 40) as cnt2 from (select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty limit 24000);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ty between 21 and 40) as cnt2 ??? from (select store_sales.ticke...
  ```
- `trilogy file write query11.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = ext_list_price - ext_dis…
    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 125.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ore_sales.billing_customer.id ??? where store_sales.date.year =

  Write stats: received 1779 chars / 1779 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = ext_list_price - ext_dis…
    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 35.
  Expected one of:
          * SELECT

  Location:
  ...e store_sales.date.year = 2001 ??? ; auto store_rev_2002 <- sum(s...

  Write stats: received 1692 chars / 1692 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = ext_list_price - ext_dis…
    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 125.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ore_sales.billing_customer.id ??? where store_sales.date.year =

  Write stats: received 1720 chars / 1720 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as cs;


where cs.sold_date.year = 2001
  and cs.sold_date.quarter = 2
  and (
       sub…)
select
    cs.bill_address.zip as zip_code,
    sum(cs.sales_price) as total_sales_price
group by zip_code
order by zip_code nulls first
limit 100;`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 14, column 1.
  Expected one of:
          * METADATA
          * MERGE
          * LIMIT
          * COMMA
          * ORDER
          * WHERE
          * HAVING
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'total_sales_price')]

  Location:
  ...s_price) as total_sales_price ??? group by zip_code order by zip...

  Write stats: received 463 chars / 463 bytes; tail: …'e\\ngroup by
  zip_code\\norder by zip_code nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

# Filters
where catalog_sales.sold_date.year = 1998
  and catalog_sale…    catalog_sales.item.category asc nulls first,
    g_country asc nulls first,
    g_state asc nulls first,
    g_county asc nulls first
limit 100;
`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...'VA')  # Define rollup helper ??? def by_geo(metric) -> avg(metr...

  Write stats: received 2233 chars / 2233 bytes; tail: …'te asc nulls first,\\n
  g_county asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run --import raw.all_sales select all_sales.sales_channel as chan limit 10;`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  all_sales.sales_channel. Suggestions: ['sales_channel']")
  ```
- `trilogy run --import raw.all_sales:all_sales select sales_channel as chan limit 10;`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  sales_channel. Suggestions: ['all_sales.sales_channel',
  'all_sales.channel_dim_id']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog.ext_discount_amount. Suggestions: ['catalog.discount_amount',
  'store.ext_discount_amount', 'web.ext_discount_amount']")
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.ticket_number ? ticket_qty between 1 and 20) as cnt;`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  ticket_qty.')
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.address.county. Suggestions:
  ['web_sales.ship_customer.address.county',
  'web_sales.ship_customer.address.country',
  'catalog_sales.customer_address.county']")
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand_id. Suggestions: ['store_sales.item.brand_id']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date.year. Suggestions: ['store_sales.date.year']")
  ```

### `cli-misuse`

- `trilogy explore raw/sales_channel.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales_channel.preql' does not exist.
  ```
- `trilogy explore raw/raw/physical_sales.preql --regex return_amount|return_net_loss|net_profit`

  ```text
  Invalid value for 'PATH': File 'raw/raw/physical_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run -i raw.physical_sales:store_sales select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty, avg(store_sales.ext_discount_amount) as avg_disc, avg(store_sales.net_paid) as avg_paid group by store_sales.ticket_number limit 10;`

  ```text
  'raw.physical_sales:store_sales' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel;
  ```
- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel as sc;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel as sc;
  ```
- `trilogy run --import raw.all_sales:s select distinct s.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...l_sales as s; select distinct ??? s.sales_channel as chan;
  ```
- `trilogy run --import raw.all_sales select distinct all_sales.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...aw.all_sales; select distinct ??? all_sales.sales_channel as cha...
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type ArrayType<STRING>' passed into
  SUBSTRING function from function SPLIT in position 1. Valid: 'STRING'
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_list. Valid:
  'STRING'.
  ```

### `join-resolution`

- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  catalog_sales.warehouse_id@Grain<catalog_sales.warehouse_id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```
