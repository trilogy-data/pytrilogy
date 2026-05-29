# Trilogy failure analysis — 20260531-020357

- Run `20260531-020356_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 376 | failed: 34 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 12 | 35% |
| `syntax-parse` | 12 | 35% |
| `undefined-concept` | 5 | 15% |
| `syntax-missing-alias` | 2 | 6% |
| `join-resolution` | 1 | 3% |
| `type-error` | 1 | 3% |
| `cli-misuse` | 1 | 3% |

## Detail

### `syntax-parse`

- `trilogy run --import raw/store_sales:store_sales select store_sales.return_store.state, store_sales.return_date.year, store_sales.return_customer.id, sum(sto…re_sales.return_store.state='TN' and store_sales.return_date.year=2000 group by store_sales.return_customer.id, store_sales.return_store.id limit 10;`

  ```text
  --> 2:230
    |
  2 | select store_sales.return_store.state, store_sales.return_date.year,
  store_sales.return_customer.id, sum(store_sales.return_amount) as total_returns
  where store_sales.return_store.state='TN' and store_sales.return_date.year=2000
  group by store_sales.return_customer.id, store_sales.return_store.id limit 10;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...e_sales.return_date.year=2000 ??? group by store_sales.return_cu...
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.date as date_dim;

# Merge date concepts ac…0) + coalesce(cs_by_wk, 0) as total;

select
    ws.date.week_seq as week_seq,
    ws.date.year,
    ws.date.day_of_week as dow,
    sum_wk
limit 20;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 18, column 62.
  Expected one of:
          * _TERMINATOR

  Location:
  ...k, 0) + coalesce(cs_by_wk, 0) ??? as total;  select     ws.date....

  Write stats: received 923 chars / 923 bytes; tail: …'e.year,\\n
  ws.date.day_of_week as dow,\\n    sum_wk\\nlimit 20;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.date as date_dim;

# Merge date concepts ac…und(sat_total / (select sat_total where date_dim.week_seq = week_seq_2001 + 53 and date_dim.year = 2002), 2) as sat_ratio
order by week_seq_2001 asc;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...q_2001,     round(sun_total / ??? (select sun_total where date_d...

  Write stats: received 2786 chars / 2786 bytes; tail: …'im.year = 2002), 2) as
  sat_ratio\\norder by week_seq_2001 asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.date as date_dim;

# Merge date concepts ac…thu_ratio,
    round(fri_2001 / fri_2002_shifted, 2) as fri_ratio,
    round(sat_2001 / sat_2002_shifted, 2) as sat_ratio
order by week_seq_2001 asc;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 44, column 35.
  Expected one of:
          * _TERMINATOR

  Location:
   sun_2002_shifted <- sun_2002 ??? by (date_dim.week_seq - 53); a...

  Write stats: received 3335 chars / 3335 bytes; tail: …'at_2002_shifted, 2) as
  sat_ratio\\norder by week_seq_2001 asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c import raw.store_sales as store_sales;

parameter zips string;

# Step 1: Find ZIP codes from the parameter list whose…ng_prefixes

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 113.
  Expected one of:
          * _TERMINATOR

  Location:
   'Y') by customer.address.zip ??? as pref_count;  # ZIPs with mo...

  Write stats: received 1327 chars / 1327 bytes; tail: …'fit) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.customer.address.zip, count(store_sales.customer.id ? store_sales.customer.preferred_cust_flag = 'Y') as pref_count group by store_sales.customer.address.zip having pref_count > 10 limit 5;`

  ```text
  --> 2:136
    |
  2 | select store_sales.customer.address.zip, count(store_sales.customer.id ?
  store_sales.customer.preferred_cust_flag = 'Y') as pref_count group by
  store_sales.customer.address.zip having pref_count > 10 limit 5;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...ust_flag = 'Y') as pref_count ??? group by store_sales.customer....
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# The question says: for each ticket quantity bucket (1-20, 21-40, 41-60, … as bucket_21_40,
    bucket_41_60 as bucket_41_60,
    bucket_61_80 as bucket_61_80,
    bucket_81_100 as bucket_81_100
where reason.id = 1
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 32, column 2.
  Expected one of:
          * WHEN

  Location:
  ...s.quantity between 1 and 20) ) ??? ; auto bucket_21_40 <- case(

  Write stats: received 2873 chars / 2873 bytes; tail: …' bucket_81_100 as
  bucket_81_100\\nwhere reason.id = 1\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

auto cnt_01_20 <- count(store_sales.ticket_number… as bucket_21_40,
    bucket_41_60 as bucket_41_60,
    bucket_61_80 as bucket_61_80,
    bucket_81_100 as bucket_81_100
where reason.id = 1
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'cnt_01_20 ') at line 11, column 5.
  Expected one of:
          * TRUE
          * RPAR
          * FALSE
          * DBLQUOTE
          * LSQB
          * NULL
          * QUOTE
          * LBRACE
          * _STRUCT
          * MULTILINE_STRING
          * INT_LITERAL_PART
          * /\-?[0-9]*\.[0-9]+/
          * LPAR
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...bucket_1_20 <- case(true,     ??? cnt_01_20 > 74129,     avg(sto...

  Write stats: received 1804 chars / 1804 bytes; tail: …' bucket_81_100 as
  bucket_81_100\\nwhere reason.id = 1\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    case(
        coun…store_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20,     case(

  Write stats: received 1535 chars / 1535 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    case(
        coun…store_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20,     case(

  Write stats: received 1535 chars / 1535 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    case(
        coun…iscount_amount ? store_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    )
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 10, column 6.
  Expected one of:
          * WHEN

  Location:
  ...antity between 1 and 20)     ) ??? ,     case(         count(stor...

  Write stats: received 1455 chars / 1455 bytes; tail: …'id ?
  store_sales.quantity between 81 and 100)\\n    )\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    case(
        coun…unt ? store_sales.quantity between 1 and 20),
        avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    ) as bucket_1_20
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20 limit 1;

  Write stats: received 385 chars / 385 bytes; tail: …'les.quantity between 1 and
  20)\\n    ) as bucket_1_20\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    case(
        coun…xt_discount_amount ? store_sales.quantity between 1 and 20),
        avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    )
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LIMIT', 'limit') at line 11, column 1.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? limit 1;

  Write stats: received 370 chars / 370 bytes; tail: …'paid ?
  store_sales.quantity between 1 and 20)\\n    )\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge web_sales.customer.id into ~store…ag as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 21, column 1.
  Expected one of:
          * SELECT

  Location:
  ...already have st_2001>0 above) ??? auto store_ratio <- st_2002 /

  Write stats: received 1755 chars / 1755 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge web_sales.customer.id into ~store…ag as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 20, column 1.
  Expected one of:
          * SELECT

  Location:
    # Store ratio and web ratio ??? auto store_ratio <- st_2002 /

  Write stats: received 1693 chars / 1693 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge web_sales.customer.id into ~store…ag as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 20, column 1.
  Expected one of:
          * SELECT

  Location:
  ...omputation, compute in SELECT ??? auto store_ratio <- st_2002 /

  Write stats: received 1625 chars / 1625 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge web_sales.customer.id into ~store…ag as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 10, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'store_ratio')]

  Location:
   and web_ratio > store_ratio  ??? auto st_2001 <- sum(store_sale...

  Write stats: received 1269 chars / 1269 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge web_sales.customer.id into ~store…ag as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'store_ratio')]

  Location:
   and web_ratio > store_ratio  ??? auto st_2001 <- sum(store_sale...

  Write stats: received 1271 chars / 1271 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.customer.demographics.education_status, store_sales.customer.demographics.marital_status, count(store_sales.ticket_number) as cnt group by 1,2;`

  ```text
  --> 2:150
    |
  2 | select store_sales.customer.demographics.education_status,
  store_sales.customer.demographics.marital_status,
  count(store_sales.ticket_number) as cnt group by 1,2;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.ticket_number) as cnt ??? group by 1,2;
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.customer.demographics.education_status, store_sales.customer.demographics.marital_status, store_sales.customer.household_demographic.dependent_count, count(store_sales.ticket_number) as cnt group by 1,2,3;`

  ```text
  --> 2:210
    |
  2 | select store_sales.customer.demographics.education_status,
  store_sales.customer.demographics.marital_status,
  store_sales.customer.household_demographic.dependent_count,
  count(store_sales.ticket_number) as cnt group by 1,2,3;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.ticket_number) as cnt ??? group by 1,2,3;
  ```
- `trilogy run --import raw/store_sales:store_sales where store_sales.date.year in (2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010) select store_sales.date.year as yr, count(store_sales.ticket_number) as cnt group_by store_sales.date.year order by yr;`

  ```text
  --> 2:169
    |
  2 | where store_sales.date.year in (2001, 2002, 2003, 2004, 2005, 2006, 2007,
  2008, 2009, 2010) select store_sales.date.year as yr,
  count(store_sales.ticket_number) as cnt group_by store_sales.date.year order by
  yr;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.ticket_number) as cnt ??? group_by store_sales.date.year...
  ```
- `trilogy run - --import raw/catalog_sales.preql:catalog_sales`

  ```text
  --> 3:42
    |
  3 | where catalog_sales.sold_date.year = 1998;
    |                                          ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...og_sales.sold_date.year = 1998 ??? ;
  ```
- `trilogy run --import raw/inventory:inventory select inventory.item.product_name, inventory.item.brand_name, inventory.item.class, inventory.item.category, av…elect inventory.item.category, null::string, null::string, null::string, avg(inventory.quantity_on_hand) as avg_qty where inventory.date.year = 2000;`

  ```text
  --> 3:1
    |
  3 | union
    | ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...e inventory.date.year = 2000  ??? union select inventory.item.br...
  ```
- `trilogy file write query22.preql --content import raw.inventory as inventory;

# Level 1: product_name + brand_name + class + category (full grain)
auto prod…: class + category
no class_cat_avg <- avg(inventory.quantity_on_hand ? inventory.date.year = 2000) by inventory.item.class, inventory.item.category;`

  ```text
  …
         * COPY
          * PROPERTY
          * PARAMETER
          * ROWSET
          * IMPORT
          * SHOW
          * MOCK
          * SELF_IMPORT
          * RAW_SQL
          * _DEF_TABLE
          * SHORTHAND_MODIFIER
          * WITH
          * UNIQUE
          * PARAM
          * TYPE
          * _PROPERTIES
          * DATASOURCE
          * PURPOSE
          * AUTO
          * PERSIST_MODE
          * CHART
          * "merge"i
          * FROM
          * DATASOURCE_PARTIAL
          * PUBLISH_ACTION
          * SELECT
          * DEF
          * VALIDATE
  Previous tokens: [Token('PARSE_COMMENT', '# Level 3: class + category\n')]

  Location:
    # Level 3: class + category ??? no class_cat_avg <- avg(invent...

  Write stats: received 699 chars / 699 bytes; tail: …'ar = 2000) by
  inventory.item.class, inventory.item.category;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run --import raw/store_sales:store_sales select store_sales.return_store.id as store_id, avg(sum(store_sales.return_amount)) by store_sales.return_cu…stomer_return where store_sales.return_store.state='TN' and store_sales.return_date.year=2000 and store_sales.return_customer.id is not null limit 5;`

  ```text
  HAVING references 'local.cust_store_return',
  'local.store_avg_return', which are not in the SELECT projection (line 9). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.cust_store_return,
  --local.store_avg_return
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0 and all_sales.sal…') and all_sales.date.year = 2002) by (all_sales.date.week_seq - 53) as sun_2002 where sun_2002 is not null order by all_sales.date.week_seq limit 5;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.sun_2002) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run --import raw.all_sales:all_sales auto sun_2001 <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0 and all_sales.sales_channel in (…ll_sales.date.week_seq) as wk_seq, sun_2001, (all_sales.date.week_seq - 53) as wk2002, sun_2002 having sun_2001 is not null order by wk_seq limit 10;`

  ```text
  Parenthetical with non-supported content
  ref:all_sales.date.week_seq (<class 'trilogy.core.models.author.ConceptRef'>)
  not yet supported
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', 'local.catalog_2001',
  'local.store_2002', 'local.store_2001', 'local.web_2002', 'local.web_2001',
  which are not in the SELECT projection (line 19). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.catalog_2002, --local.catalog_2001,
  --local.store_2002, --local.store_2001, --local.web_2002, --local.web_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_8851570471982349 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local.has_store_sale_2002_q1 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local.has_store_sale_2002_q1 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query04.preql`

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 31 column 12 (char 1091). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.num_channels',
  'local.overall_avg_sale', which are not in the SELECT projection (line 20). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.num_channels,
  --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query15.preql`

  ```text
  Value 'Q2' is not valid for enum field
  'catalog_sales.sold_date.quarter'. Allowed values: 1, 2, 3, 4.
  ```
- `trilogy run query18.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, min(all_sales.date.week_seq) as min_ws, max(all_sales.date.week_seq) as max_ws where all_sales.sales_channel in ('WEB','CATALOG') group by all_sales.date.year order by all_sales.date.year limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._channel in ('WEB','CATALOG') ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.date.week_seq) as num_weeks where all_sales.sales_channel in ('WEB','CATALOG') group by all_sales.date.year order by all_sales.date.year limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._channel in ('WEB','CATALOG') ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.all_sales:all_sales auto sales_2001 <- sum(all_sales.ext_sales_price ? all_sales.sales_channel in ('WEB','CATALOG') and all_sales.da…_2002_shifted having sales_2001 is not null and sales_2002_shifted is not null order by all_sales.date.week_seq, all_sales.date.day_of_week limit 15;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...d all_sales.date.year = 2002) ??? by (all_sales.date.week_seq -
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto outlet <- case
    when all_sales.sales_channel = 'STORE'
        then co…ice, all_sales.return_amount, sum(all_sales.net_profit) - sum(all_sales.return_net_loss))
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 14, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? def rollup_metrics(metric_sale...

  Write stats: received 1032 chars / 1032 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_OR', 'or') at line 7, column 68.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...ddress.county = 'Rush County' ??? or customer.address.county = '...

  Write stats: received 2238 chars / 2238 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 12, column 30.
  Expected one of:
          * WHEN

  Location:
                              0) ??? ) by customer.id;  # At least

  Write stats: received 2022 chars / 2022 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'customer.address.county ') at line
  7, column 33.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...to in_county <- max(case(when ??? customer.address.county = 'Rus...

  Write stats: received 2088 chars / 2088 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 7, column 253.
  Expected one of:
          * WHEN

  Location:
  ...ss.county = 'La Porte County') ??? ) by customer.id;  # At least

  Write stats: received 2265 chars / 2265 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Step 1: Find which (brand, class, category) combos appear in all 3 channels ….brand_id nulls first,
    g_class asc,
    all_sales.item.class_id nulls first,
    g_cat asc,
    all_sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [203]: Missing assignment operator '<-' and expression in derivation.
  Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`).
  Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.
  Location:
  ...all 3 channels auto valid_bcc ??? = all_sales.item.brand_id, all...

  Write stats: received 3494 chars / 3494 bytes; tail: …'t asc,\\n
  all_sales.item.category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value (qty * list_price) across all 3 channels in 1999-…nulls first,
    all_sales.item.brand_id nulls first,
    all_sales.item.class_id nulls first,
    all_sales.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 39, column 44.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ems <- all_sales.item.brand_id ??? , all_sales.item.class_id, all...

  Write stats: received 4386 chars / 4386 bytes; tail: …'irst,\\n
  all_sales.item.category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Sale value = quantity * list price
auto sale_value <- all_sales.quantity * a…nd_id nulls first,
    --g_class asc,
    all_sales.item.class_id nulls first,
    --g_cat asc,
    all_sales.item.category_id nulls first
limit 100;`

  ```text
  …
          * _MAP_VALUES
          * _MONTH_NAME
          * _VARIANCE
          * _GEO_X
          * CURRENT_DATETIME
          * _ILIKE
          * FILTER
          * _GEO_TRANSFORM
          * _UPPER
          * /add\(/
          * _REGEXP_EXTRACT
          * "@"
          * _SUBSTRING
          * _SUBSELECT
          * _GEO_CENTROID
          * _REGEXP_REPLACE
          * _CEIL
          * _STDDEV
          * _DATE_DIFF
          * LBRACE
          * SUBTRACT
          * _DATE_SPINE
          * _PARSE_TIME
          * _STRUCT
          * _DAY_OF_WEEK
          * _STRPOS
          * _ARRAY_TRANSFORM
  Previous tokens: [Token('__ANON_7', 'by')]

  Location:
  ...overall_avg_sale order by     ??? --g_channel asc,     all_sales...

  Write stats: received 2109 chars / 2109 bytes; tail: …'t asc,\\n
  all_sales.item.category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query18.preql --content import catalog_sales as catalog_sales;

# Filter conditions
where catalog_sales.sold_date.year = 1998
  and catalo…ress.state asc nulls first,
    catalog_sales.bill_customer.address.county asc nulls first,
    catalog_sales.item.text_id asc nulls first
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...code), country, state, county ??? def avg_rollup(metric) -> avg(...

  Write stats: received 1659 chars / 1659 bytes; tail: …'t,\\n
  catalog_sales.item.text_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.preferred_cust_flag. Suggestions:
  ['store_sales.billing_customer.preferred_cust_flag',
  'store_sales.return_customer.preferred_cust_flag',
  'store_sales.billing_customer.first_name']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ss.sale_date.quarter_name. Suggestions: ['ss.date.quarter_name',
  'cs.sold_date.quarter_name', 'ss.store.date.quarter_name']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.bill_demographics.gender. Suggestions:
  ['catalog_sales.bill_customer.demographics.gender',
  'catalog_sales.bill_customer_demographic.gender',
  'catalog_sales.billing_customer.demographics.gender']")
  ```
- `trilogy run query20.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

s…         then concat('web_site_', all_sales.channel_dim_text_id)
    end) as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `(case
          when all_sales.sales_channel = 'STORE'
              then concat('store_', all_sales.channel_dim_text_id)
          when all_sales.sales_channel = 'CATALOG'
              then concat('catalog_page_', all_sales.channel_dim_text_id)
          when all_sales.sales_channel = 'WEB'
              then concat('web_site_', all_sales.channel_dim_text_id)
      end) as case_when_all_sales_sales_channel_store_`
  Location:
  ...ollup all_sales.sales_channel, ??? (case         when all_sales....

  Write stats: received 1962 chars / 1962 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write test_channels.preql --content import raw.all_sales as all_sales;

where all_sales.date.year = 2001 and all_sales.date.month_of_year = 11
select distinct all_sales.sales_channel
limit 10;`

  ```text
  refused to write 'test_channels.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct all_sales.sales_channel as
  distinct_all_sales_sales_channel`
  Location:
  ...tinct all_sales.sales_channel ??? limit 10;

  Write stats: received 156 chars / 156 bytes; tail: …'_year = 11\\nselect
  distinct all_sales.sales_channel\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `join-resolution`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, min(all_sales.date.d_week_seq1) as min_dws, max(all_sales.date.d_week_seq1) as max_d…week_seq) as max_ws where all_sales.sales_channel in ('WEB','CATALOG') and all_sales.date.year in (2001, 2002) order by all_sales.date.year limit 10;`

  ```text
  No datasource exists for root concept
  all_sales.date.d_week_seq1@Grain<all_sales.date.id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.intersecting_zips. Valid:
  'STRING'.
  ```

### `cli-misuse`

- `trilogy explore raw/physical_sales.preql --show imports,datasources`

  ```text
  Invalid value for '--show': 'imports,datasources' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
