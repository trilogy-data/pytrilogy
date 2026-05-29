# Trilogy failure analysis — 20260528-151500

- Run `20260528-151500_enriched` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 610 | failed: 58 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 24 | 41% |
| `other` | 14 | 24% |
| `syntax-missing-alias` | 8 | 14% |
| `undefined-concept` | 5 | 9% |
| `cli-misuse` | 3 | 5% |
| `file-not-found` | 2 | 3% |
| `type-error` | 1 | 2% |
| `join-resolution` | 1 | 2% |

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
  Recursion error building concept local.avg_customer_return
  with grain Grain<store_sales.return_customer.id> and lineage
  avg(sum(ref:store_sales.return_amount)<abstract>)<['ref:store_sales.return_cust
  omer.id']>. This is likely due to a circular reference.
  ```
- `trilogy file write query02.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy file write query02.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy unit query03.preql`

  ```text
  Mocking not implemented for datatype Numeric(15,2)
  ```
- `trilogy run query04.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query04.preql`

  ```text
  Cannot compare Numeric(15,2)
  (divide(ref:local.catalog_2002_total,ref:local.catalog_2001_total)) and UNKNOWN
  (divide(ref:local.web_2002_total,ref:local.web_2001_total)) of different types
  with operator > in
  divide(ref:local.catalog_2002_total,ref:local.catalog_2001_total) >
  divide(ref:local.web_2002_total,ref:local.web_2001_total)
  ```
- `trilogy run query04.preql`

  ```text
  Cannot compare Numeric(15,2)
  (divide(ref:local.catalog_2002_total,ref:local.catalog_2001_total)) and UNKNOWN
  (divide(ref:local.web_2002_total,ref:local.web_2001_total)) of different types
  with operator > in
  divide(ref:local.catalog_2002_total,ref:local.catalog_2001_total) >
  divide(ref:local.web_2002_total,ref:local.web_2001_total)
  ```
- `trilogy run query06.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query11.preql`

  ```text
  Have {'GroupNode<local.st_2001,store_sales.customer.id>':
  None, 'GroupNode<local.wb_2001,web_sales.customer.id>': None,
  'SelectNode<store_sales.customer.first_name,store_sales.customer.id,store_sales
  .customer.last_name...1 more>': None} and need local.st_2001 > 0 and
  local.wb_2001 > 0 and local.web_ratio > local.store_ratio
  ```
- `trilogy run query16.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 3 column 10 (char 183). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 101 (char 100). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales select store_sales.return_customer.id as customer_id, sum(store_sales.return_amount) as total_returns, avg(s…vg_for_store where store_sales.return_store.state='TN' and store_sales.return_date.year=2000 and store_sales.return_customer.id is not null limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...m(store_sales.return_amount)) ??? over store_sales.return_store....
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
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...les_price from both web_sales ??? and catalog_sales, treating ch...

  Write stats: received 3107 chars / 3107 bytes; tail: …'at_2002_shifted, 2) as
  sat_ratio\\norder by week_seq_2001 asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    simple_case(
     …xt_discount_amount ? store_sales.quantity between 1 and 20),
        avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    )
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...tween 1 and 20)     ) limit 1; ???

  Write stats: received 377 chars / 377 bytes; tail: …'paid ?
  store_sales.quantity between 1 and 20)\\n    )\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
import raw.reason as reason;

where reason.id = 1
select
    simple_case(
     …unt ? store_sales.quantity between 1 and 20),
        avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    ) as bucket_1_20
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
       ) as bucket_1_20 limit 1; ???

  Write stats: received 392 chars / 392 bytes; tail: …'les.quantity between 1 and
  20)\\n    ) as bucket_1_20\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select date.year, date.id, count(ticket_number) group by 1,2 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...date.id, count(ticket_number) ??? group by 1,2 limit 5;
  ```
- `trilogy run --import raw/call_center:call_center select distinct call_center.county, call_center.name from call_center;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   call_center; select distinct ??? call_center.county, call_cente...
  ```
- `trilogy run --import raw/catalog_sales.preql:catalog_sales select catalog_sales.ship_date.date, count(catalog_sales.order_number) limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...t(catalog_sales.order_number) ??? limit 20;
  ```
- `trilogy run - --import raw/catalog_sales.preql:catalog_sales`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(catalog_sales.order_number) ??? ; where catalog_sales.sold_dat...
  ```

### `undefined-concept`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.preferred_cust_flag. Suggestions:
  ['store_sales.customer.preferred_cust_flag',
  'store_sales.return_customer.preferred_cust_flag']")
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select date.year, date.id, count(ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  date.year. Suggestions: ['store_sales.date.year']")
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.date.year as sale_year, count(ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.ticket_number. Suggestions: ['store_sales.ticket_number']")
  ```
- `trilogy run --import raw/store_sales:store_sales where store_sales.date.year = 2001 and store_sales.customer.demographics.marital_status = 'M' and store_sale…egree' and store_sales.customer.household_demographic.dependent_count = 3 select sum(quantity) as total_qty, count(store_sales.ticket_number) as cnt;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  local.quantity.')
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  sold_date.year. Suggestions: ['catalog_sales.sold_date.year']")
  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/catalog_sales.preql --show concepts --grep list_price $and --grep coupon_amt --grep quantity --grep sales_price --grep net_profit`

  ```text
  Got unexpected extra argument ($and)
  ```
- `trilogy data --import raw/inventory:inventory select inventory.item.product_name, inventory.item.brand_name, inventory.item.class, inventory.item.category, avg(inventory.quantity_on_hand) as avggrp where inventory.date.year = 2000;`

  ```text
  No such command 'data'.
  ```

### `file-not-found`

- `trilogy run query02.preql`

  ```text
  Input 'query02.preql' does not exist.
  ```
- `trilogy --debug run query15.preql`

  ```text
  Input 'query15.preql' does not exist.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips. Valid:
  'STRING'.
  ```

### `join-resolution`

- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_id<Purpose.KEY>Derivation.BASIC>',
  'local.before_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
