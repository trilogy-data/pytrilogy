# Trilogy failure analysis — 20260528-021826

- Run `20260528-021826` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 564 | failed: 48 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 11 | 23% |
| `undefined-concept` | 11 | 23% |
| `cli-misuse` | 11 | 23% |
| `other` | 9 | 19% |
| `file-not-found` | 3 | 6% |
| `syntax-missing-alias` | 2 | 4% |
| `join-resolution` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql -e -c import raw.store_returns as sr;
import raw.store as store;
import raw.customer as customer;

merge sr.store.store_sk i… > 1.2 * avg(cust_store_total) by sr.store.store_sk
select
    sr.customer.customer_sk as customer_sk
order by
    sr.customer.customer_sk
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store.store_sk\n  ') at line 13,
  column 10.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ....store.store_sk in (   select ??? store.store_sk   where store.s...

  Write stats: received 670 chars / 670 bytes; tail: …' customer_sk\\norder by\\n
  sr.customer.customer_sk\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -c import raw.store_returns as sr;\nimport raw.store as store;\nimport raw.customer as customer;\n\nmerge sr.store.store_sk …k\n)\n  and cust_store_total > 1.2 * store_avg\nselect\n    sr.customer.customer_sk as customer_sk\norder by\n    sr.customer.customer_sk\nlimit 100;`

  ```text
  …

          * SELECT
          * MOCK
          * $END
          * RAW_SQL
          * PROPERTY
          * ROWSET
          * CREATE
          * SELF_IMPORT
          * CHART
          * PUBLISH_ACTION
          * "merge"i
          * PARAMETER
          * LINE_SEPARATOR
          * _DEF_TABLE
          * SHORTHAND_MODIFIER
          * _PROPERTIES
          * SHOW
          * PERSIST_MODE
          * TYPE
          * PARAM
          * PARSE_COMMENT
          * IMPORT
          * VALIDATE
          * COPY
          * AUTO
          * DATASOURCE_ROOT
          * DATASOURCE
          * DATASOURCE_PARTIAL
  Previous tokens: [Token('_TERMINATOR', ';')]

  Location:
  ...mport raw.store_returns as sr; ??? \nimport raw.store as store;\n...

  Write stats: received 785 chars / 785 bytes; tail: …'stomer_sk\\norder by\\n
  sr.customer.customer_sk\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as sr;
import raw.store as store;
import raw.customer as customer;

merge sr.store.store_…store_sk
)
  and cust_store_total > 1.2 * store_avg
select
    sr.customer.customer_sk as customer_sk
order by
    sr.customer.customer_sk
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store.store_sk\n  ') at line 15,
  column 10.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ....store.store_sk in (   select ??? store.store_sk   where store.s...

  Write stats: received 762 chars / 762 bytes; tail: …' customer_sk\\norder by\\n
  sr.customer.customer_sk\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as sr;
import raw.store as store;
import raw.customer as customer;

merge sr.store.store_…te = 'TN')
  and cust_store_total > 1.2 * store_avg
select
    sr.customer.customer_sk as customer_sk
order by
    sr.customer.customer_sk
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store.store_sk ') at line 14,
  column 36.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
   sr.store.store_sk in (select ??? store.store_sk where store.sta...

  Write stats: received 732 chars / 732 bytes; tail: …' customer_sk\\norder by\\n
  sr.customer.customer_sk\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select catalog_sales.sold_date.date as sale_date, catalog_sales.ext_sales_price as ext_sales_price union select web_sales.sold_date.date as sale_date, web_sales.ext_sales_price as ext_sales_price limit 5;`

  ```text
  --> 3:100
    |
  3 | select catalog_sales.sold_date.date as sale_date,
  catalog_sales.ext_sales_price as ext_sales_price union select
  web_sales.sold_date.date as sale_date, web_sales.ext_sales_price as
  ext_sales_price limit 5;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...ales_price as ext_sales_price ??? union select web_sales.sold_da...
  ```
- `trilogy file write query10.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…rital_status,
    education_status,
    purchase_estimate,
    credit_rating,
    dep_count,
    dep_employed_count,
    dep_college_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store_sales.customer.customer_sk ')
  at line 46, column 53.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...stomer.customer_sk in (select ??? store_sales.customer.customer_...

  Write stats: received 3096 chars / 3096 bytes; tail: …'nt,\\n
  dep_employed_count,\\n    dep_college_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.customer.household_demographics.dep_count from store_sales where store_sales.date_dim.yea…s = 'M' and store_sales.customer.customer_demographics.education_status = 'Advanced Degree' and store_sales.sales_price between 100 and 150 limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...sehold_demographics.dep_count ??? from store_sales where store_s...
  ```
- `trilogy file write query22.preql --content import raw.inventory as inventory;
import raw.date_dim as date_dim;
import raw.item as item;

# Rollup: each group…_qoh asc nulls first,
    product_name asc nulls first,
    brand asc nulls first,
    class asc nulls first,
    category asc nulls first
limit 100;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'union') at line 38, column 1.
  Expected one of:
          * MERGE
          * _TERMINATOR
          * HAVING
          * ORDER
          * LIMIT

  Location:
  ...inventory.item.category) = 0  ??? union  select     inventory.it...

  Write stats: received 2509 chars / 2509 bytes; tail: …'ass asc nulls first,\\n
  category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query22.preql --content import raw.inventory as inventory;
import raw.date_dim as date_dim;
import raw.item as item;

# Rollup over (produ…_qoh asc nulls first,
    product_name asc nulls first,
    brand asc nulls first,
    class asc nulls first,
    category asc nulls first
limit 100;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n\n') at line 25, column 1.
  Expected one of:
          * METADATA
          * COMMA
          * ORDER
          * _TERMINATOR
          * WHERE
          * HAVING
          * MERGE
          * LIMIT
  Previous tokens: [Token('IDENTIFIER', 'avg_qoh')]

  Location:
     avg_qoh_detail as avg_qoh  ??? union  select     inventory.it...

  Write stats: received 1912 chars / 1912 bytes; tail: …'ass asc nulls first,\\n
  category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query22.preql --content import raw.inventory as inventory;
import raw.date_dim as date_dim;
import raw.item as item;

# Year 2000 filter a…_qoh asc nulls first,
    product_name asc nulls first,
    brand asc nulls first,
    class asc nulls first,
    category asc nulls first
limit 100;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 26, column 1.
  Expected one of:
          * HAVING
          * ORDER
          * LIMIT
          * METADATA
          * _TERMINATOR
          * COMMA
          * WHERE
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'avg_qoh')]

  Location:
      avg_qoh_detail as avg_qoh ??? union select     inventory.ite...

  Write stats: received 2026 chars / 2026 bytes; tail: …'ass asc nulls first,\\n
  category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query22.preql --content import raw.inventory as inventory;
import raw.date_dim as date_dim;
import raw.item as item;

# Rollup over (produ…_qoh asc nulls first,
    product_name asc nulls first,
    brand asc nulls first,
    class asc nulls first,
    category asc nulls first
limit 100;`

  ```text
  …
      * PARSE_COMMENT
          * PUBLISH_ACTION
          * SELECT
          * UNIQUE
          * _DEF_TABLE
          * PARAM
          * RAW_SQL
          * PROPERTY
          * "merge"i
          * SHOW
          * PARAMETER
          * TYPE
          * DATASOURCE
          * AUTO
          * WHERE
          * $END
          * IMPORT
          * DATASOURCE_ROOT
          * MOCK
          * WITH
          * PERSIST_MODE
          * SHORTHAND_MODIFIER
          * COPY
          * CHART
          * DATASOURCE_PARTIAL
          * _PROPERTIES
          * VALIDATE
          * FROM
          * ROWSET
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
  ...tory.date_dim.date) = 2000);  ??? union(     select inventory.it...

  Write stats: received 1712 chars / 1712 bytes; tail: …'ass asc nulls first,\\n
  category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand. Suggestions: ['store_sales.item.brand']")
  ```
- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  local.ext_sales_price. Suggestions: ['store_sales.ext_sales_price',
  'store_sales.list_price', 'store_sales.sales_price']")
  ```
- `trilogy run query12.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.item_id. Suggestions: ['web_sales.item.item_id',
  'web_sales.item.item_desc']")
  ```
- `trilogy run query16.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  warehouse.warehouse_sk. Suggestions: ['catalog_sales.warehouse.warehouse_sk',
  'catalog_returns.warehouse.warehouse_sk',
  'catalog_sales.warehouse.warehouse_id']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.item_id. Suggestions: ['item_id', 'store_sales.item.item_id',
  'store_returns.item.item_id']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store.state. Suggestions: ['store_state', 'store_sales.store.state',
  'store_returns.store.state']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store.state. Suggestions: ['store_state', 'store_sales.store.state',
  'store_returns.store.state']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.item_id. Suggestions: ['catalog_sales.item.item_id']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.item_id. Suggestions: ['catalog_sales.item.item_id']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  bill_customer.customer_address.country. Suggestions:
  ['catalog_sales.bill_customer.customer_address.country',
  'catalog_sales.bill_customer.customer_address.county',
  'catalog_sales.ship_customer.customer_address.country']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand_id. Suggestions: ['store_sales.item.brand_id']")
  ```

### `cli-misuse`

- `trilogy run -- -e import raw.store_sales as s; select s.customer.customer_sk, s.customer.first_name, s.customer.last_name, s.customer.preferred_cust_flag, s.customer.customer_id limit 5;`

  ```text
  'import raw.store_sales as s; select s.customer.customer_sk, s.customer.first_name, s.customer.last_name, s.customer.preferred_cust_flag, s.customer.customer_id limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c merge c.bill_customer.customer_sk into ~s.customer.customer_sk; select s.customer.customer_sk, c.bill_customer.customer_sk limit 5;`

  ```text
  'select s.customer.customer_sk, c.bill_customer.customer_sk limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run -- --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c merge c.bill_customer.customer_sk into ~s.customer.customer_sk; select s.customer.customer_sk, c.bill_customer.customer_sk limit 5;`

  ```text
  'raw/store_sales.preql:s' looks like a file path, not a dialect. The dialect argument comes AFTER the input file.
    Try: trilogy run raw/store_sales.preql:s <dialect>
  ```
- `trilogy explore raw/web_sales.preql --grep web_site -p key`

  ```text
  No such option: -p
  ```
- `trilogy explore raw/web_returns.preql --show concepts -p key`

  ```text
  No such option: -p
  ```
- `trilogy explore raw/store_sales.preql --show concepts -p key`

  ```text
  No such option: -p
  ```
- `trilogy read_file trilogy.toml`

  ```text
  No such command 'read_file'.
  ```
- `trilogy run - select 1 as test;`

  ```text
  'select 1 as test;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy file delete test_q2.preql test_q2_2.preql test_q2_name.preql test_year.preql`

  ```text
  Got unexpected extra arguments (test_q2_2.preql test_q2_name.preql test_year.preql)
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy run --import raw/inventory:inventory --import raw/item:item select grouping(inventory.item.product_name) --limit 1`

  ```text
  'grouping(inventory.item.product_name)' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `other`

- `trilogy run -e import raw.store_sales as s; select s.customer.customer_sk, s.customer.first_name, s.customer.last_name, s.customer.preferred_cust_flag, s.customer.customer_id limit 5;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: import raw.store_sales as s; select s.customer.customer_sk,
  s.customer.first_name, s.customer.last_name, s.customer.preferred_cust_flag,
  s.customer.customer_id limit 5;
  ```
- `trilogy run --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c merge c.bill_customer.customer_sk into ~s.customer.customer_sk; select s.customer.customer_sk, c.bill_customer.customer_sk limit 5;`

  ```text
  Duplicate select output for s.customer.customer_sk; Line: 3
  ```
- `trilogy run --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c merge c.bill_customer.customer_sk into ~s.customer.customer_sk; select s.customer.customer_sk, c.bill_customer.customer_sk limit 5;`

  ```text
  Duplicate select output for s.customer.customer_sk; Line: 3
  ```
- `trilogy run --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c merge c.bill_customer.customer_sk into ~s.customer.customer_sk; select s.cust….ext_wholesale_cost - s.ext_discount_amt + s.ext_sales_price) / 2 ? s.date_dim.year = 2002) as store_2002 where coalesce(store_2001, 0) > 0 limit 10;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.store_2001) in the same statement where clause; move to the HAVING
  clause instead; Line: 3
  ```
- `trilogy run -e -c import raw.store_sales as store_sales;
select 
  store_sales.date_dim.year,
  count(store_sales.ticket_number) as cnt
limit 10;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: -c
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 45 (char 44). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query18.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 153:     grouping(coalesce("vacuous"."catalog_sales_bill_customer_cu...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "catalog_sales_catalog_sales"."cs_bill_cdemo_sk" as
  "catalog_sales_bill_cdemo_demo_sk",
      "catalog_sales_catalog_sales"."cs_bill_customer_sk" as
  "catalog_sales_bill_customer_customer_sk",
      "catalog_sales_catalog_sales"."cs_item_sk" as "catalog_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "cat
  …
  ") asc nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ountry","young"."catalog_sales_bill_customer_customer_address_country")) asc
  nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_s
  tate","young"."catalog_sales_bill_customer_customer_address_state")) asc nulls
  first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ounty","young"."catalog_sales_bill_customer_customer_address_county")) asc
  nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy run query22.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```

### `file-not-found`

- `trilogy run raw/store_sales.preql:s duckdb --import raw/store_sales.preql:s --import raw/catalog_sales.preql:c`

  ```text
  Input 'raw/store_sales.preql:s' does not exist.
  ```
- `trilogy run query15.preql`

  ```text
  Input 'query15.preql' does not exist.
  ```
- `trilogy run query17.preql`

  ```text
  Input 'query17.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_sales select web_sales.ext_sales_price ? web_sales.ext_sales_price is not null limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...s.ext_sales_price is not null ??? limit 5;
  ```
- `trilogy run --import raw/store_sales:store_sales select count(store_sales.ticket_number) where store_sales.item.manager_id = 8;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.item.manager...
  ```

### `join-resolution`

- `trilogy run --import raw/catalog_sales.preql:c --import raw/store_sales.preql:s --import raw/web_sales.preql:w select c.bill_customer.customer_sk, s.customer.customer_sk, w.bill_customer.customer_sk limit 5;`

  ```text
  Could not resolve connections for query with output
  ['c.bill_customer.customer_sk<Purpose.KEY>Derivation.ROOT>',
  's.customer.customer_sk<Purpose.KEY>Derivation.ROOT>',
  'w.bill_customer.customer_sk<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
