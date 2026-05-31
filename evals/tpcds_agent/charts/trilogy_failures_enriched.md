# Trilogy failure analysis — 20260531-175940

- Run `20260531-175939_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 192 | failed: 18 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 6 | 33% |
| `other` | 4 | 22% |
| `join-resolution` | 3 | 17% |
| `undefined-concept` | 2 | 11% |
| `syntax-missing-alias` | 2 | 11% |
| `file-not-found` | 1 | 6% |

## Detail

### `syntax-parse`

- `trilogy file write query60.preql -c import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
i…,
    coalesce(store_total, 0) + coalesce(catalog_total, 0) + coalesce(web_total, 0) as combined_total
order by item_code, combined_total
limit 100;
`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 18, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'music_class_ids')]

  Location:
  ...ress, Sep 1998, GMT offset -5 ??? auto store_total <- sum(store_...

  Write stats: received 1498 chars / 1498 bytes; tail: …'ombined_total\\norder by
  item_code, combined_total\\nlimit 100;\\n'.
  ```
- `trilogy file write query61.preql --content import raw.physical_sales as store_sales;

# Filter: November 1998, Jewelry category, store GMT offset -5, custome…l,
  total_sales as overall_total,
  (promo_sales / nullif(total_sales, 0)) * 100 as promo_pct
order by promo_channel_total, overall_total
limit 100;`

  ```text
  refused to write 'query61.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 11, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ll total extended sales price ??? auto total_sales <- sum(ext_sa...

  Write stats: received 805 chars / 805 bytes; tail: …'o_pct\\norder by
  promo_channel_total, overall_total\\nlimit 100;'.
  ```
- `trilogy file write query63.preql --content import raw.physical_sales as physical_sales;

# Filter: year 2000, store must have recorded store id, and items ma…month_total - avg_monthly_sales) / avg_monthly_sales > 0.1
order by
    manager_id asc,
    avg_monthly_sales asc,
    per_month_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 17, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ..._price (per-unit sales price) ??? auto per_month_total <- sum(ph...

  Write stats: received 1442 chars / 1442 bytes; tail: …'   avg_monthly_sales
  asc,\\n    per_month_total asc\\nlimit 100;'.
  ```
- `trilogy file write query63.preql --content import raw.physical_sales as physical_sales;
where physical_sales.date.year = 2000
  and physical_sales.store.id i…month_total - avg_monthly_sales) / avg_monthly_sales > 0.1
order by
    manager_id asc,
    avg_monthly_sales asc,
    per_month_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 13, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
   #1', 'importoamalg #1'))   ) ??? auto per_month_total <- sum(ph...

  Write stats: received 1233 chars / 1233 bytes; tail: …'   avg_monthly_sales
  asc,\\n    per_month_total asc\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_returns as cr;

# Merge catalog_returns' sales.item.id to item…
  and yr_2000.cnt <= yr_1999.cnt
order by
    yr_1999.pn,
    yr_1999.sn,
    yr_2000.cnt desc,
    yr_1999.wc desc,
    yr_2000.wc desc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'ss.item.product_name ') at line 21,
  column 9.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ..._sales <-      select         ??? ss.item.product_name as pn,

  Write stats: received 3889 chars / 3889 bytes; tail: …'t desc,\\n    yr_1999.wc
  desc,\\n    yr_2000.wc desc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.item.category, count(store_sales.row_counter) group by store_sales.item.category limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(store_sales.row_counter) ??? group by store_sales.item.cate...
  ```

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 30 column 12 (char 1821). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query65.preql`

  ```text
  ORDER BY references 'physical_sales.store.id', which is not
  in the SELECT projection (line 10). Add it to SELECT to sort by it — prefix
  with `--` to keep it out of the output rows, e.g. `select ...,
  --physical_sales.store.id order by physical_sales.store.id asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 134 column 12 (char 14712). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query69.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```

### `join-resolution`

- `trilogy run query64.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['ss.item.product_name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.item_id<Purpose.KEY>Derivation.BASIC>',
  'ss.store.name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.store_zip<Purpose.PROPERTY>Derivation.BASIC>',
  'ss.sale_address.street_number<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.sale_address.street_name<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.sale_address.city<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sale_zip<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cust_street_number<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cust_street_n
  …
  PROPERTY>Derivation.BASIC>',
  'local.cust_city<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cust_zip<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sale_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cust_first_sales_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cust_first_ship_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sale_line_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.wholesale_cost_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.list_price_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.coupon_amt_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query69.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.billing_customer.demographics.gender<Purpose.PROPERTY>Derivation.
  ROOT>',
  'store_sales.billing_customer.demographics.marital_status<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.billing_customer.demographics.education_status<Purpose.PROPERTY>De
  rivation.ROOT>',
  'store_sales.billing_customer.demographics.purchase_estimate<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.billing_customer.demographics.credit_rating<Purpose.PROPERTY>Deriv
  ation.ROOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query69.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.billing_customer.demographics.gender<Purpose.PROPERTY>Derivation.
  ROOT>',
  'store_sales.billing_customer.demographics.marital_status<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.billing_customer.demographics.education_status<Purpose.PROPERTY>De
  rivation.ROOT>',
  'store_sales.billing_customer.demographics.purchase_estimate<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.billing_customer.demographics.credit_rating<Purpose.PROPERTY>Deriv
  ation.ROOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```

### `undefined-concept`

- `trilogy run query61.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ext_sales_price. Suggestions: ['store_sales.ext_sales_price',
  'store_sales.list_price', 'store_sales.sales_price']")
  ```
- `trilogy run query64.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  cr.ext_list_price. Suggestions: ['cs.ext_list_price', 'ss.ext_list_price',
  'cr.sales.ext_list_price']")
  ```

### `syntax-missing-alias`

- `trilogy file write query67.preql --content import raw.physical_sales as store_sales;

# Sum of (sales_price * quantity) treating null as 0
auto total_sales <…,
    month_of_year asc nulls first,
    store_code asc nulls first,
    summed_sales asc nulls first,
    within_cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `total_sales by rollup store_sales.item.category as
  total_sales_by_rollup_store_sales_item_c`
  Location:
  ...store_code,     total_sales by ??? rollup store_sales.item.categ...

  Write stats: received 1447 chars / 1447 bytes; tail: …' nulls first,\\n
  within_cat_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.item.category limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct store_sales.item.category as
  distinct_store_sales_item_category`
  Location:
   store_sales; select distinct ??? store_sales.item.category limi...
  ```

### `file-not-found`

- `trilogy run query69.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  web_sales_billing_customer_customers does not exist!
  Did you mean "web_sales"?

  LINE 10: ...ng_customer_customers."web_sales_billing_customer_id" from
  web_sales_billing_customer_customers where web_sales_billin...
                                                                         ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as
  "store_sales_billing_customer_id",
      "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
  FROM
      "store_sales" as "store_sales_store_sales"
  WHER
  …
      count("uneven"."store_sales_billing_customer_id") as "customer_count3"
  FROM
      "uneven"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "uneven"."store_sales_billing_customer_demographics_gender" asc,
      "uneven"."store_sales_billing_customer_demographics_marital_status" asc,
      "uneven"."store_sales_billing_customer_demographics_education_status" asc,
      "uneven"."store_sales_billing_customer_demographics_purchase_estimate" asc,
      "uneven"."store_sales_billing_customer_demographics_credit_rating" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
