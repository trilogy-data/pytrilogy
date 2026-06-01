# Trilogy failure analysis — 20260601-025819

- Run `20260601-025817_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 132 | failed: 10 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 4 | 40% |
| `undefined-concept` | 3 | 30% |
| `syntax-parse` | 2 | 20% |
| `syntax-missing-alias` | 1 | 10% |

## Detail

### `other`

- `trilogy run query23.preql duckdb`

  ```text
  Have
  {'MergeNode<all_sales.sales_channel,local._virt_func_substring_2877672313873438
  ,store_sales.item.text_id...10 more>': None} and need
  BuildSubselectComparison(left=all_sales.sales_channel@Grain<all_sales.sales_cha
  nnel>, right=('CATALOG', 'WEB'), operator=<ComparisonOperator.IN: 'in'>) and
  all_sales.date.year = 2000 and all_sales.date.month_of_year = 2 and
  local.frequent_item_count > 4 and local.cust_total >
  multiply(0.5,local.max_cust@Grain<Abstract>)
  ```
- `trilogy run query24.preql`

  ```text
  HAVING references 'local.avg_stage1_total', which is not in
  the SELECT projection (line 20). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.avg_stage1_total
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query24.preql`

  ```text
  HAVING references 'local.avg_stage1_total', which is not in
  the SELECT projection (line 26). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.avg_stage1_total
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query27.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```

### `undefined-concept`

- `trilogy run query23.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  last_name.')
  ```
- `trilogy run query23.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  last_name.')
  ```
- `trilogy run query23.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  last_name.')
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.billing_customer.id, count(all_sales.order_id) as cnt where all_sales.date.year=2000 and all_sa….month_of_year=2 and all_sales.sales_channel in ('CATALOG','WEB') and all_sales.billing_customer.id is not null group by 1 order by cnt desc limit 5;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...lling_customer.id is not null ??? group by 1 order by cnt desc l...
  ```
- `trilogy file write query23.preql`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
  ...em.text_id ? fr_item_cnt > 4 ??? group by all_sales.billing_cus...

  Write stats: received 1406 chars / 1406 bytes; tail: …'c, first_name asc,
  total_sales asc nulls first\r\\nlimit 100;\r\\n'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.ticket_number) where store_sales.date.year between 2000 and 2003;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as
  ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.date.year be...
  ```
