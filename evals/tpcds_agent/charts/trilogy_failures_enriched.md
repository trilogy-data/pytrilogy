# Trilogy failure analysis — 20260603-005059

- Run `20260603-005045_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 46 | failed: 10 (22%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 5 | 50% |
| `other` | 2 | 20% |
| `undefined-concept` | 2 | 20% |
| `syntax-missing-alias` | 1 | 10% |

## Detail

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales: total gross sales and net profit by channel + entity
# Filter: sales date in …_a, 0) -> gross_sales,
    coalesce(total_returns_b, 0) -> total_returns,
    coalesce(sale_profit_a, 0) - coalesce(return_loss_b, 0) -> net_profit;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', ': sale_entity_id,
  return_entity_id\nderive\n    coalesce(gross_sales_a, 0) -> gross_sales,\n
  coalesce(total_returns_b, 0) -> total_returns,\n    coalesce(sale_profit_a, 0)
  - coalesce(return_loss_b, 0) -> net_profit;\n') at line 24, column 8.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * COMMA
          * AND
          * DERIVE
          * WHERE
          * ORDER
  Previous tokens: [Token('IDENTIFIER', 'ent')]

  Location:
  ...annel, return_channel,     ent ??? : sale_entity_id, return_entit...

  Write stats: received 1361 chars / 1361 bytes; tail: …'le_profit_a, 0) -
  coalesce(return_loss_b, 0) -> net_profit;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales aggregate by channel entity
where s.date.date between '2000-08-23'::date and '…alesce(sale_profit_a, 0) - coalesce(return_loss_b, 0) -> net_profit
order by sale_channel asc nulls first, sale_entity_id asc nulls first
limit 100;
`

  ```text
  …
  RATE_ARRAY
          * WINDOW_TYPE_SQL_NAVIGATION
          * INT_LITERAL_PART
          * _LIKE
          * CONCAT
          * _UPPER
          * FALSE
          * _SUBSTRING
          * _STRUCT
          * CURRENT_DATETIME
          * _UNNEST
          * _UNION
          * MULTILINE_STRING
          * DIVIDE
          * LPAR
          * TRUE
          * _VARIANCE
          * _ILIKE
          * _ROUND
          * _HOUR
          * _DAY
          * _MONTH_NAME
          * GREATEST
          * IDENTIFIER
          * _DAY_NAME
          * QUOTE
          * _ARRAY_SORT
          * WINDOW_TYPE_SQL_NUMBERING
  Previous tokens: [Token('WHEN', 'when')]

  Location:
  ...ntity_id derive     case when ??? --ch = 'CATALOG' then 'catalog...

  Write stats: received 1692 chars / 1692 bytes; tail: …' asc nulls first,
  sale_entity_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales aggregate by channel entity
where s.date.date between '2000-08-23'::date and '…n ch = 'grand total' then 0 when ent = 'catalog_page' or ent = 'store' or ent = 'web_site' then 1 else 2 end asc,
    ch asc,
    ent asc
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 28, column 10.
  Expected one of:
          * LIMIT
          * _TERMINATOR

  Location:
   net_profit order by     case ??? when ch = 'grand total' then 0...

  Write stats: received 2349 chars / 2349 bytes; tail: …"' then 1 else 2 end
  asc,\\n    ch asc,\\n    ent asc\\nlimit 100;\\n".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Sales aggregate by channel entity
where s.date.date between '2000-08-23'::date and '… coalesce(sale_profit_a, 0) - coalesce(return_loss_b, 0) -> net_profit
order by ch collate nocase asc, ent collate nocase asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'collate ') at line 27, column 13.
  Expected one of:
          * _TERMINATOR
          * LIMIT
  Previous tokens: [Token('ORDER_IDENTIFIER', 'ch ')]

  Location:
   0) -> net_profit order by ch ??? collate nocase asc, ent collat...

  Write stats: received 2257 chars / 2257 bytes; tail: …'e nocase asc, ent
  collate nocase asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.return_channel_dim_text_id, sum(all_sales.return_amount) as ret_amt wh…turn_channel_dim_id is not null and all_sales.channel_dim_id is null group by all_sales.sales_channel, all_sales.return_channel_dim_text_id limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._sales.channel_dim_id is null ??? group by all_sales.sales_chann...
  ```

### `other`

- `trilogy run --import raw/all_sales:all_sales select all_sales.sales_channel, all_sales.channel_dim_text_id as sale_entity, all_sales.return_channel_dim_text_id as return_entity, all_sales.net_profit, all_sales.return_amount, all_sales.return_net_loss where all_sales.return_amount > 0 limit 10;`

  ```text
  (_duckdb.OutOfMemoryException) Out of Memory Error: could not
  allocate block of size 256.0 KiB (24.9 GiB/25.0 GiB used)

  Possible solutions:
  * Reducing the number of threads (SET threads=X)
  * Disabling insertion-order preservation (SET preserve_insertion_order=false)
  * Increasing the memory limit (SET memory_limit='...GB')

  See also
  https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_return_channel_dim_text_id",
       'CATALOG'  as "all_sales_sales_channel"
  FROM
  …
  =
  "concerned"."all_sales_order_id" AND "sweltering"."all_sales_sales_channel" =
  "concerned"."all_sales_sales_channel"
      LEFT OUTER JOIN "uneven" on "sweltering"."all_sales_channel_dim_id" =
  "uneven"."all_sales_channel_dim_id" AND "sweltering"."all_sales_sales_channel"
  = "uneven"."all_sales_sales_channel"
      INNER JOIN "thoughtful" on "sweltering"."all_sales_sales_channel" =
  "thoughtful"."all_sales_sales_channel"
  WHERE
      "concerned"."all_sales_return_amount" > 0

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run --import raw/all_sales:all_sales 
# Sales by channel entity
auto sale_gross <- sum(all_sales.ext_sales_price) 
    by all_sales.sales_channel, al…rn_channel_dim_text_id;

select all_sales.sales_channel, all_sales.channel_dim_text_id, sale_gross, sale_profit, return_total, return_loss
limit 20;
`

  ```text

  ```

### `undefined-concept`

- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  channel_dim_text_id. Suggestions: ['s.channel_dim_text_id',
  's.return_channel_dim_text_id', 's.channel_dim_id']")
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  channel_dim_text_id. Suggestions: ['s.channel_dim_text_id',
  's.return_channel_dim_text_id', 's.channel_dim_id']")
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.sales_channel, all_sales.channel_dim_text_id limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct all_sales.sales_channel as
  distinct_all_sales_sales_channel`
  Location:
  ...as all_sales; select distinct ??? all_sales.sales_channel, all_s...
  ```
