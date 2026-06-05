# Trilogy failure analysis — 20260603-144033

- Run `20260603-144030_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 227 | failed: 23 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 9 | 39% |
| `other` | 7 | 30% |
| `undefined-concept` | 3 | 13% |
| `syntax-missing-alias` | 2 | 9% |
| `join-resolution` | 1 | 4% |
| `cli-misuse` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year, count(all_sales.order_id) as cnt group by all_sales.date.year order by all_sales.date.year;`

  ```text

  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Restrict to WEB and CATALOG channels only
where all_sales.sales_channel in (…    fri_ratio as friday,
    sat_ratio as saturday
where
    all_sales.date.week_seq in wk_2001
order by
    all_sales.date.week_seq asc nulls first;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw/all_sales as a;

# Sales side: gross sales (ext_sales_price) and net profit
# filtered by sales date an…end;

select
    channel_label,
    entity_id,
    gross_sales,
    total_returns,
    net_profit_value
order by channel_label, entity_id
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as a;

# --- Sales-side aggregates (by sales_channel + channel_dim_id) ---
auto sales_gross <…es_profit - return_loss_val as net_profit_value
where sales_gross is not null or return_amt is not null
order by channel_label, entity_id
limit 100;
`

  ```text

  ```
- `trilogy run --import raw/all_sales:a select a.sales_channel, a.channel_dim_id, a.channel_dim_id.channel_dim_text_id, count(a.row_one) as cnt where a.channel_dim_id is not null group by a.sales_channel, a.channel_dim_id, a.channel_dim_id.channel_dim_text_id limit 10;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as a;

# Sales side: group by channel and sales entity
where a.date.date between '2000-08-23'…esce(r_returns, 0) as total_returns,
    coalesce(s_profit, 0) - coalesce(r_loss, 0) as net_profit_value
order by channel asc, dim_id asc
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as a;

# Sales side: group by channel and sales entity
where a.date.date between '2000-08-23'…esce(r_returns, 0) as total_returns,
    coalesce(s_profit, 0) - coalesce(r_loss, 0) as net_profit_value
order by channel asc, dim_id asc
limit 100;
`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.all_sales as a;

# Combined entity ID using coalesce
# For rows with a sale (date is not null, channel_…ing gross_sales > 0 or total_returns > 0
order by g_channel asc, a.sales_channel asc nulls first, g_entity asc, entity_id asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query10.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

# Mer…ndent_count,
    store_sales.customer.demographics.employed_dependent_count,
    store_sales.customer.demographics.college_dependent_count
limit 100;`

  ```text

  ```

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 51 column 12 (char 2235). Re-issue the call with valid JSON arguments.
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.date.day_of_week, all_sales.date.day_name where all_sales.date.year = 2001 order by all_sales.date.day_of_week;`

  ```text

  ```
- `trilogy run --import raw/all_sales:a select a.sales_channel, count(a.row_one) limit 10;`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw/all_sales:a --import raw/store:s --import raw/catalog_page:cp --import raw/web_site:ws select a.sales_channel, a.channel_dim_id, a.channel_dim_id.channel_dim_text_id, s.id.text_id, s.id.name limit 10;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
