# Trilogy failure analysis — 20260608-031503

- Run `20260608-031455_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 2320 | failed: 374 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 205 | 55% |
| `syntax-parse` | 90 | 24% |
| `undefined-concept` | 34 | 9% |
| `join-resolution` | 17 | 5% |
| `syntax-missing-alias` | 15 | 4% |
| `cli-misuse` | 8 | 2% |
| `type-error` | 3 | 1% |
| `file-not-found` | 2 | 1% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 63 column 12 (char 3181). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query07.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales parameter zips string;
auto pzips <- unnest(split(zips, ','));
auto pref_zips <- store_sales.customer.custom…2) in qprefixes
select store_sales.store.store_name, sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text

  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 52 column 12 (char 2732). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select count(ss.item.item_sk) as ss_cnt, count(cs.item.item_sk) as cs_cnt, ss.item.brand_id as br limit 5;`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs merge cs.item.item_sk into ~ss.item.item_sk; merge cs.bill_customer.customer_sk into ~s…oy in (1,2,3) and ss.customer.customer_sk in cs.bill_customer.customer_sk select count(ss.quantity) as ss_cnt, count(cs.quantity) as cs_cnt limit 10;`

  ```text

  ```
- `trilogy run query17.preql`

  ```text

  ```
- `trilogy run query17.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs merge cs.item.item_sk into ~ss.item.item_sk; merge cs.bill_customer.customer_sk into ~s…tity) as ss_cnt, count(cs.quantity) as cs_cnt, count(ss.quantity ? ss.customer.customer_sk in cs.bill_customer.customer_sk) as ss_cnt_inner limit 10;`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select substring(store_sales.item.item_desc, 1, 30) as desc_prefix, count(store_sales.date_dim.date_sk) as cnt, store_sales.item.item_sk where store_sales.date_dim.year between 2000 and 2003 and cnt > 4 limit 10;`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run query23.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss auto per_customer_spend <- sum(ss.quantity * ss.sales_price ? ss.date_dim.year between 2000 and 2003 and ss.customer.…? ss.date_dim.year between 2000 and 2003 and ss.customer.customer_sk is not null) as sp having sp > 0.5 * global_max_spend order by sp desc limit 10;`

  ```text

  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql`

  ```text

  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query26.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query33.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query34.preql`

  ```text

  ```
- `trilogy file read query34.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query36.preql`

  ```text

  ```
- `trilogy run query36.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.item.category) + count(store_sales.item.class) as cnt, count_distinct(concat(store_…em.category)+grouping(store_sales.item.class) as string))) as distinct_cnt where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN';`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category as cat, store_sales.item.class as cls, sum(store_sales.net_profit) as np, s…grouping(store_sales.item.class) as hl where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' and hl > 0 order by hl desc, gm asc;`

  ```text

  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy run query39.preql`

  ```text

  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query42.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05_check9.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy run query54.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy run query57.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query60.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query61.preql`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query56test15.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2342 (char 2341). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy file read query64.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query65.preql`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query66.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1470 (char 1469). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy run query70.preql`

  ```text

  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe inventory`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query73.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy run query74.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 53 column 12 (char 1843). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05_check.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr - duck_db`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.web_sales:ws - duck_db`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 67 column 12 (char 2277). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query80.preql --import raw.store_sales:ss select 'store' as ch, concat('store', ss.store.store_id) as out, count(ss.ticket_number) as cnt where s…-08-23'::date and '2000-09-22'::date and ss.item.current_price > 50 and ss.promotion.channel_tv is not null and ss.store.store_sk is not null limit 3`

  ```text

  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;

where sr.date_dim.week_seq in (… null and cr_qty_by_item is not null and wr_qty_by_item is not null
order by item asc nulls first, store_qty asc nulls first
limit 100; query83.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05_check.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy run query87.preql`

  ```text

  ```
- `trilogy file read raw/time_dim.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query77_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/reason.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr merge sr.item.item_sk into ~ss.item.item_sk; merge sr.ticket_number into ~ss.ticket_num…n sr.return_quantity is not null then (ss.quantity - sr.return_quantity) * ss.sales_price else ss.quantity * ss.sales_price end ) is not null; duckdb`

  ```text

  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/store_returns:store_returns select store_returns.customer.customer_id, sum(store_returns.return_amt) as total_returns, store_returns….date_dim.year = 2000 group by store_returns.customer.customer_id, store_returns.store.store_sk order by store_returns.customer.customer_id limit 20;`

  ```text

  ```
- `trilogy run --import raw.date_dim:date_dim select date_dim.week_seq, date_dim.year, min(date_dim.date) as first_day, max(date_dim.date) as last_day where date_dim.year in (2001, 2002) group by date_dim.week_seq, date_dim.year order by date_dim.week_seq limit 20;`

  ```text

  ```
- `trilogy run -e PAGER= --import raw.date_dim:date_dim select date_dim.week_seq, count(date_dim.date_sk) as days where date_dim.year = 2001 group by date_dim.week_seq order by date_dim.week_seq limit 100;`

  ```text

  ```
- `trilogy run --import raw.date_dim:dd select dd.week_seq where dd.year = 2002 and dd.week_seq - 53 in (select dd.week_seq as ws where dd.year = 2001) limit 5;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…    coalesce(ra, 0) as total_returns,
    coalesce(gs, 0) - coalesce(nl, 0) as net_profit
order by ch asc nulls first, eid asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…as total_returns_out,
    coalesce(net_loss_amt, 0) as net_profit
order by channel_type asc nulls first, entity_identifier asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query08.preql --content parameter zips string;

# Import the store_sales fact model
import raw/store_sales as store_sales;

# Preferred cu…ip_prefixes
select
  store_sales.store.store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_name, sum(store_sales.net_profit) as total_net_profit, count(store_sales.tick…tore_sales.date_dim.year = 1998 and store_sales.date_dim.qoy = 2 group by store_sales.store.store_name order by store_sales.store.store_name limit 5;`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…cs_cnt_nov,
    coalesce(ws_cnt, 0) as ws_cnt_nov
having ss_present > 0 and cs_present > 0 and ws_present > 0
    and ss_sales_nov > overall_avg_sale`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…present > 0 and cs_present > 0 and ws_present > 0
    and (ss_sales > overall_avg_sale or cs_sales > overall_avg_sale or ws_sales > overall_avg_sale)`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Overall average sale valu…present > 0 and cs_present > 0 and ws_present > 0
    and (ss_sales > overall_avg_sale or cs_sales > overall_avg_sale or ws_sales > overall_avg_sale)`

  ```text

  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Find (brand_id, c…(ss.date_dim.date) = 11) as total_sales,
    count(ss.ticket_number ? year(ss.date_dim.date) = 2001 and month(ss.date_dim.date) = 11) as total_count;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws merge ss.item.item_sk into cs.item.item_sk; merge ss.item.ite…lect ss.item.brand_id as br, count(ss.item.item_sk) as ss_cnt, count(cs.item.item_sk) as cs_cnt, count(ws.item.item_sk) as ws_cnt group by 1 limit 5;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Merge shared dime…s catalog_sale_quantity_cv
order by ss.item.item_id asc nulls first,
  ss.item.item_desc asc nulls first,
  ss.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss select ss.date_dim.fy_year, ss.date_dim.qoy, count(ss.quantity) as cnt where ss.date_dim.fy_year = 2001 group by ss.date_dim.fy_year, ss.date_dim.qoy order by ss.date_dim.fy_year, ss.date_dim.qoy limit 20;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

merge sr.item.item_…qty_avg,
    --stddev(ss.quantity) as ss_qty_std,
    --count(sr.return_quantity) as sr_qty_cnt
    --avg(sr.return_quantity) as sr_qty_avg
limit 10;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

merge sr.item.item_…s_qty_avg_b, 0) as catalog_sale_quantity_cv
order by item_code asc nulls first,
  item_desc asc nulls first,
  store_state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

merge sr.item.item_…s_qty_avg_b, 0) as catalog_sale_quantity_cv
order by item_code asc nulls first,
  item_desc asc nulls first,
  store_state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

merge sr.item.item_…s_qty_avg_b, 0) as catalog_sale_quantity_cv
order by item_code asc nulls first,
  item_desc asc nulls first,
  store_state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query17.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

merge sr.item.item_…s store_return_quantity_cv
order by ss.item.item_id asc nulls first,
  ss.item.item_desc asc nulls first,
  ss.store.state asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select cs.bill_customer.customer_address.state as st, count(cs.order_number) as cnt where cs.sold_date.year = 1998 …d cs.bill_customer.birth_month in (1,2,6,8,9,12) and cs.bill_customer.customer_address.state in ('MS','IN','ND','OK','NM','VA') group by st limit 20;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select substring(store_sales.item.item_desc, 1, 30) as desc_prefix, count(store_sales.item.item_sk, store_sales.date_dim.date_sk) as cnt, store_sales.item.item_sk where store_sales.date_dim.year between 2000 and 2003 limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales auto item_date_pairs <- select store_sales.item.item_sk, store_sales.date_dim.date_sk where store_sales.date…as dp, store_sales.item.item_sk, count(item_date_pairs.date_sk) as cnt where store_sales.date_dim.year between 2000 and 2003 having cnt > 4 limit 10;`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;

# Frequent items: items where count of distinct sold dates > 4 (2000-2003)
rowset f…, 0) + coalesce(web_sales, 0) as total_sales

order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;
`

  ```text

  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…s, 0) + coalesce(web_sales, 0) as total_sales

order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

inner join sr.ticket_number = ss.ticket_number
inne…,
    --avg_all
having peach_person_store_sum > 0.05 * avg_all
order by ss.customer.last_name, ss.customer.first_name, ss.store.store_name
limit 50;
`

  ```text

  ```
- `trilogy file write query24.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.store.market_id = 8
  and ss.customer.cust…,
    --avg_all
having peach_person_store_sum > 0.05 * avg_all
order by ss.customer.last_name, ss.customer.first_name, ss.store.store_name
limit 50;
`

  ```text

  ```
- `trilogy file write query25.preql -e -c import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where ss.date_dim.year …) as store_returns_net_loss,
  sum(cs.net_profit) as catalog_sales_net_profit
order by item_code, item_description, store_code, store_name
limit 100;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Total extended store sales price by c…date_dim.qoy = 2), 0)) as store_q3_over_q2

having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2

order by county;
`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Store sales extended price by county …oy = 2), 0)) as store_q3_over_q2

having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2

order by county
limit 1000;`

  ```text

  ```
- `trilogy file write query31.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_add…oy = 2), 0)) as store_q3_over_q2

having
    web_q2_over_q1 > store_q2_over_q1
    and web_q3_over_q2 > store_q3_over_q2

order by county
limit 1000;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select max(cnt) as max_count from (select count(store_sales.ticket_number) as cnt where year(store_sales.dat…r.household_demographics.dep_count::numeric / store_sales.customer.household_demographics.vehicle_count::numeric > 1.2) by store_sales.ticket_number;`

  ```text

  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Arm 1: store sal… cs_sale_date, ws_sale_date

having cs_last_name is not null and ws_last_name is not null;

select count(1) as unique_customer_date_combos
limit 100;`

  ```text

  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Arm 1: store sal….first_name as ws_first_name,
    --web.sold_date.date as ws_sale_date
    count(1) as ws_count

align
    c: ss_count, cs_count, ws_count
limit 100;`

  ```text

  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;

# For year 2001, compute per (warehouse_sk, item_sk, moy):
# mean and stddev of quan… ? inv.date_dim.year = 2001 and inv.date_dim.moy = 2) by inv.warehouse.warehouse_sk, inv.item.item_sk / feb_mean end as feb_cv
    having feb_cv > 1;`

  ```text

  ```
- `trilogy run --import raw.item:item select item.manufact, count(item.item_sk) as cnt where (item.category = 'Books' and item.color = 'tan' and item.units = 'O…A') or (item.category = 'Electronics' and item.color = 'mint' and item.units = 'Gross' and item.size = 'N/A') group by item.manufact having cnt >= 2;`

  ```text

  ```
- `trilogy file write query44.preql`

  ```text

  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;
import raw.store as store;
import raw.item as item;
import raw.date_dim as dd;

# Fi….category, item.brand, store.store_name, store.company_name order by dd.year, dd.moy) as next_total
    sum(ss.sales_price) as monthly_total
limit 5;`

  ```text

  ```
- `trilogy run --import raw.web_sales:ws select ws.item.item_sk, sum(ws.quantity) as qty where ws.sold_date.year=2001 and ws.sold_date.moy=12 and ws.net_profit>0 and ws.net_paid>0 and ws.quantity>0 group by ws.item.item_sk order by ws.item.item_sk limit 5;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…k, return_currency_rank) as return_currency_rank
order by channel asc, return_qty_rank asc, return_currency_rank asc, item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…k, return_currency_rank) as return_currency_rank
order by channel asc, return_qty_rank asc, return_currency_rank asc, item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…k, return_currency_rank) as return_currency_rank
order by channel asc, return_qty_rank asc, return_currency_rank asc, item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…k, return_currency_rank) as return_currency_rank
order by channel asc, return_qty_rank asc, return_currency_rank asc, item asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr rowset web_items <- merge wr.order_number into ~ws.order_number; merge wr.item.item_sk into… web_items.channel, web_items.item, web_items.qty_ratio, web_items.qty_rank order by web_items.channel, web_items.qty_rank, web_items.item limit 100;`

  ```text

  ```
- `trilogy file write query51.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Daily sum of unit price per item for store sales
auto…esce(w_rm_val, w_rm_val) as web_running_max
having web_running_max > store_running_max
order by item asc nulls first, date asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query53.preql --content import raw.store_sales as ss;

# Filter: store sales in year 2000, items matching profile A or B, only recorded st…nd abs(total - avg_quarterly_sales) / avg_quarterly_sales > 0.1
order by
  avg_quarterly_sales asc,
  total asc,
  ss.item.manufact_id asc
limit 100;`

  ```text

  ```
- `trilogy file write query53.preql --content import raw.store_sales as ss;

# Per-quarter total unit price by (manufact_id, quarter)
auto quarterly_total <- su…
  avg_sales > 0
  and abs(quarterly_total - avg_sales) / avg_sales > 0.1
order by
  avg_sales asc,
  total asc,
  ss.item.manufact_id asc
limit 100;`

  ```text

  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.customer as c…s.state
select
  cust.customer_sk,
  sum(ss.ext_sales_price) as total_spent
  having total_spent is not null
  order by cust.customer_sk
  limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store select store.item.item_id as code, sum(store.ext_sales_price) as total where store.date_dim.year=2001 and store.da…er.customer_address.gmt_offset=-5 and store.item.color in ('slate', 'blanched', 'burnished') group by store.item.item_id order by total desc limit 5;`

  ```text

  ```
- `trilogy file write query56test4.preql --content import raw.store_sales as store;

where store.date_dim.year = 2001
  and store.date_dim.moy = 2
  and store.c…_id as ss_item_code,
  sum(store.ext_sales_price) as ss_total
align
  item_code: ss_item_code
derive
  ss_total as total
order by total desc
limit 5;`

  ```text

  ```
- `trilogy file write query56test8.preql --content import raw.store_sales as store;
import raw.item as item;

# All items arm (no color filter - just to see if … ss_tot
align
  item_code: all_code, ss_code
derive
  coalesce(ss_tot, 0) as total
where item_code = 'AAAAAAAAOLJDAAAA'
order by total desc
limit 10;`

  ```text

  ```
- `trilogy file write query56test15.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

merge st…_tot,
  sum(web.ext_sales_price
    ? web.sold_date.year = 2001 and web.sold_date.moy = 2
      and web.bill_addr.gmt_offset = -5) as w_tot
limit 10;`

  ```text

  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.date, date_dim.week_seq, date_dim.year from date_dim where date_dim.date = '2000-01-03'::date;`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_name, store_sales.store.store_id, store_sales.date_dim.week_seq, store_sales.…_sales.date_dim.dow = 0 and store_sales.date_dim.week_seq >= 5270 and store_sales.date_dim.week_seq <= 5273 group by 1,2,3,4 order by 1,3,4 limit 30;`

  ```text

  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…number) as cnt_00,
    --sum(ss.ext_wholesale_cost) as wholesale_00,
    --sum(ss.ext_list_price) as list_00,
    --sum(ss.coupon_amt) as coupon_00,
`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# State-level total net profit for year 2000, for determining top 5 states…ty,
    total,
    level as hierarchy_level,
    rnk as rank_within_parent

order by
    level desc,
    store_sales.store.state,
    rnk

limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# Compute state-level net profit for year 2000
auto state_profit_2000 <- s…al_net_profit,
    level as hierarchy_level,
    rnk as rank_within_parent

order by
    level desc,
    store_sales.store.state,
    rnk

limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# State-level net profit for year 2000 for ranking top states
auto state_p…otal_net_profit,
    level as hierarchy_level,
    rnk as rank_within_parent
order by
    level desc,
    store_sales.store.state,
    rnk
limit 100;`

  ```text

  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# State-level net profit for year 2000 (for ranking top states, only real …otal_net_profit,
    level as hierarchy_level,
    rnk as rank_within_parent
order by
    level desc,
    store_sales.store.state,
    rnk
limit 100;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select date_diff(cs.ship_date.date, cs.sold_date.date, DAY) as days, count(cs.order_number) as cnt where cs.sold_date.year = 1999 and cs.bill_hdemo.buy_potential = '>10000' and cs.bill_cdemo.marital_status = 'D' group by days order by days limit 20;`

  ```text

  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.customer.customer_sk, sum(store_sales.net_paid) as total, store_sales.date_dim.year where store_sales.date_dim.year in (2001, 2002) group by store_sales.customer.customer_sk, store_sales.date_dim.year limit 5;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.item as item;

import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

import raw.store_sale…lesce(ss_net_amt, 0) + coalesce(ws_net_amt, 0) as net_amt

order by year asc, brand_id asc, class_id asc, category_id asc, manufact_id asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.item as item;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales …e(amt_2002, 0) as amt_diff
having coalesce(qty_2002, 0) * 1.0 / nullif(coalesce(qty_2001, 0), 0) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query75.preql --content import raw.item as item;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales …-coalesce(curr_amt,0) as amt_diff
having coalesce(curr_qty,0)*1.0/nullif(coalesce(prev_qty,0),0) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…ve
    sales as sales,
    returns as returns,
    profit as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…turns,
    coalesce(profit_store, profit_cat, profit_web) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…
    coalesce(--profit_store, --profit_cat, --profit_web) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77_store3.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.stor…ns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-22'::date)), 0) as profit
order by outlet asc nulls first
limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr - duck_db`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr - duck_db`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr - duck_db`

  ```text

  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.web_sales as ws;
import raw.web_returns a…r by year asc, item asc, customer asc, store_qty desc, store_wc desc, store_sp desc, other_qty asc, other_wc asc, other_sp asc, ratio asc
limit 100;
`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query80.preql`

  ```text

  ```
- `trilogy file write query81.preql --content import raw/catalog_returns as cr;

auto cust_state_total <- sum(cr.return_amt_inc_tax) by cr.returning_customer.cu…ustomer.customer_address.gmt_offset asc,
    cr.returning_customer.customer_address.location_type asc,
    total_return_amount_inc_tax asc
limit 100;`

  ```text

  ```
- `trilogy run --import raw/store_returns:sr select sr.item.item_id, sum(sr.return_quantity) as sr_qty where sr.date_dim.week_seq in (5244, 5257, 5264) group by sr.item.item_id limit 5;`

  ```text

  ```
- `trilogy file write query85.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Join web_returns to web_sales by item and order_numbe…,
  avg(wr.refunded_cash) as avg_refunded_cash,
  avg(wr.fee) as avg_fee
order by r_reason_desc, avg_sold_qty, avg_refunded_cash, avg_fee
limit 100;
`

  ```text

  ```
- `trilogy file write query85.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;

# Join web_returns to web_sales by item and order_numbe…,
  avg(wr.refunded_cash) as avg_refunded_cash,
  avg(wr.fee) as avg_fee
order by r_reason_desc, avg_sold_qty, avg_refunded_cash, avg_fee
limit 100;
`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer identi…mer.first_name || ss.date_dim.date::string) not in (ws.bill_customer.last_name || ws.bill_customer.first_name || ws.sold_date.date::string)
limit 10;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer identi…e, ws.bill_customer.first_name, ws.sold_date.date;

select
    sum(case when in_ss > 0 and in_cs = 0 and in_ws = 0 then 1 else 0 end) as unique_count`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer identi…te is null
select count_distinct(ss.customer.last_name || '|' || ss.customer.first_name || '|' || ss.date_dim.date::string) as unique_count
limit 10;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Rowset of catalog_sales combos in 2000
rowset cs_…e.date::string as combo
where cs.sold_date.year = 2000;

# Check count of cs combos
select count_distinct(combo) as cs_count
from cs_combos
limit 10;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Define the set of catalog sales combos in 2000
au…t in cs_combo
select count_distinct(ss.customer.last_name || '|' || ss.customer.first_name || '|' || ss.date_dim.date::string) as not_in_cs
limit 10;`

  ```text

  ```
- `trilogy file write query93.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

inner join sr.item.item_sk = ss.item.item_sk
inner …quantity * ss.sales_price
    end
  ) as adjusted_amount
order by adjusted_amount asc nulls first, ss.customer.customer_id asc nulls first
limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr merge sr.item.item_sk into ~ss.item.item_sk; merge sr.ticket_number into ~ss.ticket_num… adjusted_amount group by ss.customer.customer_id order by adjusted_amount asc nulls first, ss.customer.customer_id asc nulls first limit 100; duckdb`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr merge sr.item.item_sk into ~ss.item.item_sk; merge sr.ticket_number into ~ss.ticket_num…ss.sales_price end ) as adjusted_amount order by adjusted_amount asc nulls first, ss.customer.customer_id asc nulls first limit 100 offset 90; duckdb`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw/store_returns:store_returns select date_dim.year, store_returns.return_amt limit 5;`

  ```text

  ```
- `trilogy run --import raw/store_returns:store_returns select store_returns.customer.customer_id, store_returns.store.store_sk, --date_dim.year where store_returns.date_dim.year=2000 limit 5;`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.year, store_sales.qoy, count(store_sales.ticket_number) as cnt limit 5;`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.item:item select item.brand_id, sum(ss.quantity * ss.list_price) as sales, count(ss.item_sk) as cnt, year(ss.date_dim.date) as yr limit 5;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs merge cs.item.item_sk into ~ss.item.item_sk; merge cs.bill_customer.customer_sk into ~s…em_sk = ss.item.item_sk select ss.item.item_id, count(ss.quantity) as ss_cnt, count(cs.quantity) as cs_cnt order by item_id asc nulls first limit 20;`

  ```text

  ```
- `trilogy run query19.preql`

  ```text

  ```
- `trilogy run query25.preql`

  ```text

  ```
- `trilogy run query27.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query38.preql`

  ```text

  ```
- `trilogy run query45.preql`

  ```text

  ```
- `trilogy run query48.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query68.preql`

  ```text

  ```
- `trilogy run query71.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query77.preql`

  ```text

  ```
- `trilogy run query80.preql`

  ```text

  ```
- `trilogy run query86.preql`

  ```text

  ```
- `trilogy run test_grouping.preql`

  ```text

  ```
- `trilogy run query88.preql`

  ```text

  ```
- `trilogy run query89.preql`

  ```text

  ```
- `trilogy run query95.preql`

  ```text

  ```
- `trilogy run query96.preql`

  ```text

  ```
- `trilogy run query98.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select count(ss.quantity) as ss_cnt, count(sr.return_quantity) as sr_cnt where ss.date_dim.fy_year = 2001 and ss.date_dim.qoy = 1 and sr.date_dim.fy_year = 2001 and sr.date_dim.qoy in (1,2,3) limit 10;`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select count(ss.quantity) as ss_cnt, count(cs.quantity) as cs_cnt where ss.date_dim.fy_year = 2001 and ss.date_dim.qoy = 1 and cs.sold_date.fy_year = 2001 and cs.sold_date.qoy in (1,2,3) limit 10;`

  ```text

  ```
- `trilogy run query17.preql`

  ```text

  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs where ss.date_dim.fy_year = 2001 and ss.date_dim.qoy = 1 and cs.sold_date.fy_year = 200…oy in (1,2,3) and ss.customer.customer_sk in cs.bill_customer.customer_sk select count(ss.quantity) as ss_cnt, count(cs.quantity) as cs_cnt limit 10;`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.order_number, cs.item.item_sk, cr.refunded_cash limit 5`

  ```text

  ```
- `trilogy run query47.preql`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```
- `trilogy run query56.preql`

  ```text

  ```
- `trilogy run query64.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query75.preql`

  ```text

  ```
- `trilogy run query84.preql`

  ```text

  ```
- `trilogy run --import raw.store_returns:sr --import raw.customer:c select c.customer_id, concat(c.last_name, ', ', c.first_name) as full_name where c.customer…graphics.income_band.lower_bound >= 38128 and c.household_demographics.income_band.upper_bound <= 88128 order by c.customer_id nulls first limit 100;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.quantity, ss.sales_price, sr.return_quantity, ss.customer.customer_id, sr.reason.desc limit 20; duckdb`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.year, store_sales.date_dim.qoy, count(store_sales.ticket_number) limit 5;`

  ```text

  ```
- `trilogy run --import raw.catalog_sales:cs select distinct cs.bill_customer.customer_address.country limit 5;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss rowset frequent_items <- select ss.item.item_sk as item_sk, --count(ss.date_dim.date_sk) as date_cnt where ss.date_dim.year between 2000 and 2003 having date_cnt > 4; select count(frequent_items.item_sk);`

  ```text

  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
and store_sales.store.state = 'TN'
… hierarchy_level desc nulls first,
    case when hierarchy_level = 0 then category end nulls first,
    within_parent_rank asc nulls first
limit 100;`

  ```text

  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching the 8 detailed attribute profiles
auto profile_items <- item.item_sk ? …where item.manufact_id between 1 and 500
and item.manufact in shared_manufact
select distinct item.product_name
order by item.product_name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.item:item select count(item.item_id) where item.color in ('slate', 'blanched', 'burnished');`

  ```text

  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.date_dim.dow, store_sales.date_dim.day_name order by store_sales.date_dim.dow limit 10;`

  ```text

  ```
- `trilogy run --import raw.store_sales:ss select ss.date_dim.year, count(ss.ticket_number) limit 5;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select cs.warehouse.warehouse_name, count(cs.order_number) limit 5;`

  ```text

  ```
- `trilogy file write query77.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…turns,
    coalesce(profit_store, profit_cat, profit_web) as profit
order by channel asc nulls first, outlet asc nulls first, returns desc
limit 100;`

  ```text

  ```
- `trilogy file write query77_store.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.store….date between '2000-08-23'::date and '2000-09-22'::date), 0) by rollup store_sales.store.store_sk as profit
order by outlet asc nulls first
limit 10;`

  ```text

  ```
- `trilogy file write query77_store.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.store…p store_sales.store.store_sk as returns,
    profit_by_store by rollup store_sales.store.store_sk as profit
order by outlet asc nulls first
limit 10;`

  ```text

  ```
- `trilogy file write query77_store2.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.stor…s returns,
    sum(store_sales.net_profit) - coalesce(sl, 0) by rollup store_sales.store.store_sk as profit
order by outlet asc nulls first
limit 10;`

  ```text

  ```
- `trilogy run --import raw/store_returns:sr --import raw/date_dim:dd select sr.item.item_sk, count(sr.return_quantity) limit 5;`

  ```text

  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge ss.customer.last_…s.customer.first_name as fname,
    ss.date_dim.date as sdate,
    -- cs side (null if no match)
    cs.bill_customer.last_name as cs_lname
limit 20;`

  ```text

  ```

### `cli-misuse`

- `trilogy run query34.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run --format json query49.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query72.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5;`

  ```text
  'ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5; duckdb`

  ```text
  'ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5; duck_db`

  ```text
  'ss.item.item_sk, ss.customer.customer_sk, ss.store.date_dim.year, sum(ss.quantity) as total_qty limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run - --import raw/store_sales:ss select ss.ticket_number, ss.coupon_amt, ss.net_profit, ss.customer.last_name, ss.customer.first_name limit 5`

  ```text
  'select ss.ticket_number, ss.coupon_amt, ss.net_profit, ss.customer.last_name, ss.customer.first_name limit 5' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query49.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs merge cs.item.item_sk into ~ss.item.item_sk; where ss.date_dim.fy_year = 2001 and ss.da…oy in (1,2,3) and ss.customer.customer_sk in cs.bill_customer.customer_sk select count(ss.quantity) as ss_cnt, count(cs.quantity) as cs_cnt limit 10;`

  ```text

  ```
- `trilogy run query37.preql`

  ```text

  ```
