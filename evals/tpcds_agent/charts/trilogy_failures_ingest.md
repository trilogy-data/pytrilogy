# Trilogy failure analysis — 20260603-134731

- Run `20260603-134730_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 240 | failed: 20 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 7 | 35% |
| `syntax-parse` | 6 | 30% |
| `undefined-concept` | 5 | 25% |
| `join-resolution` | 2 | 10% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq where cs.sold_date.week_seq between 5269 and 5275 having cs.sold_date.year = 2001 order by cs.sold_date.week_seq;`

  ```text

  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.store as s;

where ss.date_dim.date between '2000-08-23…t
    'store channel' as channel,
    concat('store', ss.store.store_id) as entity_id,
    sum(ss.net_profit) as gross_sales
limit 100; query05.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query05.preql duckdb`

  ```text

  ```
- `trilogy run query05.preql duckdb`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```
- `trilogy run query10.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq, cs.s…s cs_total, sum(ws.ext_sales_price) as ws_total, cs_total + ws_total as combined_total group by cs.sold_date.week_seq, cs.sold_date.day_name limit 5;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs select cs.sold_date.year, cs.sold_date.week_seq, sum(cs.ext_sales_price) as total where cs.sold_date.year in (2000,2001,2002) and cs.sold_date.day_name='Sunday' group by cs.sold_date.year, cs.sold_date.week_seq order by cs.sold_date.week_seq limit 20;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sales_by_wd <- coalesce(sum(c…e; select cs.sold_date.week_seq, cs.sold_date.day_name, sales_by_wd where cs.sold_date.week_seq between 5270 and 5280 order by cs.sold_date.week_seq;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; auto sun_total <- coalesce(sum(cs.…old_date.week_seq; select cs.sold_date.week_seq, sun_total where cs.sold_date.week_seq between 5270 and 5323 order by cs.sold_date.week_seq limit 60;`

  ```text

  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq, min(…yr, max(cs.sold_date.year) as max_yr where cs.sold_date.week_seq between 5269 and 5271 group by cs.sold_date.week_seq order by cs.sold_date.week_seq;`

  ```text

  ```
- `trilogy file write query04.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw/date_dim select date_dim.year, date_dim.week_seq where date_dim.year in (2000,2001,2002) and date_dim.day_name = 'Sunday' order by date_dim.week_seq limit 20;`

  ```text

  ```
- `trilogy run query03.preql`

  ```text

  ```
- `trilogy run query03.preql`

  ```text

  ```
- `trilogy run query06.preql`

  ```text

  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text

  ```

### `join-resolution`

- `trilogy run query05.preql duckdb`

  ```text

  ```
- `trilogy run query05.preql duckdb`

  ```text

  ```
