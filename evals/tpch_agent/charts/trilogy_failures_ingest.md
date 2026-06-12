# Trilogy failure analysis — 20260612-153927

- Run `20260612-153922_ingest` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 290 | failed: 36 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 23 | 64% |
| `syntax-parse` | 7 | 19% |
| `syntax-missing-alias` | 2 | 6% |
| `undefined-concept` | 2 | 6% |
| `cli-misuse` | 1 | 3% |
| `join-resolution` | 1 | 3% |

## Detail

### `other`

- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/lineitem.preql duckdb --import raw.lineitem:lineitem select distinct part.type limit 20;`

  ```text

  ```
- `trilogy run raw/nation.preql duckdb --import raw.nation:nation select nation.name;`

  ```text

  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/lineitem.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/partsupp.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query12.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query15.preql duckdb`

  ```text

  ```
- `trilogy run query15.preql duckdb`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy file read query20.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql -e -c import raw.partsupp as ps;

where ps.supplier.nation.region.name = 'EUROPE'
  and ps.part.size = 15
  and ps.part.type…by ps.supplycost) as cost_rank
having --cost_rank = 1
order by
    supplier_acctbal desc,
    nation_name,
    supplier_name,
    part_key
limit 100;`

  ```text

  ```
- `trilogy run --import raw.partsupp:ps select ps.part.partkey, count(ps.supplier.suppkey) as num_suppliers, min(ps.supplycost) as min_cost where ps.supplier.nation.region.name = 'EUROPE' and ps.part.size = 15 and ps.part.type like '%BRASS' group by ps.part.partkey having num_suppliers > 1 limit 10;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;
import raw.part as part;

where lineitem.shipdate >= '1995-09-01'::date
  and lin…dprice * (1 - lineitem.discount) ? starts_with(part.type, 'PROMO')) / sum(lineitem.extendedprice * (1 - lineitem.discount)) as promo_revenue
limit 1;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;
import raw.part as part;

where lineitem.shipdate >= '1995-09-01'::date
  and lin…price * (1 - lineitem.discount)) ? starts_with(part.type, 'PROMO')) / sum(lineitem.extendedprice * (1 - lineitem.discount)) as promo_revenue
limit 1;`

  ```text

  ```
- `trilogy file write query15.preql --content import raw.lineitem as lineitem;

# Revenue per supplier = sum of extendedprice * (1 - discount)
# Filter: shipped…plier.phone as s_phone,
    supplier_revenue as total_revenue,
    --max_revenue
having
    total_revenue = max_revenue
order by
    s_suppkey asc
;
`

  ```text

  ```
- `trilogy file write query20.preql --content import raw.partsupp as ps;
import raw.lineitem as li;

# Oversupplied (part, supplier) pairs: availqty > 0.5 * tot…ier_name,
    oversupplied.s_address as supplier_address
group by
    oversupplied.s_name,
    oversupplied.s_address
order by
    supplier_name asc;`

  ```text

  ```
- `trilogy run --import raw.partsupp:ps --import raw.lineitem:li select
    ps.supplier.name as name,
    ps.part.name as part,
    ps.availqty as avail,
    su… ps.part.name ilike 'forest%'
left join ps.supplier.suppkey = li.supplier.suppkey
left join ps.part.partkey = li.part.partkey
order by name
limit 50;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct part.type limit 30; duckdb`

  ```text

  ```
- `trilogy run --import raw.part:part select part.type limit 15 offset 15; duckdb`

  ```text

  ```

### `undefined-concept`

- `trilogy run query14.preql`

  ```text

  ```
- `trilogy run query19.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy run --format json query13.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text

  ```
