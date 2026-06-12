# Trilogy failure analysis — 20260612-153927

- Run `20260612-153922_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 249 | failed: 30 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 15 | 50% |
| `syntax-missing-alias` | 7 | 23% |
| `cli-misuse` | 4 | 13% |
| `syntax-parse` | 2 | 7% |
| `join-resolution` | 2 | 7% |

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
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query12.preql`

  ```text

  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy file read query21.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/order.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query22_check7.preql duckdb`

  ```text

  ```
- `trilogy run query22_check9.preql duckdb`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct part.supplier.nation.name, order.customer.nation.name limit 10;`

  ```text

  ```
- `trilogy run --import raw/part:part select distinct part.type limit 50;`

  ```text

  ```
- `trilogy run --import raw/part:part select distinct part.container limit 50;`

  ```text

  ```
- `trilogy run --import raw/part:part select distinct part.type as type_name limit 50;`

  ```text

  ```
- `trilogy run --import raw/part:part select distinct part.container as container_name limit 50;`

  ```text

  ```
- `trilogy run --import raw.nation:n select distinct n.name where n.region.name = 'MIDDLE EAST';`

  ```text

  ```
- `trilogy run --import raw.order:o select distinct o.status as stat limit 10;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy syntax example correlated-exists-via-grouped-counts`

  ```text
  No such command 'syntax'.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.lineitem as lineitem;

where lineitem.order.date >= '1994-01-01'::date
  and lineitem.order.date < '199….name
select
    lineitem.order.customer.nation.name as nation,
    sum(lineitem.revenue) as revenue
group by nation
order by revenue desc
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;

# Lineitems shipped between 1995-09-01 (inclusive) and 1995-10-01 (exclusive)
wh…::date

select
    100.0 * sum(lineitem.revenue ? starts_with(lineitem.part.type, 'PROMO'))
    / nullif(sum(lineitem.revenue), 0) as promo_revenue
;`

  ```text

  ```

### `join-resolution`

- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query22_check10.preql duckdb`

  ```text

  ```
