# Trilogy failure analysis — 20260605-121151

- Run `20260605-121150_enriched` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 235 | failed: 15 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-missing-alias` | 5 | 33% |
| `other` | 4 | 27% |
| `cli-misuse` | 2 | 13% |
| `undefined-concept` | 2 | 13% |
| `syntax-parse` | 1 | 7% |
| `join-resolution` | 1 | 7% |

## Detail

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.type limit 20;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.order.customer.nation.region.name limit 10;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.supplier.nation.name limit 10;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.order.customer.nation.name limit 20;`

  ```text

  ```
- `trilogy file write query15.preql --content import raw.lineitem as li;

# revenue per supplier: sum of lineitem.revenue (extended_price * (1 - discount)) ship…rt.supplier.address,
    li.part.supplier.phone,
    supplier_revenue
to rank_filtered
where rev_rank = 1
order by li.part.supplier.id asc
limit 100;`

  ```text

  ```

### `other`

- `trilogy run query02.preql duckdb`

  ```text

  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `cli-misuse`

- `trilogy explore raw/partsupp.preql`

  ```text
  Invalid value for 'PATH': File 'raw/partsupp.preql' does not exist.
  ```
- `trilogy run query18.preql --format rich`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `undefined-concept`

- `trilogy run query09.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write test17.preql --content import raw.lineitem as lineitem;

auto part_avg_qty <- avg(lineitem.quantity) by lineitem.part.id;

# First, check …neitem.quantity) as raw_avg, part_avg_qty
where lineitem.part.id = 1
group by lineitem.part.id, lineitem.part.brand, lineitem.part.container
limit 5;`

  ```text

  ```

### `join-resolution`

- `trilogy run --import raw.supplier:supplier --import raw.nation:nation select supplier.name, supplier.id, nation.name where nation.name='SAUDI ARABIA';`

  ```text

  ```
