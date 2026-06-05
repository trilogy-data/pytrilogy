# Trilogy failure analysis — 20260605-130243

- Run `20260605-130243_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 174 | failed: 8 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `join-resolution` | 4 | 50% |
| `cli-misuse` | 1 | 12% |
| `other` | 1 | 12% |
| `syntax-parse` | 1 | 12% |
| `syntax-missing-alias` | 1 | 12% |

## Detail

### `join-resolution`

- `trilogy run query13.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query18.preql`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/partsupp.preql`

  ```text
  Invalid value for 'PATH': File 'raw/partsupp.preql' does not exist.
  ```

### `other`

- `trilogy run query11.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query15.preql --content import raw.lineitem as lineitem;
import raw.supplier as supplier;

# Filter to shipped between 1996-01-01 (inclusi…
    lineitem.part.supplier.address as address,
    lineitem.part.supplier.phone as phone,
    supplier_revenue as revenue
order by supplier_key asc;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.supplier.nation.name, lineitem.part.supplier.nation.region.name;`

  ```text

  ```
