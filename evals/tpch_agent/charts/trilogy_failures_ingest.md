# Trilogy failure analysis — 20260605-130243

- Run `20260605-130243_ingest` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 162 | failed: 7 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 4 | 57% |
| `syntax-parse` | 3 | 43% |

## Detail

### `other`

- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query08.preql`

  ```text

  ```
- `trilogy file write query14.preql`

  ```text

  ```
- `trilogy file write query16.preql -e -c import raw.partsupp as ps;

# Filter conditions from the question
where ps.part.brand != 'Brand#45'
and not starts_wit…size as size,
    count_distinct(ps.supplier.suppkey) as supplier_count

order by
    supplier_count desc,
    brand asc,
    type asc,
    size asc;`

  ```text

  ```
