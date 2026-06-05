# Trilogy failure analysis — 20260605-005207

- Run `20260605-005205_ingest` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 272 | failed: 23 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 11 | 48% |
| `syntax-parse` | 3 | 13% |
| `undefined-concept` | 3 | 13% |
| `join-resolution` | 2 | 9% |
| `syntax-missing-alias` | 2 | 9% |
| `type-error` | 1 | 4% |
| `file-not-found` | 1 | 4% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

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
- `trilogy run query15.preql`

  ```text

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

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.partsupp as partsupp;

where part.
  partsupp.supplier.nation.region.name = 'EUROPE'
  and part.size = …ne as supplier_phone,
  partsupp.supplier.comment as supplier_comment
order by supplier_acctbal desc, nation_name, supplier_name, part_key
limit 100;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;

# Filter to lineitems shipped between 1995-09-01 (inclusive) and 1995-10-01 (exc…count) ? starts_with(lineitem.part.type, 'PROMO'))
    / nullif(sum(lineitem.extendedprice * (1 - lineitem.discount)), 0) as promo_revenue
limit 10;
`

  ```text

  ```
- `trilogy file write query15.preql --content import raw.lineitem as lineitem;

# compute revenue per lineitem
auto revenue <- lineitem.extendedprice * (1 - lin…>= '1996-01-01'::date
  and lineitem.shipdate < '1996-04-01'::date
  and supplier_revenue = (select max(supplier_revenue))
order by supplier_key asc;`

  ```text

  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run --import raw.partsupp:partsupp select partsupp.part.partkey, part.size, part.type where partsupp.part.size = 15 and partsupp.part.type like '%BRASS' and partsupp.supplier.nation.region.name = 'EUROPE';`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```

### `join-resolution`

- `trilogy run query04.preql`

  ```text

  ```
- `trilogy run query13.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.part:part select distinct part.type limit 20;`

  ```text

  ```
- `trilogy run --import raw.part:part select distinct part.type as ptype limit 20;`

  ```text

  ```

### `type-error`

- `trilogy run query02.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run query22.preql`

  ```text

  ```
