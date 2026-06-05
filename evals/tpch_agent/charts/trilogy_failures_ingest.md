# Trilogy failure analysis — 20260605-033154

- Run `20260605-033153_ingest` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 253 | failed: 20 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 8 | 40% |
| `syntax-missing-alias` | 7 | 35% |
| `other` | 3 | 15% |
| `join-resolution` | 1 | 5% |
| `undefined-concept` | 1 | 5% |

## Detail

### `syntax-parse`

- `trilogy file write query08.preql`

  ```text

  ```
- `trilogy file write query11.preql --content import raw.partsupp as partsupp;

# Grand total of value across ALL Germany partsupp rows
auto grand_total <- sum(…

select
    partsupp.part.partkey as part_key,
    value_by_part as total_value
having total_value > 0.0001 * grand_total
order by total_value desc;`

  ```text

  ```
- `trilogy file write query11.preql --content import raw.partsupp as partsupp;

# Grand total value across ALL GERMANY partsupp rows (single scalar)
auto grand_…rt.partkey as part_key,
    sum(partsupp.supplycost * partsupp.availqty) as total_value
total_value > 0.0001 * grand_total
order by total_value desc;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;

# Qualifying lineitems: shipped between 1995-09-01 (inclusive) and 1995-10-01 (e…e * (1 - lineitem.discount) ? starts_with(lineitem.part.type, 'PROMO'))
  / sum(lineitem.extendedprice * (1 - lineitem.discount))
  as promo_revenue;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select lineitem.part.brand, lineitem.part.container, count(lineitem.linenumber) as cnt where lineitem.part.brand = 'Brand#23' group all order by cnt desc limit 20;`

  ```text

  ```
- `trilogy file write query21.preql`

  ```text

  ```
- `trilogy file write query21_check5.preql`

  ```text

  ```
- `trilogy run --import raw.customer:customer select substring(customer.phone, 1, 2) as phone_prefix, count(customer.custkey) as cnt where substring(customer.phone, 1, 2) in ('13','31','23','29','30','18','17') group by phone_prefix order by phone_prefix;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.type limit 20;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.orders.customer.nation.name limit 30;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.supplier.nation.name limit 30;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select lineitem.part.brand, lineitem.part.container, count(lineitem.linenumber) where lineitem.part.brand = 'Brand#23' and lineitem.part.container = 'MED BOX';`

  ```text

  ```
- `trilogy run --import raw.customer:customer select count(customer.custkey);`

  ```text

  ```
- `trilogy run --import raw.customer:customer select count(customer.custkey ? customer.acctbal > 0);`

  ```text

  ```
- `trilogy run --import raw.customer:customer select customer.custkey, customer.phone, customer.acctbal ? substring(customer.phone, 1, 2) in ('13','31','23','29','30','18','17') limit 10;`

  ```text

  ```

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

### `join-resolution`

- `trilogy run query16.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run query22.preql`

  ```text

  ```
