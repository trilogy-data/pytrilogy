# Trilogy failure analysis — 20260605-005207

- Run `20260605-005205_enriched` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 200 | failed: 16 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 6 | 38% |
| `syntax-parse` | 5 | 31% |
| `syntax-missing-alias` | 3 | 19% |
| `cli-misuse` | 1 | 6% |
| `join-resolution` | 1 | 6% |

## Detail

### `other`

- `trilogy run query09.preql`

  ```text

  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run query20.preql`

  ```text

  ```
- `trilogy run --import raw.customer:customer --import raw.order:order select customer.id, count(order.id) as order_count where customer.id in ('13','23','5') and customer.id in order.customer.id order by customer.id;`

  ```text

  ```

### `syntax-parse`

- `trilogy file write query16.preql`

  ```text

  ```
- `trilogy file write query16.preql`

  ```text

  ```
- `trilogy file write query18.preql --content import raw.lineitem as lineitem;

# Sum of quantity per order
auto total_qty_by_order <- sum(lineitem.quantity) by…order.total_price as total_order_price,
    sum(lineitem.quantity) as sum_quantity
order by
    total_order_price desc,
    order_date asc
limit 100;`

  ```text

  ```
- `trilogy file write query22_check.preql --content import raw.customer as customer;
select 
    substring(customer.phone, 1, 2) as cntrycode,
    count(custome…omer.account_balance > 0
    and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
group by cntrycode
order by cntrycode;`

  ```text

  ```
- `trilogy run --import raw.customer:customer --import raw.order:order select customer.id, count(order.id) as order_count where customer.id in ('13','23','5') and customer.id in order.customer.id group by customer.id;`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.supplier.nation.name limit 10;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.supplier.nation.name as sn_name where sn_name = 'BRAZIL' limit 5;`

  ```text

  ```
- `trilogy run --import raw.part:part select part.brand, part.container, count(part.id) limit 20;`

  ```text

  ```

### `cli-misuse`

- `trilogy explore raw/partsupp.preql`

  ```text
  Invalid value for 'PATH': File 'raw/partsupp.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run --import raw.customer:customer --import raw.order:order select customer.id, count(order.id) as order_count where customer.id in (13, 23, 5) and customer.id in order.customer.id order by customer.id;`

  ```text

  ```
