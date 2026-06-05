# Trilogy failure analysis — 20260605-033154

- Run `20260605-033153_enriched` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 282 | failed: 24 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 8 | 33% |
| `other` | 5 | 21% |
| `undefined-concept` | 4 | 17% |
| `syntax-missing-alias` | 4 | 17% |
| `cli-misuse` | 2 | 8% |
| `join-resolution` | 1 | 4% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.lineitem:li select year(li.order.date) as year, count(li.line_no) as cnt where li.part.type = 'ECONOMY ANODIZED STEEL' and li.order.date between '1995-01-01'::date and '1996-12-31'::date group by year order by year;`

  ```text

  ```
- `trilogy run --import raw.lineitem:li select year(li.order.date) as year, count(li.line_no) as cnt where li.order.customer.nation.region.name = 'AMERICA' and li.part.type = 'ECONOMY ANODIZED STEEL' and li.order.date between '1995-01-01'::date and '1996-12-31'::date group by year order by year;`

  ```text

  ```
- `trilogy file write query13.preql --content import raw.order as order;

# Count orders per customer where comment does NOT match %special%requests%
auto order…  orders_not_matching as bucket,
    count(order.customer.id) as customer_count
order by
    customer_count desc,
    bucket desc
limit 31
offset 30;`

  ```text

  ```
- `trilogy file write query14.preql --content import raw.lineitem as lineitem;

# Lineitems shipped between 1995-09-01 (inclusive) and 1995-10-01 (exclusive)
wh…-10-01'::date

select
  100.0 * sum(lineitem.revenue ? starts_with(lineitem.part.type, 'PROMO'))
  / sum(lineitem.revenue) as promo_revenue
limit 10;`

  ```text

  ```
- `trilogy file write query21.preql`

  ```text

  ```
- `trilogy file write query21.preql`

  ```text

  ```
- `trilogy file write debug10.preql`

  ```text

  ```
- `trilogy run --import raw.customer:customer select substring(customer.phone, 1, 2) as area, count(customer.id) as cnt, avg(customer.account_balance) as avg_bal where customer.account_balance > 0 group by area order by area limit 20;`

  ```text

  ```

### `other`

- `trilogy run query01.preql`

  ```text

  ```
- `trilogy run query02.preql`

  ```text

  ```
- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run debug6.preql`

  ```text

  ```
- `trilogy database describe customer`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```

### `undefined-concept`

- `trilogy run query03.preql`

  ```text

  ```
- `trilogy run query05.preql`

  ```text

  ```
- `trilogy run query07.preql`

  ```text

  ```
- `trilogy run query22.preql`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.lineitem:li select count(li.line_no) where li.order.customer.nation.region.name = 'AMERICA' and li.part.type = 'ECONOMY ANODIZED STEEL' and li.order.date between '1995-01-01'::date and '1996-12-31'::date;`

  ```text

  ```
- `trilogy run --import raw.lineitem:li select distinct li.part.supplier.nation.name as nation_name where li.order.customer.nation.region.name = 'AMERICA' and li.part.type = 'ECONOMY ANODIZED STEEL' and li.order.date between '1995-01-01'::date and '1996-12-31'::date order by nation_name;`

  ```text

  ```
- `trilogy run --import raw.lineitem:li select distinct li.order.customer.nation.name as cust_nation, sum(li.revenue) as rev where li.order.customer.nation.regi…ame = 'AMERICA' and li.part.type = 'ECONOMY ANODIZED STEEL' and li.order.date between '1995-01-01'::date and '1996-12-31'::date order by cust_nation;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select lineitem.part.brand, lineitem.part.container, count(lineitem.line_no) where lineitem.part.brand like '%23%' limit 20;`

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

### `join-resolution`

- `trilogy run query17.preql`

  ```text

  ```
