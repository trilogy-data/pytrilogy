# Trilogy failure analysis — 20260605-121151

- Run `20260605-121150_ingest` | `deepseek/deepseek-chat` | sf=0.01
- `trilogy` calls: 313 | failed: 53 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 28 | 53% |
| `syntax-parse` | 12 | 23% |
| `syntax-missing-alias` | 9 | 17% |
| `join-resolution` | 1 | 2% |
| `undefined-concept` | 1 | 2% |
| `type-error` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

- `trilogy run query11.preql`

  ```text

  ```
- `trilogy run query15.preql`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.orders.orderkey, l.supplier.suppkey, l.supplier.name as sname, max(1 ? l.receiptdate > l.commitdate) by (l.order…g, max(1 ? l.supplier.nation.name = 'SAUDI ARABIA') by (l.orders.orderkey, l.supplier.suppkey) as is_saudi where l.orders.orderstatus = 'F' limit 20;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.orders.orderkey, count_distinct(l.supplier.suppkey) as total_supp, count_distinct(1 ? l.receiptdate > l.commitdate) by l.orders.orderkey as late_supp_count where l.orders.orderstatus = 'F' and l.supplier.suppkey = 74 limit 10;`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy database describe orders`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run --import raw.customer:cust --import raw.orders:ord 
# Area code
auto cntrycode <- substring(cust.phone, 1, 2);
# Target avg
auto target_avg <- av…ust.custkey not in ord.customer.custkey
select
  cntrycode,
  count(cust.custkey) as num_cust,
  sum(cust.acctbal) as totacctbal
order by cntrycode;
`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```
- `trilogy run query22.preql duckdb`

  ```text

  ```

### `syntax-parse`

- `trilogy run --import raw.lineitem:lineitem select lineitem.part.type as part_type, count(lineitem.part.partkey) as cnt where lineitem.orders.orderdate between '1995-01-01'::date and '1996-12-31'::date group by lineitem.part.type limit 50;`

  ```text

  ```
- `trilogy run --import raw.part:part select part.brand, count(part.partkey) as cnt where part.container = 'MED BOX' group by part.brand order by part.brand limit 10;`

  ```text

  ```
- `trilogy file write query21.preql --content import raw.lineitem as l;

# Filter to Saudi Arabia suppliers and orders with status F only
where l.supplier.natio…
  and late_suppliers = 1

select
  l.supplier.name,
  count_distinct(l.orders.orderkey) as numwait
order by numwait desc, l.supplier.name
limit 100;`

  ```text

  ```
- `trilogy file write query21.preql --content import raw.lineitem as l;

# For each (order, supplier) pair: does this supplier have any late lineitem?
auto supp…
  and late_suppliers = 1

select
  l.supplier.name,
  count_distinct(l.orders.orderkey) as numwait
order by numwait desc, l.supplier.name
limit 100;`

  ```text

  ```
- `trilogy run --import raw.lineitem:l select distinct(l.supplier.nation.name) as nation order by nation;`

  ```text

  ```
- `trilogy run --import raw.lineitem:l select l.supplier.nation.name as nation group by nation order by nation limit 25;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.orders.orderkey, l.supplier.suppkey, l.supplier.name, max(1 ? l.receiptdate > l.commitdate) as late_flag, max(1 ….nation.name = 'SAUDI ARABIA') as is_saudi where l.orders.orderstatus = 'F' group by l.orders.orderkey, l.supplier.suppkey, l.supplier.name limit 20;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.supplier.suppkey, l.supplier.name, l.orders.orderkey where l.supplier.nation.name = 'SAUDI ARABIA' and l.orders.orderstatus = 'F' and l.orders.orderkey in (select l2.orders.orderkey where l2.supplier.suppkey != l.supplier.suppkey) limit 20;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.orders.orderkey, l.supplier.suppkey, l.supplier.name as sname, l.linenumber, l.receiptdate, l.commitdate where l…rderkey as ok where l2.supplier.suppkey = 74 and l2.orders.orderstatus = 'F')) order by l.orders.orderkey, l.supplier.suppkey, l.linenumber limit 50;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select count(l.orders.orderkey, l.linenumber) as total_lineitems;`

  ```text

  ```
- `trilogy run --import raw/lineitem:l select l.orders.orderkey, l.supplier.suppkey, l.supplier.nation.name as nat, max(1 ? l.receiptdate > l.commitdate) by (l.…orderkey as orderkey where l2.supplier.nation.name = 'SAUDI ARABIA' and l2.orders.orderstatus = 'F')) order by l.orders.orderkey, l.supplier.suppkey;`

  ```text

  ```
- `trilogy file write query22.preql --content import raw.customer as customer;

# Area code (first two chars of phone)
auto cntrycode <- substring(customer.phon…(select o_custkey from orders)

select
  cntrycode,
  count(customer.custkey) as num_cust,
  sum(customer.acctbal) as totacctbal
order by cntrycode;
`

  ```text

  ```

### `syntax-missing-alias`

- `trilogy run --import raw.part:part select distinct part.type limit 30;`

  ```text

  ```
- `trilogy run --import raw.lineitem:lineitem select distinct lineitem.part.type limit 20;`

  ```text

  ```
- `trilogy run --import raw.part:part select distinct part.brand limit 20;`

  ```text

  ```
- `trilogy run --import raw.part:part select distinct part.container limit 20;`

  ```text

  ```
- `trilogy run --import raw.part:part select count(part.partkey) where part.brand = 'Brand#23' and part.container = 'MED BOX';`

  ```text

  ```
- `trilogy run --import raw.lineitem:l select distinct l.supplier.nation.name order by l.supplier.nation.name;`

  ```text

  ```
- `trilogy file write query22.preql --content import raw.customer as customer;
import raw.orders as orders;

# Area code (first two chars of phone)
auto cntryco…t in custs_with_orders.custkey

select
  cntrycode,
  count(customer.custkey) as num_cust,
  sum(customer.acctbal) as totacctbal
order by cntrycode;
`

  ```text

  ```
- `trilogy file write query22.preql --content import raw.customer as customer;
import raw.orders as orders;

# Area code (first two chars of phone)
auto cntryco…t in custs_with_orders.custkey

select
  cntrycode,
  count(customer.custkey) as num_cust,
  sum(customer.acctbal) as totacctbal
order by cntrycode;
`

  ```text

  ```
- `trilogy run --import raw.customer:cust --import raw.orders:ord select count(cust.custkey) where cust.custkey not in ord.customer.custkey limit 5;`

  ```text

  ```

### `join-resolution`

- `trilogy run query20.preql`

  ```text

  ```

### `undefined-concept`

- `trilogy run --import raw/supplier.preql select supplier.nation.name as nation where supplier.nation.name like '%SAU%' or supplier.nation.name like '%IRAN%' or supplier.nation.name like '%ARAB%' limit 20;`

  ```text

  ```

### `type-error`

- `trilogy run query21.preql`

  ```text

  ```

### `file-not-found`

- `trilogy run --import raw.customer:cust --import raw.orders:ord 
auto target_avg <- avg(cust.acctbal ? cust.acctbal > 0 and (substring(cust.phone, 1, 2) = '13…
select count(cust.custkey) as cnt where cust.custkey not in ord.customer.custkey and substring(cust.phone,1,2) = '13' and cust.acctbal > target_avg;`

  ```text

  ```
