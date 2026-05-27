# Trilogy failure analysis — 20260527-025802

- Run `20260527-025802` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 8 | failed: 2 (25%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 1 | 50% |
| `join-resolution` | 1 | 50% |

## Detail

### `other`

- `trilogy run query13.preql`

  ```text
  Unable to import '.\orders.preql': [Errno 2] No such file or
  directory: '.\\orders.preql'. Did you mean: raw.orders?
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\part.preql': [Errno 2] No such file or
  directory: '.\\part.preql'. Did you mean: raw.part?
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query21.preql`

  ```text
  'bool' object has no attribute 'row_arguments'
  ```

### `syntax-missing-alias`

- `trilogy run --debug where lineitem.orders.customer.nation.region.name = 'ASIA' and lineitem.orders.orderdate >= '1994-01-01'::date and lineitem.orders.orderd….name as nation_name, sum(lineitem.extendedprice * (1 - lineitem.discount)) as revenue order by revenue desc limit 10; --import raw/lineitem:lineitem`

  ```text
  Input 'where lineitem.orders.customer.nation.region.name = 'ASIA' and
  lineitem.orders.orderdate >= '1994-01-01'::date and lineitem.orders.orderdate <
  '1995-01-01'::date and lineitem.orders.customer.nation.nationkey =
  lineitem.supplier.nation.nationkey select lineitem.orders.customer.nation.name
  as nation_name, sum(lineitem.extendedprice * (1 - lineitem.discount)) as
  revenue order by revenue desc limit 10; --import raw/lineitem:lineitem' does
  not exist.
  ```
- `trilogy run --debug debug_check8.preql`

  ```text
  Input 'debug_check8.preql' does not exist.
  ```
- `trilogy run --import raw/partsupp:partsupp where partsupp.supplier.nation.name = 'GERMANY' select partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost * partsupp.availqty) by () order by total_value desc limit 1000;`

  ```text
  Input 'where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;' does not exist.
  ```
- `trilogy run - --import raw/partsupp:partsupp`

  ```text
  Input 'where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;' does not exist.
  ```
- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  Input 'where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;' does not exist.
  ```
- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  Input 'where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;' does not exist.
  ```
- `trilogy run - --import raw/partsupp:partsupp`

  ```text
  Input 'where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.0001 / 100.0 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;' does not exist.
  ```

### `syntax-parse`

- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  --> 2:1
    |
  2 | > echo;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ort raw.partsupp as partsupp; ??? > echo;
  ```
- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  --> 2:1
    |
  2 | > echo;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ort raw.partsupp as partsupp; ??? > echo;
  ```
- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  --> 2:1
    |
  2 | > echo;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ort raw.partsupp as partsupp; ??? > echo;
  ```
- `trilogy run --import raw/partsupp:partsupp -`

  ```text
  --> 2:220
    |
  2 | where partsupp.supplier.nation.name = 'GERMANY' select
  partsupp.part.partkey, sum(partsupp.supplycost * partsupp.availqty) as
  total_value having total_value > 0.000001 * sum(partsupp.supplycost *
  partsupp.availqty) by () order by total_value desc limit 1000;
    |
  ^---
    |
    = expected expr_over_list
  Location:
  ...cost * partsupp.availqty) by ( ??? ) order by total_value desc li...
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "line: 5: Undefined concept:
  supplier.acctbal. Suggestions: ['partsupp.supplier.acctbal']")
  ```
- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  lineitem.orderkey. Suggestions: ['lineitem.orders.orderkey',
  'lineitem.orders.clerk', 'lineitem.orders.comment']")
  ```

### `join-resolution`

- `trilogy run query13.preql`

  ```text
  Could not resolve connections for query with output
  ['local.order_count<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
