# Trilogy failure analysis — 20260526-130241

- Run `20260526-130241` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 241 | failed: 25 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 7 | 28% |
| `file-not-found` | 7 | 28% |
| `syntax-parse` | 4 | 16% |
| `undefined-concept` | 2 | 8% |
| `join-resolution` | 2 | 8% |
| `cli-misuse` | 2 | 8% |
| `syntax-missing-alias` | 1 | 4% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy run query02.preql`

  ```text
  HAVING references 'partsupp.supplycost', which is not in the
  SELECT projection (line 4). Fix one of: (a) add it to SELECT — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --partsupp.supplycost`;
  (b) move the filter to WHERE — for an aggregate condition on a non-output
  grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query18.preql`

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

### `file-not-found`

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

- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.nation<Purpose.PROPERTY>Derivation.BASIC>',
  'local.o_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.profit<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/part:part --import raw/lineitem:lineitem select part.brand, part.container, part.size, lineitem.quantity, lineitem.shipmode, lineitem.shipinstruct, lineitem.extendedprice, lineitem.discount, lineitem.linenumber limit 1;`

  ```text
  Could not resolve connections for query with output
  ['part.brand<Purpose.PROPERTY>Derivation.ROOT>',
  'part.container<Purpose.PROPERTY>Derivation.ROOT>',
  'part.size<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.shipmode<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.shipinstruct<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.extendedprice<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.discount<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.linenumber<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```

### `cli-misuse`

- `trilogy datexplore raw/partsupp.preql`

  ```text
  No such command 'datexplore'.
  ```
- `trilogy run --import raw/lineitem:lineitem where lineitem.part.brand = 'Brand#12' and lineitem.part.container in ('SM CASE','SM BOX','SM PACK','SM PKG') and …hipmode in ('AIR','AIR REG') and lineitem.shipinstruct = 'DELIVER IN PERSON' select sum(lineitem.extendedprice * (1 - lineitem.discount)) as revenue;`

  ```text
  'select sum(lineitem.extendedprice * (1 - lineitem.discount)) as revenue;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/lineitem:lineitem select lineitem.part.brand, lineitem.part.container, count(lineitem.linenumber) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...r, count(lineitem.linenumber) ??? limit 10;
  ```
