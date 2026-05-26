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

### `syntax-parse`

- `trilogy run -`

  ```text
  --> 2:46
    |
  2 | where lineitem.shipdate <= '1998-09-02'::date;
    |                                              ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...shipdate <= '1998-09-02'::date ??? ; select lineitem.returnflag,
  ```
- `trilogy run - --import raw/orders:orders --import raw/lineitem:lineitem`

  ```text
  --> 3:5
    |
  3 | with;
    |     ^---
    |
    = expected IDENTIFIER
  Location:
  ...raw.lineitem as lineitem; with ??? ;
  ```
- `trilogy run query16.preql`

  ```text
  --> 4:22
    |
  4 |   and not starts_with(ps.part.type, 'MEDIUM POLISHED')
    |                      ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...rand#45'   and not starts_with ??? (ps.part.type, 'MEDIUM POLISHE...
  ```
- `trilogy run --import raw/customer:customer select substring(customer.phone, 1, 2) as cnt from customer order by cnt limit 5;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...(customer.phone, 1, 2) as cnt ??? from customer order by cnt lim...
  ```
- `trilogy run query22.preql`

  ```text
  --> 9:165
    |
  9 |   and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and
  substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17'))
  by substring(customer.phone, 1, 2);
    |
  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...30', '18', '17')) by substring ??? (customer.phone, 1, 2);  selec...
  ```
- `trilogy run --import raw/customer:customer select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as …customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') group by cntrycode order by cntrycode limit 5;`

  ```text
  --> 2:235
    |
  2 | select substring(customer.phone, 1, 2) as cntrycode,
  count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal where
  customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23',
  '29', '30', '18', '17') group by cntrycode order by cntrycode limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...'23', '29', '30', '18', '17') ??? group by cntrycode order by cn...
  ```
- `trilogy run query22.preql`

  ```text
  --> 9:165
    |
  9 |   and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and
  substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17'))
  by substring(customer.phone, 1, 2);
    |
  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...30', '18', '17')) by substring ??? (customer.phone, 1, 2);  selec...
  ```
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; where customer.acctbal > 0 and su…, 2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;`

  ```text
  --> 3:354
    |
  3 | merge customer.custkey into ~orders.customer.custkey; where
  customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23',
  '29', '30', '18', '17') and orders.orderkey is null and customer.acctbal >
  avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phone, 1, 2)
  in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1,
  2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey)
  as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;
    |
  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...30', '18', '17')) by substring ??? (customer.phone, 1, 2) select
  ```
- `trilogy run --import raw/customer:customer --import raw/orders:orders -- merge customer.custkey into ~orders.customer.custkey; where customer.acctbal > 0 and…, 2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;`

  ```text
  --> 3:354
    |
  3 | merge customer.custkey into ~orders.customer.custkey; where
  customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23',
  '29', '30', '18', '17') and orders.orderkey is null and customer.acctbal >
  avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phone, 1, 2)
  in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1,
  2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey)
  as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;
    |
  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...30', '18', '17')) by substring ??? (customer.phone, 1, 2) select
  ```
- `trilogy run test_merge.preql`

  ```text
  --> 8:30
    |
  8 |   and orders.orderkey is null;
    |                              ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
     and orders.orderkey is null ??? ;  select     count(customer.c...
  ```
- `trilogy run test_merge.preql`

  ```text
  --> 8:30
    |
  8 |   and orders.orderkey is null;
    |                              ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
     and orders.orderkey is null ??? ;  select     count(customer.c...
  ```
- `trilogy run --import raw/customer:customer select count(customer.custkey) as c, sum(customer.acctbal) as t, substring(customer.phone, 1, 2) as cntrycode wher…, '30', '18', '17') and customer.custkey not in (merge customer.custkey into orders.customer.custkey) select orders.customer.custkey from raw.orders;`

  ```text
  --> 2:255
    |
  2 | select count(customer.custkey) as c, sum(customer.acctbal) as t,
  substring(customer.phone, 1, 2) as cntrycode where customer.acctbal > 0 and
  substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
  and customer.custkey not in (merge customer.custkey into
  orders.customer.custkey) select orders.customer.custkey from raw.orders;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ustomer.custkey not in (merge ??? customer.custkey into orders.c...
  ```

### `other`

- `trilogy run -`

  ```text
  Unable to import
  'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpch_agent\results\20260526
  -032601\workspace\_worker_3\lineitem.preql': [Errno 2] No such file or
  directory:
  'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpch_agent\\results\\
  20260526-032601\\workspace\\_worker_3\\lineitem.preql'. Did you mean:
  raw.lineitem?
  ```
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
