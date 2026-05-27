# Trilogy failure analysis — 20260527-151651

- Run `20260527-151651` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 340 | failed: 40 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 14 | 35% |
| `syntax-parse` | 8 | 20% |
| `cli-misuse` | 6 | 15% |
| `join-resolution` | 5 | 12% |
| `undefined-concept` | 3 | 8% |
| `syntax-missing-alias` | 3 | 8% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy file write query03.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query03.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query04.preql`

  ```text
  Unable to import '.\orders.preql': [Errno 2] No such file or
  directory: '.\\orders.preql'. Did you mean: raw.orders?
  ```
- `trilogy run query05.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query05.preql duckdb`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 40 (char 39). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  HAVING references 'local.grand_total', which is not in the
  SELECT projection (line 12). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ..., --local.grand_total`;
  (b) move the filter to WHERE — for an aggregate condition on a non-output
  grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query11.preql`

  ```text
  HAVING references 'local.grand_total', which is not in the
  SELECT projection (line 14). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ..., --local.grand_total`;
  (b) move the filter to WHERE — for an aggregate condition on a non-output
  grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 309 (char 308). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query15.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query18.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```

### `syntax-parse`

- `trilogy file write query04.preql -e -c import orders as orders;
import lineitem as lineitem;

# Orders placed between 1993-07-01 (inclusive) and 1993-10-01 (…rkey ? exists(lineitem.linenumber ? lineitem.commitdate < lineitem.receiptdate) by orders.orderkey) as order_count
order by orders.orderpriority asc;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '(lineitem.linenumber ?
  lineitem.commitdate < lineitem.receiptdate) by orders.orderkey) as
  order_count\norder by orders.orderpriority asc;') at line 13, column 35.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'exists')]

  Location:
  ...count(orders.orderkey ? exists ??? (lineitem.linenumber ? lineite...

  Write stats: received 657 chars / 657 bytes; tail: …'.orderkey) as
  order_count\\norder by orders.orderpriority asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.lineitem as lineitem;

# Revenue = sum of extendedprice * (1 - discount) for qualifying lineitems
# Fil…en 0.05 and 0.07
  and lineitem.quantity < 24
select
    sum(lineitem.extendedprice * (1 - lineitem.discount)) as revenue
to control_user on success;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'to ') at line 12, column 1.
  Expected one of:
          * HAVING
          * WHERE
          * LIMIT
          * METADATA
          * ORDER
          * MERGE
          * _TERMINATOR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'revenue')]

  Location:
  ...ineitem.discount)) as revenue ??? to control_user on success;

  Write stats: received 526 chars / 526 bytes; tail: …'- lineitem.discount)) as
  revenue\\nto control_user on success;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/lineitem:lineitem select lineitem.part.type as pt, count(lineitem.orders.orderkey) as cnt where lineitem.orders.orderdate between '1995-01-01'::date and '1996-12-31'::date group by pt order by cnt desc limit 20;`

  ```text
  --> 2:155
    |
  2 | select lineitem.part.type as pt, count(lineitem.orders.orderkey) as cnt
  where lineitem.orders.orderdate between '1995-01-01'::date and
  '1996-12-31'::date group by pt order by cnt desc limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...::date and '1996-12-31'::date ??? group by pt order by cnt desc
  ```
- `trilogy file write query15.preql --content import lineitem as lineitem;

# Compute revenue = sum of extendedprice * (1 - discount) for each supplier
# on lin…e as supplier_name,
    lineitem.supplier.address,
    lineitem.supplier.phone,
    revenue
order by supplier_key asc;
having revenue = max(revenue);`

  ```text
  …
         * UNIQUE
          * ROWSET
          * SELF_IMPORT
          * _DEF_TABLE
          * DATASOURCE_ROOT
          * PARAMETER
          * WITH
          * MOCK
          * CREATE
          * RAW_SQL
          * PURPOSE
          * $END
          * SHORTHAND_MODIFIER
          * PERSIST_MODE
          * PROPERTY
          * SHOW
          * PUBLISH_ACTION
          * DATASOURCE_PARTIAL
          * FROM
          * "merge"i
          * SELECT
          * COPY
          * _PROPERTIES
          * VALIDATE
          * PARAM
          * DEF
          * AUTO
          * DATASOURCE
          * CHART
  Previous tokens: [Token('LINE_SEPARATOR', '\n')]

  Location:
  ...ue order by supplier_key asc; ??? having revenue = max(revenue);...

  Write stats: received 651 chars / 651 bytes; tail: …'ue\\norder by supplier_key
  asc;\\nhaving revenue = max(revenue);'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/part:part select * from part where brand = 'Brand#23' limit 20;`

  ```text
  --> 2:8
    |
  2 | select * from part where brand = 'Brand#23' limit 20;
    |        ^---
    |
    = expected select_item
  Location:
  ...port raw.part as part; select ??? * from part where brand = 'Bra...
  ```
- `trilogy run --import raw/part:part select part.brand, part.container from part where brand = 'Brand#23' limit 20;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ct part.brand, part.container ??? from part where brand = 'Brand...
  ```
- `trilogy run --import raw/part:part select part.container, count(part.partkey) as cnt where part.brand = 'Brand#23' group by part.container limit 30;`

  ```text
  --> 2:81
    |
  2 | select part.container, count(part.partkey) as cnt where part.brand =
  'Brand#23' group by part.container limit 30;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...where part.brand = 'Brand#23' ??? group by part.container limit
  ```
- `trilogy file write query22.preql --content import raw.orders as orders;
import raw.customer as customer;

# Customers with phone area code (first 2 chars) in…d substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1, 2)
order by cntrycode asc
limit 100;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...23', '29', '30', '18', '17')) ??? by substring(customer.phone, 1...

  Write stats: received 973 chars / 973 bytes; tail: …'ring(customer.phone, 1,
  2)\\norder by cntrycode asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `cli-misuse`

- `trilogy run --import raw/lineitem:li --import raw/partsupp:ps merge ps.supplycost into ~li.supplycost; select li.supplier.nation.name, year(li.orders.orderdate) as yr, sum(li.extendedprice * (1 - li.discount) - li.supplycost * li.quantity) as profit limit 5;`

  ```text
  'select li.supplier.nation.name, year(li.orders.orderdate) as yr, sum(li.extendedprice * (1 - li.discount) - li.supplycost * li.quantity) as profit limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/lineitem:li --import raw/partsupp:ps select li.supplier.nation.name, year(li.orders.orderdate), sum(li.extendedprice * (1 - li.discount) - ps.supplycost * li.quantity) as profit where li.part.name like '%green%' limit 5;`

  ```text
  'li.supplier.nation.name,' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy --import raw/lineitem.preql:lineitem select lineitem.shipinstruct, lineitem.orders.orderpriority limit 3;`

  ```text
  No such option: --import
  ```
- `trilogy list_files`

  ```text
  No such command 'list_files'.
  ```
- `trilogy list`

  ```text
  No such command 'list'.
  ```
- `trilogy read_file raw/lineitem.preql`

  ```text
  No such command 'read_file'.
  ```

### `join-resolution`

- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['orders.orderpriority<Purpose.PROPERTY>Derivation.ROOT>',
  'local.order_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/lineitem:lineitem --import raw/partsupp:partsupp select lineitem.part.partkey, lineitem.supplier.suppkey, partsupp.supplycost limit 5;`

  ```text
  Could not resolve connections for query with output
  ['lineitem.part.partkey<Purpose.KEY>Derivation.ROOT>',
  'lineitem.supplier.suppkey<Purpose.KEY>Derivation.ROOT>',
  'partsupp.supplycost<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw/lineitem:li --import raw/partsupp:ps select li.part.partkey, li.supplier.suppkey, li.part.name, ps.supplycost limit 5;`

  ```text
  Could not resolve connections for query with output
  ['li.part.partkey<Purpose.KEY>Derivation.ROOT>',
  'li.supplier.suppkey<Purpose.KEY>Derivation.ROOT>',
  'li.part.name<Purpose.PROPERTY>Derivation.ROOT>',
  'ps.supplycost<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.supplier_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.numwait<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.supplier_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.numwait<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `undefined-concept`

- `trilogy run --import raw/lineitem:lineitem select year(orders.orderdate) as yr, sum(extendedprice * (1 - discount) ? supplier.nation.name = 'BRAZIL') / sum(e…tomer.nation.region.name = 'AMERICA' and part.type = 'ECONOMY ANODIZED STEEL' and orders.orderdate between '1995-01-01'::date and '1996-12-31'::date;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  orders.orderdate. Suggestions: ['lineitem.orders.orderdate',
  'lineitem.orders.orderstatus', 'lineitem.orders.orderkey']")
  ```
- `trilogy run query09.preql`

  ```text
  (UndefinedConceptException(...), "line: 6: Undefined concept:
  li.supplycost. Suggestions: ['ps.supplycost', 'li.supplier.comment',
  'li.supplier.acctbal']")
  ```
- `trilogy run query21.preql`

  ```text
  (UndefinedConceptException(...), "line: 29: Undefined
  concept: orders.orderkey. Suggestions: ['lineitem.orders.orderkey',
  'lineitem.orders.orderdate']")
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/lineitem:lineitem select distinct lineitem.part.type limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   as lineitem; select distinct ??? lineitem.part.type limit 20;
  ```
- `trilogy run --import raw/lineitem.preql:lineitem select distinct lineitem.shipinstruct;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   as lineitem; select distinct ??? lineitem.shipinstruct;
  ```
- `trilogy run --import raw/lineitem.preql:lineitem select lineitem.shipinstruct in ('MAIL', 'SHIP') and lineitem.commitdate < lineitem.receiptdate and lineitem…tdate and lineitem.receiptdate >= '1994-01-01'::date and lineitem.receiptdate < '1995-01-01'::date, lineitem.shipinstruct, lineitem.shipmode limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...pinstruct in ('MAIL', 'SHIP') ??? and lineitem.commitdate < line...
  ```

### `file-not-found`

- `trilogy run query22.preql`

  ```text
  Input 'query22.preql' does not exist.
  ```
