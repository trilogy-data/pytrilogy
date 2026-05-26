# Trilogy failure analysis — 20260526-032601

- Run `20260526-032601` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 281 | failed: 35 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 12 | 34% |
| `other` | 11 | 31% |
| `undefined-concept` | 4 | 11% |
| `join-resolution` | 4 | 11% |
| `cli-misuse` | 3 | 9% |
| `syntax-missing-alias` | 1 | 3% |

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
- `trilogy run query04.preql`

  ```text
  Unable to import '.\orders.preql': [Errno 2] No such file or
  directory: '.\\orders.preql'. Did you mean: raw.orders?
  ```
- `trilogy run query09.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query11.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy run query13.preql`

  ```text
  Unable to import '.\customer.preql': [Errno 2] No such file
  or directory: '.\\customer.preql'. Did you mean: raw.customer?
  ```
- `trilogy run query15.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query15.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query16.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy run query17.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query19.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "line: 4: Undefined concept:
  supplier.acctbal. Suggestions: ['partsupp.supplier.acctbal']")
  ```
- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  lineitem.orderkey. Suggestions: ['lineitem.orders.orderkey',
  'lineitem.orders.clerk', 'lineitem.orders.comment']")
  ```
- `trilogy run -`

  ```text
  (UndefinedConceptException(...), 'line: 1: Undefined concept:
  orders.orderdate.')
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  part.brand. Suggestions: ['lineitem.part.brand']")
  ```

### `join-resolution`

- `trilogy run - --import raw/orders:orders --import raw/lineitem:lineitem`

  ```text
  Could not resolve connections for query with output
  ['orders.orderkey<Purpose.KEY>Derivation.ROOT>',
  'lineitem.commitdate<Purpose.PROPERTY>Derivation.ROOT>',
  'lineitem.receiptdate<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw/supplier:supplier --import raw/lineitem:lineitem select supplier.suppkey, sum(lineitem.extendedprice * (1 - lineitem.discount)) as total_revenue where lineitem.shipdate >= '1996-01-01'::date and lineitem.shipdate < '1996-04-01'::date order by supplier.suppkey asc limit 5;`

  ```text
  Could not resolve connections for query with output
  ['supplier.suppkey<Purpose.KEY>Derivation.ROOT>',
  'local.total_revenue<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query15.preql`

  ```text
  Could not resolve connections for query with output
  ['supplier.suppkey<Purpose.KEY>Derivation.ROOT>',
  'supplier.name<Purpose.PROPERTY>Derivation.ROOT>',
  'supplier.address<Purpose.PROPERTY>Derivation.ROOT>',
  'supplier.phone<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_revenue<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/customer:customer --import raw/orders:orders select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust…ustomer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null order by cntrycode;`

  ```text
  Could not resolve connections for query with output
  ['local.cntrycode<Purpose.PROPERTY>Derivation.BASIC>',
  'local.numcust<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.totacctbal<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `cli-misuse`

- `trilogy `

  ```text
  Usage: python -m trilogy.scripts.trilogy [OPTIONS] COMMAND [ARGS]...

    Trilogy CLI - A beautiful data productivity tool.

  Options:
    --version          Show version and exit.
    --debug            Enable debug mode (show tracebacks on errors)
    --debug-file TEXT  Write SQL debug output to the specified file path
    --help             Show this message and exit.

  Commands:
    agent        Pass off a multi-step orchestration task to an AI agent.
    agent-info   Output comprehensive CLI documentation for AI agents.
    database     Inspect the database configured in trilogy.toml.
    explore      Parse
  …
  ipts.
    public       Work with trilogy-public-models hosted at...
    refresh      Refresh stale assets in Trilogy scripts.
    render       Render a Trilogy markdown report to an image or HTML file.
    run          Execute a Trilogy script or query.
    serve        Start a FastAPI server to expose Trilogy models from a...
    unit         Run unit tests on Trilogy scripts with mocked datasources.

    NOTE FOR AI AGENTS (Claude, Gemini, GPT, etc.): For comprehensive usage
    documentation, run 'trilogy agent-info' to get a detailed AGENTS.md-style
    guide with all commands, options, and usage examples.
  ```
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; where customer.acctbal > 0 and su…, 2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;`

  ```text
  'where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1, 2) select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal order by cntrycode;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; select count(customer.custkey) as c where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null;`

  ```text
  'select count(customer.custkey) as c where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/lineitem:lineitem select count(lineitem.linenumber), count(lineitem.linenumber ? lineitem.part.brand='Brand#23' and lineitem.part.container='MED BOX') limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ect count(lineitem.linenumber) ??? , count(lineitem.linenumber ?
  ```
