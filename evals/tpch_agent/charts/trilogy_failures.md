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
  - --> 2:46 | 2 | where lineitem.shipdate <= '1998-09-02'::date; | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: ...shipdate <= '1998-09-…
- `trilogy run - --import raw/orders:orders --import raw/lineitem:lineitem`
  - --> 3:5 | 3 | with; | ^--- | = expected IDENTIFIER Location: ...raw.lineitem as lineitem; with ??? ;
- `trilogy run query16.preql`
  - --> 4:22 | 4 | and not starts_with(ps.part.type, 'MEDIUM POLISHED') | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Locatio…
- `trilogy run --import raw/customer:customer select substring(customer.phone, 1, 2) as cnt from customer order by cnt limit 5;`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...(customer.phone, 1, 2) as cnt ??? from customer order by cnt lim...
- `trilogy run query22.preql`
  - --> 9:165 | 9 | and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1, 2); | ^--- | = expecte…
- `trilogy run --import raw/customer:customer select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as nu…`
  - --> 2:235 | 2 | select substring(customer.phone, 1, 2) as cntrycode, count(customer.custkey) as numcust, sum(customer.acctbal) as totacctbal where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', …
- `trilogy run query22.preql`
  - --> 9:165 | 9 | and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')) by substring(customer.phone, 1, 2); | ^--- | = expecte…
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; w…`
  - --> 3:354 | 3 | merge customer.custkey into ~orders.customer.custkey; where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null and customer.…
- `trilogy run --import raw/customer:customer --import raw/orders:orders -- merge customer.custkey into ~orders.customer.custkey…`
  - --> 3:354 | 3 | merge customer.custkey into ~orders.customer.custkey; where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null and customer.…
- `trilogy run test_merge.preql`
  - --> 8:30 | 8 | and orders.orderkey is null; | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: and orders.orderkey is null ??? ; select c…
- `trilogy run test_merge.preql`
  - --> 8:30 | 8 | and orders.orderkey is null; | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: and orders.orderkey is null ??? ; select c…
- `trilogy run --import raw/customer:customer select count(customer.custkey) as c, sum(customer.acctbal) as t, substring(custome…`
  - --> 2:255 | 2 | select count(customer.custkey) as c, sum(customer.acctbal) as t, substring(customer.phone, 1, 2) as cntrycode where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30…

### `other`

- `trilogy run -`
  - Unable to import 'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpch_agent\results\20260526 -032601\workspace\_worker_3\lineitem.preql': [Errno 2] No such file or directory: 'C:\\Users\\ethan\\coding_projects\\pytri…
- `trilogy run query02.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query04.preql`
  - Unable to import '.\orders.preql': [Errno 2] No such file or directory: '.\\orders.preql'. Did you mean: raw.orders?
- `trilogy run query09.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query11.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query13.preql`
  - Unable to import '.\customer.preql': [Errno 2] No such file or directory: '.\\customer.preql'. Did you mean: raw.customer?
- `trilogy run query15.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query15.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query16.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query17.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query19.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?

### `undefined-concept`

- `trilogy run query02.preql`
  - (UndefinedConceptException(...), "line: 4: Undefined concept: supplier.acctbal. Suggestions: ['partsupp.supplier.acctbal']")
- `trilogy run query03.preql`
  - (UndefinedConceptException(...), "Undefined concept: lineitem.orderkey. Suggestions: ['lineitem.orders.orderkey', 'lineitem.orders.clerk', 'lineitem.orders.comment']")
- `trilogy run -`
  - (UndefinedConceptException(...), 'line: 1: Undefined concept: orders.orderdate.')
- `trilogy run query17.preql`
  - (UndefinedConceptException(...), "Undefined concept: part.brand. Suggestions: ['lineitem.part.brand']")

### `join-resolution`

- `trilogy run - --import raw/orders:orders --import raw/lineitem:lineitem`
  - Could not resolve connections for query with output ['orders.orderkey<Purpose.KEY>Derivation.ROOT>', 'lineitem.commitdate<Purpose.PROPERTY>Derivation.ROOT>', 'lineitem.receiptdate<Purpose.PROPERTY>Derivation.ROOT>'] from…
- `trilogy run --import raw/supplier:supplier --import raw/lineitem:lineitem select supplier.suppkey, sum(lineitem.extendedprice…`
  - Could not resolve connections for query with output ['supplier.suppkey<Purpose.KEY>Derivation.ROOT>', 'local.total_revenue<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
- `trilogy run query15.preql`
  - Could not resolve connections for query with output ['supplier.suppkey<Purpose.KEY>Derivation.ROOT>', 'supplier.name<Purpose.PROPERTY>Derivation.ROOT>', 'supplier.address<Purpose.PROPERTY>Derivation.ROOT>', 'supplier.pho…
- `trilogy run --import raw/customer:customer --import raw/orders:orders select substring(customer.phone, 1, 2) as cntrycode, co…`
  - Could not resolve connections for query with output ['local.cntrycode<Purpose.PROPERTY>Derivation.BASIC>', 'local.numcust<Purpose.METRIC>Derivation.AGGREGATE>', 'local.totacctbal<Purpose.METRIC>Derivation.AGGREGATE>'] fr…

### `cli-misuse`

- `trilogy `
  - exit_code: 2 --- stdout --- --- stderr --- Usage: python -m trilogy.scripts.trilogy [OPTIONS] COMMAND [ARGS]... Trilogy CLI - A beautiful data productivity tool. Options: --version Show version and exit. --debug Enable d…
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; w…`
  - 'where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null and customer.acctbal > avg(customer.acctbal ? customer.acctbal > 0 and substring(c…
- `trilogy run --import raw/customer:customer --import raw/orders:orders merge customer.custkey into ~orders.customer.custkey; s…`
  - 'select count(customer.custkey) as c where customer.acctbal > 0 and substring(customer.phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and orders.orderkey is null;' is not a valid dialect. Choose one of: bigqu…

### `syntax-missing-alias`

- `trilogy run --import raw/lineitem:lineitem select count(lineitem.linenumber), count(lineitem.linenumber ? lineitem.part.brand…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...ect count(lineitem.linenumber) ??? , count(lineitem.linenumber ?
