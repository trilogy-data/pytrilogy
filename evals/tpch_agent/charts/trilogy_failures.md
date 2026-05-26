# Trilogy failure analysis — 20260526-021153

- Run `20260526-021153` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 285 | failed: 48 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 22 | 46% |
| `join-resolution` | 15 | 31% |
| `syntax-parse` | 4 | 8% |
| `undefined-concept` | 3 | 6% |
| `cli-misuse` | 3 | 6% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query04.preql`
  - Unable to import '.\orders.preql': [Errno 2] No such file or directory: '.\\orders.preql'. Did you mean: raw.orders?
- `trilogy run query09.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query11.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query13.preql`
  - Unable to import '.\orders.preql': [Errno 2] No such file or directory: '.\\orders.preql'. Did you mean: raw.orders?
- `trilogy run query14.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query15.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query16.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy run query16.preql`
  - Cannot parse arg purpose for ref:part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'>
- `trilogy run query16.preql`
  - Cannot parse arg purpose for ref:partsupp.part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'>
- `trilogy run query16.preql`
  - Cannot parse arg purpose for ref:partsupp.part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'>
- `trilogy run --debug query16.preql`
  - Cannot parse arg purpose for ref:partsupp.part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'> Full traceback: Traceback (most recent call last): File "C:\Users\ethan\coding_projects\py…
- `trilogy run query16.preql`
  - Cannot parse arg purpose for ref:partsupp.part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'>
- `trilogy run --import raw/partsupp:partsupp select partsupp.part.type where not(partsupp.part.type like 'MEDIUM POLISHED%') li…`
  - Cannot parse arg purpose for ref:partsupp.part.type like MEDIUM POLISHED% of type <class 'trilogy.core.models.author.Comparison'>
- `trilogy run query17.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query18.preql`
  - Unable to import '.\orders.preql': [Errno 2] No such file or directory: '.\\orders.preql'. Did you mean: raw.orders?
- `trilogy run query19.preql`
  - Unable to import '.\lineitem.preql': [Errno 2] No such file or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
- `trilogy run query20.preql`
  - Unable to import '.\partsupp.preql': [Errno 2] No such file or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
- `trilogy `
  - Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 125 (char 124). Re-issue the call with valid JSON arguments.
- `trilogy run query22.preql`
  - Unable to import '.\customer.preql': [Errno 2] No such file or directory: '.\\customer.preql'. Did you mean: raw.customer?
- `trilogy run query22.preql`
  - (_duckdb.BinderException) Binder Error: Table "orders_customer_customer" does not have a column named "orders_customer_custkey" Candidate bindings: : "c_custkey" LINE 12: ...r_custkey" from customer as orders_customer_cu…
- `trilogy run query22.preql`
  - (_duckdb.BinderException) Binder Error: Table "orders_customer_customer" does not have a column named "orders_customer_custkey" Candidate bindings: : "c_custkey" LINE 12: ...r_custkey" from customer as orders_customer_cu…

### `join-resolution`

- `trilogy run query04.preql`
  - Could not resolve connections for query with output ['orders.orderpriority<Purpose.PROPERTY>Derivation.ROOT>', 'local.order_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
- `trilogy run --import raw/orders:orders --import raw/lineitem:lineitem select orders.orderpriority, count(orders.orderkey) as …`
  - Could not resolve connections for query with output ['orders.orderpriority<Purpose.PROPERTY>Derivation.ROOT>', 'local.order_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
- `trilogy run query14.preql`
  - Could not resolve connections for query with output ['local.promo_revenue<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['local.avg_yearly<Purpose.METRIC>Derivation.BASIC>'] from current model.
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['lineitem.quantity<Purpose.PROPERTY>Derivation.ROOT>', 'lineitem.extendedprice<Purpose.PROPERTY>Derivation.ROOT>', 'local.threshold<Purpose.PROPERTY>Derivation.BASIC>'…
- `trilogy run query17.preql`
  - Could not resolve connections for query with output ['lineitem.quantity<Purpose.PROPERTY>Derivation.ROOT>', 'lineitem.extendedprice<Purpose.PROPERTY>Derivation.ROOT>', 'local.threshold<Purpose.PROPERTY>Derivation.BASIC>'…
- `trilogy run --import raw/orders:orders --import raw/lineitem:lineitem --import raw/customer:customer select customer.name, or…`
  - Could not resolve connections for query with output ['customer.name<Purpose.PROPERTY>Derivation.ROOT>', 'orders.orderkey<Purpose.KEY>Derivation.ROOT>', 'lineitem.orders.orderkey<Purpose.KEY>Derivation.ROOT>'] from curren…
- `trilogy run --import raw/lineitem:lineitem --import raw/part:part select lineitem.extendedprice, lineitem.discount, lineitem.…`
  - Could not resolve connections for query with output ['lineitem.extendedprice<Purpose.PROPERTY>Derivation.ROOT>', 'lineitem.discount<Purpose.PROPERTY>Derivation.ROOT>', 'lineitem.quantity<Purpose.PROPERTY>Derivation.ROOT>…

### `syntax-parse`

- `trilogy run -e query16.preql select partsupp.part.type, partsupp.part.brand, partsupp.part.size, partsupp.supplier.suppkey fr…`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...ze, partsupp.supplier.suppkey ??? from partsupp limit 5;
- `trilogy run --import raw/lineitem:lineitem auto late_flag = lineitem.receiptdate > lineitem.commitdate; select late_flag limi…`
  - --> 2:6 | 2 | auto late_flag = lineitem.receiptdate > lineitem.commitdate; select late_flag limit 5; | ^--- | = expected prop_ident or prop_ident_wildcard Location: ...aw.lineitem as lineitem; auto ??? late_flag = lineit…
- `trilogy run --import raw/lineitem:lineitem late_flag := lineitem.receiptdate > lineitem.commitdate; select late_flag limit 5;`
  - --> 2:1 | 2 | late_flag := lineitem.receiptdate > lineitem.commitdate; select late_flag limit 5; | ^--- | = expected EOI, block, or show_statement Location: ...ort raw.lineitem as lineitem; ??? late_flag := lineitem.rece…
- `trilogy run query22.preql`
  - --> 10:32 | 10 | and customer.custkey not in (select orders.customer.custkey) | ^--- | = expected access_chain or literal Location: and customer.custkey not in ( ??? select orders.customer.custkey...

### `undefined-concept`

- `trilogy run query07.preql`
  - (UndefinedConceptException(...), "Undefined concept: supplier.nation.nationkey. Suggestions: ['lineitem.supplier.nation.nationkey', 'nation.nationkey', 'lineitem.supplier.nation.name']")
- `trilogy run query07.preql`
  - (UndefinedConceptException(...), "Undefined concept: nation.nationkey. Suggestions: ['lineitem.supplier.nation.nationkey']")
- `trilogy run -e query16.preql select partsupp.part.type, partsupp.part.brand, partsupp.part.size, partsupp.supplier.suppkey li…`
  - (UndefinedConceptException(...), 'line: 1: Undefined concept: partsupp.part.type.')

### `cli-misuse`

- `trilogy read_file raw/lineitem.preql`
  - No such command 'read_file'.
- `trilogy raw lineitem`
  - No such command 'raw'.
- `trilogy run --import raw/lineitem.preql:lineitem - select lineitem.supplier.suppkey, sum(lineitem.extendedprice * (1 - lineit…`
  - 'select lineitem.supplier.suppkey, sum(lineitem.extendedprice * (1 - lineitem.discount)) as total_revenue where lineitem.shipdate >= '1996-01-01'::date and lineitem.shipdate < '1996-04-01'::date group by lineitem.supplie…

### `file-not-found`

- `trilogy run query15.preql`
  - exit_code: 2 --- stdout --- --- stderr --- Input 'query15.preql' does not exist.
