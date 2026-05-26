# Trilogy failure analysis — 20260527-220231

- Run `20260527-220231` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 335 | failed: 56 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 32 | 57% |
| `syntax-missing-alias` | 7 | 12% |
| `join-resolution` | 6 | 11% |
| `syntax-parse` | 5 | 9% |
| `undefined-concept` | 4 | 7% |
| `cli-misuse` | 1 | 2% |
| `file-not-found` | 1 | 2% |

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

- `trilogy run query01.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query02.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'lineitem.linenumber', which is not in the
  SELECT projection (line 10). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --lineitem.linenumber`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query05.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query07.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 44 (char 43). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 42 (char 41). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 40 (char 39). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 44 (char 43). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 44 (char 43). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 5 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 42 (char 41). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 6 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 42 (char 41). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 7 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 3 column 6 (char 301). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query09.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query10.preql`

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
- `trilogy run query14.preql`

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
  Filter item with non ref content MagicConstants.NULL (<enum
  'MagicConstants'>) not yet supported
  ```
- `trilogy run query17.preql`

  ```text
  Unable to import '.\lineitem.preql': [Errno 2] No such file
  or directory: '.\\lineitem.preql'. Did you mean: raw.lineitem?
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\partsupp.preql': [Errno 2] No such file
  or directory: '.\\partsupp.preql'. Did you mean: raw.partsupp?
  ```
- `trilogy run query20.preql --import raw/partsupp:partsupp --import raw/lineitem:lineitem`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query22.preql`

  ```text
  Have
  {'MergeNode<customer.acctbal,customer.phone,local._virt_8542720658922315...2
  more>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal >
  local.avg_acctbal_target and customer.acctbal > 0 and
  coalesce(local._virt_agg_count_2275786582956115@Grain<customer.custkey>,0) = 0
  ```
- `trilogy run query22.preql`

  ```text
  Have
  {'MergeNode<customer.acctbal,customer.phone,local._virt_8542720658922315...2
  more>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal >
  local.avg_acctbal_target and customer.acctbal > 0 and
  coalesce(local._virt_agg_count_2275786582956115@Grain<customer.custkey>,0) = 0
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…omer.acctbal > 0
  and customer.acctbal > 5017.600775193799
  and coalesce(count(orders.orderkey) by customer.custkey, 0) = 0
order by cntrycode asc;`

  ```text
  Have
  {'SelectNode<customer.acctbal,customer.custkey,customer.phone>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal > 0 and
  customer.acctbal > 5017.600775193799 and
  coalesce(local._virt_agg_count_2275786582956115@Grain<customer.custkey>,0) = 0
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…','31','23','29','30','18','17')
  and customer.acctbal > 0
  and coalesce(count(orders.orderkey) by customer.custkey, 0) = 0
order by cntrycode asc;`

  ```text
  Have
  {'SelectNode<customer.acctbal,customer.custkey,customer.phone>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal > 0 and
  coalesce(local._virt_agg_count_2275786582956115@Grain<customer.custkey>,0) = 0
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 4 column 4 (char 256). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer auto cust_has_orders <- count(orders.orderkey) by customer.custkey;
select
    substrin…stomer.phone, 1, 2) in ('13','31','23','29','30','18','17')
  and customer.acctbal > 0
  and coalesce(cust_has_orders, 0) = 0
order by cntrycode asc;`

  ```text
  Have
  {'SelectNode<customer.acctbal,customer.custkey,customer.phone>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal > 0 and
  coalesce(local.cust_has_orders@Grain<customer.custkey>,0) = 0
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer auto cust_has_orders <- count(orders.orderkey) by customer.custkey;

select
    substri…tomer.phone, 1, 2) in ('13','31','23','29','30','18','17')
  and customer.acctbal > 0
  and coalesce(cust_has_orders, 0) = 0
order by cntrycode asc;
`

  ```text
  Have
  {'SelectNode<customer.acctbal,customer.custkey,customer.phone>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal > 0 and
  coalesce(local.cust_has_orders@Grain<customer.custkey>,0) = 0
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer auto avg_bal <- avg(customer.acctbal ? customer.acctbal > 0 and substring(customer.phon…','18','17')
  and customer.acctbal > 0
  and customer.acctbal > avg_bal
  and count(orders.orderkey) by customer.custkey = 0
order by cntrycode asc;`

  ```text
  Have
  {'MergeNode<customer.acctbal,customer.phone,local._virt_8542720658922315...2
  more>': None} and need
  BuildSubselectComparison(left=substring(customer.phone@Grain<customer.custkey>,
  1,2), right=('13', '31', '23', '29', '30', '18', '17'),
  operator=<ComparisonOperator.IN: 'in'>) and customer.acctbal > 0 and
  customer.acctbal > local.avg_bal and local._virt_agg_count_2275786582956115 = 0
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…ng(customer.phone, 1, 2) in ('13','31','23','29','30','18','17')) by (1)
  and count(orders.orderkey) by customer.custkey = 0
order by cntrycode asc;`

  ```text
  WHERE clause aggregates at multiple grains are not allowed:
  `SubselectComparison(left=substring(ref:customer.phone,1,2), right=('13', '31',
  '23', '29', '30', '18', '17'), operator=<ComparisonOperator.IN: 'in'>) and
  ref:customer.acctbal > 0 and ref:customer.acctbal > avg(<Filter:
  ref:customer.acctbal where ref:customer.acctbal > 0 and
  SubselectComparison(left=substring(ref:customer.phone,1,2), right=('13', '31',
  '23', '29', '30', '18', '17'), operator=<ComparisonOperator.IN:
  'in'>)>)<['ref:local._virt_8542720658922315']> and
  count(ref:orders.orderkey)<['ref:customer.custkey']> = 0`. Aggregates filter
  rows AFTER grouping - use HAVING (post-aggregate filter), or align all
  aggregates to the same `by` grain so the filter is a pure row-level
  pre-aggregate predicate; Line: 3
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/part:part select part.type, count(part.partkey) order by count(part.partkey) desc limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...art.type, count(part.partkey) ??? order by count(part.partkey) d...
  ```
- `trilogy run --import raw/lineitem:lineitem select year(lineitem.orders.orderdate), count(lineitem.linenumber) as cnt where year(lineitem.orders.orderdate) in…neitem.part.type='ECONOMY ANODIZED STEEL' and lineitem.orders.customer.nation.region.name='AMERICA' order by year(lineitem.orders.orderdate) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ear(lineitem.orders.orderdate) ??? , count(lineitem.linenumber) a...
  ```
- `trilogy run --import raw/lineitem:lineitem select distinct lineitem.supplier.nation.name as s_nation limit 50;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   as lineitem; select distinct ??? lineitem.supplier.nation.name
  ```
- `trilogy run --import raw/part.preql:part select part.container, count(part.partkey) where part.brand = 'Brand#23' group by part.container;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ontainer, count(part.partkey) ??? where part.brand = 'Brand#23'
  ```
- `trilogy run --import raw/part.preql:part select distinct part.container where part.brand = 'Brand#23' and part.container like '%MED%' ;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...part as part; select distinct ??? part.container where part.bran...
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select distinct customer.custkey from customer;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   as customer; select distinct ??? customer.custkey from customer...
  ```
- `trilogy run --import raw/orders:orders select count(orderkey);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...orders; select count(orderkey) ??? ;
  ```

### `join-resolution`

- `trilogy run query13.preql`

  ```text
  Could not resolve connections for query with output
  ['local.order_count<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.sname<Purpose.PROPERTY>Derivation.BASIC>',
  'local.numwait<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…string(customer.phone, 1, 2) in ('13','31','23','29','30','18','17')
  and customer.acctbal > 0
  and orders.orderkey is null
order by cntrycode asc;`

  ```text
  Could not resolve connections for query with output
  ['local.cntrycode<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer auto no_order_cust <- customer.custkey ? count(orders.orderkey) by customer.custkey = 0…mer.phone, 1, 2) in ('13','31','23','29','30','18','17')
  and customer.acctbal > 0
  and customer.custkey in (no_order_cust)
order by cntrycode asc;`

  ```text
  Could not resolve connections for query with output
  ['local.no_order_cust<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer auto no_order_cust <- customer.custkey ? count(orders.orderkey) by customer.custkey = 0;
select
    no_order_cust,
    substring(customer.phone, 1, 2) as cntrycode
limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.no_order_cust<Purpose.PROPERTY>Derivation.FILTER>',
  'local.cntrycode<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    customer.custkey,
    count(orders.orderkey) by customer.custkey as order_count
limit 10;`

  ```text
  Could not resolve connections for query with output
  ['customer.custkey<Purpose.KEY>Derivation.ROOT>',
  'local.order_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `syntax-parse`

- `trilogy run --import raw/lineitem:lineitem select lineitem.supplier.nation.name as s_nation, lineitem.supplier.nation.region.name as s_region, count(lineitem.linenumber) as cnt group by (lineitem.supplier.nation.name, lineitem.supplier.nation.region.name) order by cnt desc limit 30;`

  ```text
  --> 2:135
    |
  2 | select lineitem.supplier.nation.name as s_nation,
  lineitem.supplier.nation.region.name as s_region, count(lineitem.linenumber) as
  cnt group by (lineitem.supplier.nation.name,
  lineitem.supplier.nation.region.name) order by cnt desc limit 30;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...t(lineitem.linenumber) as cnt ??? group by (lineitem.supplier.na...
  ```
- `trilogy file write query17.preql --content import lineitem as lineitem;

# First compute per-part average quantity (only for parts with Brand#23 and MED BOX)…'MED BOX'
  and lineitem.quantity < 0.2 * avg(lineitem.quantity) by lineitem.part.partkey

select
    sum(lineitem.extendedprice) / 7.0 as avg_yearly`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...ndedprice) / 7.0 as avg_yearly ???

  Write stats: received 716 chars / 716 bytes; tail: …'\\n\\nselect\\n
  sum(lineitem.extendedprice) / 7.0 as avg_yearly'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…nd customer.acctbal > 0
  and not exists(
    select orders.orderkey
    where orders.customer.custkey = customer.custkey
  )
order by cntrycode asc;`

  ```text
  --> 9:17
    |
  9 |   and not exists(
    |                 ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or
  MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r.acctbal > 0   and not exists ??? (     select orders.orderkey
  ```
- `trilogy file write query22.preql --content import raw.customer as customer;

# First compute the scalar: average positive balance among target area codes
aut….acctbal > 0
  and customer.acctbal > avg_target_bal
  and customer.custkey not in (
      select orders.customer.custkey
  )
order by cntrycode asc;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER', 'order') at line 14, column 14.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...custkey not in (       select ??? orders.customer.custkey   ) or...

  Write stats: received 652 chars / 652 bytes; tail: …'  select
  orders.customer.custkey\\n  )\\norder by cntrycode asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/orders:orders --import raw/customer:customer select
    substring(customer.phone, 1, 2) as cntrycode,
    count(customer.custkey) as…tbal > 0
  and customer.custkey in (
    select customer.custkey
    where count(orders.orderkey) by customer.custkey = 0
  )
order by cntrycode asc;`

  ```text
  --> 10:5
     |
  10 |     select customer.custkey
     |     ^---
     |
     = expected access_chain or literal
  Location:
  ...and customer.custkey in (     ??? select customer.custkey     wh...
  ```

### `undefined-concept`

- `trilogy run query09.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  partsupp.partkey. Suggestions: ['partsupp.part.partkey', 'partsupp.part.type',
  'partsupp.part.size']")
  ```
- `trilogy run query16.preql`

  ```text
  (UndefinedConceptException(...), "line: 5: Undefined concept:
  part.brand. Suggestions: ['partsupp.part.brand']")
  ```
- `trilogy run query16.preql`

  ```text
  (UndefinedConceptException(...), "line: 5: Undefined concept:
  part.brand. Suggestions: ['partsupp.part.brand']")
  ```
- `trilogy run --import raw/orders:orders select count(orderkey) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.orderkey. Suggestions: ['orders.orderkey', 'orders.clerk']")
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

### `file-not-found`

- `trilogy run query13.preql`

  ```text
  Input 'query13.preql' does not exist.
  ```
