# Trilogy failure analysis — 20260629-175016

- Run `20260629-175015_ingest` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 232 | failed: 23 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 19 | 83% |
| `syntax-parse` | 4 | 17% |

## Detail

### `other`

- `trilogy file read query01.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query03.preql`

  ```text
  Syntax error in query03.preql: Cannot compare DATE (ref:li.orders.orderdate) and STRING (1995-03-15) of different types with operator < in ref:li.orders.orderdate < 1995-03-15
  ```
- `trilogy run query08.preql`

  ```text
  Syntax error in query08.preql: 2 undefined concept references; fix all before re-running:
    - lineitem.extendedprice (line 7, in SELECT); did you mean: extendedprice, lineitem.discount?
    - lineitem.discount (line 7, in SELECT); did you mean: discount?
  ```
- `trilogy file read raw/lineitem.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/partsupp.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query12.preql`

  ```text
  Syntax error in query12.preql: Comparison `lineitem.shipinstruct in ('MAIL', 'SHIP')` can never match enum field 'lineitem.shipinstruct', which contains only these values: 'COLLECT COD', 'DELIVER IN PERSON', 'NONE', 'TAKE BACK RETURN'. It is always false and should be removed.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Undefined concept: orders.comment. Suggestions: ['customer.nation.region.comment', 'customer.nation.comment', 'customer.comment', 'comment']
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Undefined concept: raw.orders.comment. Suggestions: ['customer.nation.region.comment', 'customer.nation.comment', 'customer.comment', 'comment']
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Undefined concept: raw.orders.customer.custkey. Suggestions: ['customer.custkey']
  ```
- `trilogy file read query13.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: 4 undefined concept references; fix all before re-running:
    - lineitem.extendedprice (line 8, in SELECT); did you mean: extendedprice, lineitem.discount, lineitem.part.type?
    - lineitem.discount (line 8, in SELECT); did you mean: discount, lineitem.shipdate?
    - lineitem.shipdate (line 8, col 7, in WHERE); did you mean: shipdate, lineitem.part.type, lineitem.discount?
    - lineitem.shipdate (line 9, col 7, in WHERE); did you mean: shipdate, lineitem.part.type, lineitem.discount?
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: Undefined concept: orders.orderkey (line 3, col 33, in WHERE). Suggestions: ['lineitem.orders.orderkey', 'orders.orderdate', 'orders.customer.custkey']
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: Undefined concept: lineitem.part.partkey. Suggestions: ['ps.part.partkey']
  ```
- `trilogy file read query20.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Syntax error in query21.preql: Cannot compare BOOL (ref:local.late_by_supplier_order) and INTEGER (0) of different types with operator > in ref:local.late_by_supplier_order > 0
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/orders.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query12.preql --content import raw.lineitem as lineitem;

# Question 12: For order/line_no pairs shipped by MAIL or SHIP,
# with commit da…orderkey, lineitem.linenumber ? not (lineitem.orders.orderpriority in ('1-URGENT', '2-HIGH'))) as low_line_count
order by
    lineitem.shipmode asc;
`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:35
     |
  16 |     count(lineitem.orders.orderkey, lineitem.linenumber ? lineitem.orders.orderpriority in ('1-URGENT', '2-HIGH')) as high_line_count,
     |                                   ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...count(lineitem.orders.orderkey ??? , lineitem.linenumber ? lineit...

  Write stats: received 970 chars / 970 bytes; tail: …"H'))) as low_line_count\\norder by\\n    lineitem.shipmode asc;\\n".
  ```
- `trilogy run --import raw.lineitem:lineitem select lineitem.orders.orderkey, lineitem.orders.totalprice, lineitem.orders.orderdate, sum(lineitem.quantity) as total_q order by totalprice desc, orderdate asc limit 5 where sum(lineitem.quantity) by lineitem.orders.orderkey > 300;`

  ```text
  Syntax error in stdin:  --> 2:123
    |
  2 | select lineitem.orders.orderkey, lineitem.orders.totalprice, lineitem.orders.orderdate, sum(lineitem.quantity) as total_q order by totalprice desc, orderdate asc limit 5 where sum(lineitem.quantity) by lineitem.orders.orderkey > 300;
    |                                                                                                                           ^---
    |
    = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...lineitem.quantity) as total_q ??? order by totalprice desc, orde...
  ```
- `trilogy file write query21.preql --content import raw.lineitem as li;

# A lineitem is late if receiptdate > commitdate
auto is_late <- li.receiptdate > li.c…r_order = 1
select
    li.supplier.name as supplier_name,
    --count(li.orders.orderkey) as numwait
order by numwait desc, supplier_name
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...pliers, and how many had late ??? rowset order_supplier_info as

  Write stats: received 1603 chars / 1603 bytes; tail: …' as numwait\\norder by numwait desc, supplier_name\\nlimit 100;\\n'.
  ```
- `trilogy file write query21.preql --content import raw.lineitem as li;

# A lineitem is late if receiptdate > commitdate
auto is_late <- li.receiptdate > li.c…per_order = 1
select
    li.supplier.name as supplier_name,
    count(li.orders.orderkey) as numwait
order by numwait desc, supplier_name
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:89
    |
  7 | auto suppliers_per_order_f <- count_distinct(li.supplier.suppkey) by li.orders.orderkey where li.orders.orderstatus = 'F';
    |                                                                                         ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...uppkey) by li.orders.orderkey ??? where li.orders.orderstatus =

  Write stats: received 1000 chars / 1000 bytes; tail: …' as numwait\\norder by numwait desc, supplier_name\\nlimit 100;\\n'.
  ```
