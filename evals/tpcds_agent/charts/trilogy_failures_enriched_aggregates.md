# Trilogy failure analysis — 20260810-211911

- Run `20260810-211903_enriched_aggregates` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 300 | failed: 18 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 5 | 28% |
| `cli-misuse` | 5 | 28% |
| `other` | 5 | 28% |
| `syntax-parse` | 2 | 11% |
| `no-output` | 1 | 6% |

## Detail

### `disabled-tool`

- `trilogy file read answer_3705756794.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `cli-misuse`

- `trilogy explore C:\trilogy\raw\store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'C:\\trilogy\\raw\\store_sales.preql' does not exist.
  ```
- `trilogy explore`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore catalog_sales.preql`

  ```text
  Invalid value for 'PATH': File 'catalog_sales.preql' does not exist.
  ```
- `trilogy explore item.preql`

  ```text
  Invalid value for 'PATH': File 'item.preql' does not exist.
  ```
- `trilogy explore date.preql`

  ```text
  Invalid value for 'PATH': File 'date.preql' does not exist.
  ```

### `other`

- `trilogy run answer_3697440276.preql`

  ```text
  zero-length delimited identifier at or near \"\"\"\"\n\nLINE 115: ...r_id_at_a_billing_customer_id_at_web01_cid_at_web01_cid']\".\"\",\"busy\".\"INVALID_ALIAS: [MODELS_EXECUTE] Concept local...\n                                                                        ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n    \"a_billing_customer_customers\".\"C_CUSTOMER_ID\" as \"web01_cid\",\n    sum(\"a_web_sales_unified\".\"WS_EXT_LIST_PRICE\" - \"a_web_sales_unified\".\"WS_EXT_DISCOUNT_AMT\") as \"web01_rev\"\nFROM\n    \"fact_web_sales\" as \"a_web_sales_unified\"\n    INNER JOIN \"dim_date_dim\" as \"a
  …
  macho\".\"web02_cid\") asc nulls first,\n    \"concerned\".\"first_name\" asc nulls first,\n    \"concerned\".\"last_name\" asc nulls first,\n    \"concerned\".\"preferred_cust_flag\" asc nulls first\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 20.076,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run answer_3697440276.preql`

  ```text
  zero-length delimited identifier at or near \"\"\"\"\n\nLINE 115: ...r_id_at_a_billing_customer_id_at_web01_cid_at_web01_cid']\".\"\",\"busy\".\"INVALID_ALIAS: [MODELS_EXECUTE] Concept local...\n                                                                        ^\n[SQL: \nWITH \nscrawny as (\nSELECT\n    \"a_billing_customer_customers\".\"C_CUSTOMER_ID\" as \"web01_cid\",\n    sum(\"a_web_sales_unified\".\"WS_EXT_LIST_PRICE\" - \"a_web_sales_unified\".\"WS_EXT_DISCOUNT_AMT\") as \"web01_rev\"\nFROM\n    \"fact_web_sales\" as \"a_web_sales_unified\"\n    INNER JOIN \"dim_date_dim\" as \"a
  …
  macho\".\"web02_cid\") asc nulls first,\n    \"concerned\".\"first_name\" asc nulls first,\n    \"concerned\".\"last_name\" asc nulls first,\n    \"concerned\".\"preferred_cust_flag\" asc nulls first\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 12.109,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting value: line 1 column 54 (char 53). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run probe2.preql`

  ```text
  Syntax error in probe2.preql: Output column 'ss_present' renames 'local.ss_present' back to the name of an existing concept 'ss_present' (defined at line 4) that 'local.ss_present' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'ss_present_out').
  ```
- `trilogy run probe6.preql`

  ```text
  Unexpected error in probe6.preql: composite membership right-hand operands must resolve to a single existence source, got ['INVALID_REFERENCE_BUG<Missing source reference to cs.billing_customer.sk>', 'dim_item as cs_item_items']
  ```

### `syntax-parse`

- `trilogy file write check2.preql`

  ```text
  refused to write 'check2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...g_sales as cs;

   select
       ??? distinct cs.catalog_page.id
   l...
  ```
- `trilogy file write probe5.preql`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:1
     |
  18 | union join ss.customer.sk = cs.billing_customer.sk
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ling_customer.sk is not null
   ??? union join ss.customer.sk = cs...
  ```

### `no-output`

- `trilogy run raw/all_sales.preql`

  ```text
  Nothing was executed: parsed 31 definition statement(s) (12 datasources, 11 imports, 5 concepts, 3 propertys) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
