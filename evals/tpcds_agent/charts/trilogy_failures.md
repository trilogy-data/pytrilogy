# Trilogy failure analysis — 20260529-021835

- Run `20260529-021834_base` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 474 | failed: 38 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `join-resolution` | 10 | 26% |
| `other` | 7 | 18% |
| `cli-misuse` | 6 | 16% |
| `syntax-missing-alias` | 5 | 13% |
| `undefined-concept` | 5 | 13% |
| `syntax-parse` | 4 | 11% |
| `file-not-found` | 1 | 3% |

## Detail

### `join-resolution`

- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales select web_sales.sold_date.week_seq, catalog_sales.sold_date.year limit 5;`

  ```text
  Could not resolve connections for query with output
  ['web_sales.sold_date.week_seq<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --debug query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base
  …
  s_v3.py", line 517, in source_query_concepts
      raise UnresolvableQueryException(
          f"Could not resolve connections for query with output {error_strings}
  from current model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket1_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket2_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket3_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket4_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.bucket5_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query17.preql`

  ```text
  Could not resolve connections for query with output
  ['item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'local.store_state<Purpose.PROPERTY>Derivation.BASIC>',
  'local.ss_qty_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ss_qty_avg<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ss_qty_stddev<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ss_qty_cv<Purpose.METRIC>Derivation.BASIC>',
  'local.sr_qty_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_qty_avg<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_qty_stddev<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_qty_cv<Purpose.METRIC>Derivation.BASIC>',
  'local.cs_qty_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_qty_avg<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_qty_stddev<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_qty_cv<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```

### `other`

- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales merge catalog_sales.sold_date.date_sk into web_sales.sold_d…w = 0) as sun_total where web_sales.sold_date.year = 2002 and web_sales.sold_date.week_seq = 5274 + 53 order by web_sales.sold_date.week_seq limit 5;`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 34:     INVALID_REFERENCE_BUG_<Missing source reference to
  web_sales.sold_date.week_seq> asc
                                              ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "web_sales_sold_date_date_dim"."d_week_seq" + 53 as "ws2002",
      sum("catalog_sales_catalog_sales"."cs_ext_sales_price") as
  "_virt_agg_sum_2210594986834200"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
      INNER JOIN "date_dim" as "web_sales_sold_date_date_dim" on
  "catalog_sales_catalog_sales"."cs_sold_date_sk" =
  "web_sales
  …
  nd
  "web_sales_sold_date_date_dim"."d_week_seq" = 5274 + 53 and
  "web_sales_sold_date_date_dim"."d_dow" = 0

  GROUP BY
      1)
  SELECT
      coalesce("cooperative"."ws2002","wakeful"."ws2002") as "ws2002",
      "cooperative"."_virt_agg_sum_5840679013408605" +
  "wakeful"."_virt_agg_sum_2210594986834200" as "sun_total"
  FROM
      "wakeful"
      INNER JOIN "cooperative" on "wakeful"."ws2002" is not distinct from
  "cooperative"."ws2002"
  ORDER BY
      INVALID_REFERENCE_BUG_<Missing source reference to
  web_sales.sold_date.week_seq> asc
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy database describe date_dim --schema`

  ```text
  Option '--schema' requires an argument.
  ```
- `trilogy run query05.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy run query09.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run`

  ```text
  Some scripts failed during execution.
  ```
- `trilogy run query18.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 153:     grouping(coalesce("vacuous"."catalog_sales_bill_customer_cu...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "catalog_sales_catalog_sales"."cs_bill_cdemo_sk" as
  "catalog_sales_bill_cdemo_demo_sk",
      "catalog_sales_catalog_sales"."cs_bill_customer_sk" as
  "catalog_sales_bill_customer_customer_sk",
      "catalog_sales_catalog_sales"."cs_item_sk" as "catalog_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "cat
  …
  ") asc nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ountry","young"."catalog_sales_bill_customer_customer_address_country")) asc
  nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_s
  tate","young"."catalog_sales_bill_customer_customer_address_state")) asc nulls
  first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ounty","young"."catalog_sales_bill_customer_customer_address_county")) asc
  nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query18.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 153:     grouping(coalesce("vacuous"."catalog_sales_bill_customer_cu...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "catalog_sales_catalog_sales"."cs_bill_cdemo_sk" as
  "catalog_sales_bill_cdemo_demo_sk",
      "catalog_sales_catalog_sales"."cs_bill_customer_sk" as
  "catalog_sales_bill_customer_customer_sk",
      "catalog_sales_catalog_sales"."cs_item_sk" as "catalog_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "cat
  …
  ") asc nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ountry","young"."catalog_sales_bill_customer_customer_address_country")) asc
  nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_s
  tate","young"."catalog_sales_bill_customer_customer_address_state")) asc nulls
  first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ounty","young"."catalog_sales_bill_customer_customer_address_county")) asc
  nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `cli-misuse`

- `trilogy run query04.preql -v --debug`

  ```text
  '-v' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
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
- `trilogy db list`

  ```text
  No such command 'db'.
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy database run select d_year, count(*) from date_dim where d_year in (2001, 2002) group by d_year order by d_year;`

  ```text
  No such command 'run'.
  ```
- `trilogy trilogy run`

  ```text
  No such command 'trilogy'.
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge catalog_sales.sold_date.date_…ce(y2001_fri / nullif(y2002_fri, 0), 0), 2) as fri_ratio,
    round(coalesce(y2001_sat / nullif(y2002_sat, 0), 0), 2) as sat_ratio
order by week_seq;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ect     distinct     y2001_ws ??? as week_seq,     round(coalesc...

  Write stats: received 3708 chars / 3708 bytes; tail: …'nullif(y2002_sat, 0),
  0), 2) as sat_ratio\\norder by week_seq;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge catalog_sales.sold_date.date_…ce(y2001_fri / nullif(y2002_fri, 0), 0), 2) as fri_ratio,
    round(coalesce(y2001_sat / nullif(y2002_sat, 0), 0), 2) as sat_ratio
order by week_seq;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   select     distinct y2001_ws ??? as week_seq,     round(coalesc...

  Write stats: received 3502 chars / 3502 bytes; tail: …'nullif(y2002_sat, 0),
  0), 2) as sat_ratio\\norder by week_seq;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge catalog_sales.sold_date.date_…ce(y2001_fri / nullif(y2002_fri, 0), 0), 2) as fri_ratio,
    round(coalesce(y2001_sat / nullif(y2002_sat, 0), 0), 2) as sat_ratio
order by week_seq;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   select     distinct y2001_ws ??? as week_seq,     round(coalesc...

  Write stats: received 3413 chars / 3413 bytes; tail: …'nullif(y2002_sat, 0),
  0), 2) as sat_ratio\\norder by week_seq;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales merge catalog_sales.sold_date.date_sk into web_sales.sold_d…ws, web_sales.sold_date.year as yr where web_sales.sold_date.year = 2001 and web_sales.sold_date.week_seq between 5270 and 5280 order by ws limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...date.date_sk; select distinct ??? web_sales.sold_date.week_seq a...
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge catalog_sales.sold_date.date_…ce(y2001_fri / nullif(y2002_fri, 0), 0), 2) as fri_ratio,
    round(coalesce(y2001_sat / nullif(y2002_sat, 0), 0), 2) as sat_ratio
order by week_seq;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
    select     distinct     sum( ??? web_sales.sold_date.week_seq ?...

  Write stats: received 4484 chars / 4484 bytes; tail: …'nullif(y2002_sat, 0),
  0), 2) as sat_ratio\\norder by week_seq;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales merge catalog_sales.sold_date.date_sk into web_sales.sold_date.date_sk; select y2001_ws as week_seq, round(coalesce(y2001_sun / nullif(y2002_sun, 0), 0), 2) as sun_ratio;`

  ```text
  (UndefinedConceptException(...), 'line: 3: Undefined concept:
  local.y2001_sun.')
  ```
- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  local.ext_sales_price. Suggestions: ['store_sales.ext_sales_price',
  'store_sales.list_price', 'store_sales.sales_price']")
  ```
- `trilogy run --import raw/store_sales:store_sales select store.date_dim.qoy, store.date_dim.year, store.store_name limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  store.date_dim.qoy. Suggestions: ['store_sales.date_dim.qoy',
  'store_sales.date_dim.moy', 'store_sales.date_dim.dow']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.sold_date.date. Suggestions: ['web_sales.sold_date.date',
  'catalog_sales.sold_date.date', 'web_sales.sold_date.date_sk']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.date_sk. Suggestions: ['store.date_dim.date_sk',
  'store.date_dim.date', 'store.date_dim.date_id']")
  ```

### `syntax-parse`

- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales merge catalog_sales.sold_date.date_sk into web_sales.sold_d…select web_sales.sold_date.dow, sum(web_sales.ext_sales_price)+sum(catalog_sales.ext_sales_price) as total_by_dow by web_sales.sold_date.dow limit 7;`

  ```text
  --> 3:187
    |
  3 | merge catalog_sales.sold_date.date_sk into web_sales.sold_date.date_sk;
  select web_sales.sold_date.dow,
  sum(web_sales.ext_sales_price)+sum(catalog_sales.ext_sales_price) as
  total_by_dow by web_sales.sold_date.dow limit 7;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ..._sales_price) as total_by_dow ??? by web_sales.sold_date.dow lim...
  ```
- `trilogy run --import raw/web_sales.preql:web_sales --import raw/catalog_sales.preql:catalog_sales merge catalog_sales.sold_date.date_sk into web_sales.sold_d….ext_sales_price) as total where web_sales.sold_date.year = 2001 group by web_sales.sold_date.week_seq order by web_sales.sold_date.week_seq limit 5;`

  ```text
  --> 3:267
    |
  3 | merge catalog_sales.sold_date.date_sk into web_sales.sold_date.date_sk;
  select web_sales.sold_date.week_seq as week_seq, web_sales.sold_date.year as
  yr, sum(web_sales.ext_sales_price)+sum(catalog_sales.ext_sales_price) as total
  where web_sales.sold_date.year = 2001 group by web_sales.sold_date.week_seq
  order by web_sales.sold_date.week_seq limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...b_sales.sold_date.year = 2001 ??? group by web_sales.sold_date.w...
  ```
- `trilogy run select s_store_name, s_zip from store limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  select s_store_name, s_zip ??? from store limit 10;
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

where 
    catalog_sales.sold_date.year = 1998
    and catalog_sales.b…log_sales.bill_customer.customer_address.state) nulls first,
    grouping(catalog_sales.bill_customer.customer_address.county) nulls first
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 21, column 1.
  Expected one of:
          * HAVING
          * METADATA
          * COMMA
          * WHERE
          * MERGE
          * LIMIT
          * ORDER
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'avg_dependent_count')]

  Location:
  ...count) as avg_dependent_count ??? group by     catalog_sales.ite...

  Write stats: received 1738 chars / 1738 bytes; tail:
  …'ill_customer.customer_address.county) nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `file-not-found`

- `trilogy run query09.preql`

  ```text
  Input 'query09.preql' does not exist.
  ```
