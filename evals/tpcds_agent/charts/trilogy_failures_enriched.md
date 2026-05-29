# Trilogy failure analysis — 20260529-125911

- Run `20260529-125911` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 45 | failed: 8 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 6 | 75% |
| `syntax-missing-alias` | 2 | 25% |

## Detail

### `syntax-parse`

- `trilogy file write query02.preql --content import raw/unified_sales as s;

# Daily sales = sum of ext_sales_price, split into 7 day-of-week columns
# (0=Sund…saturday / cs_saturday, null) as ratio_sat
where s.date.year = 2001 and s.sales_channel in ('WEB', 'CATALOG')
order by s.date.week_seq asc
limit 100;`

  ```text
  …
  nday / cs_monday, null) as ratio_mon,\n
  coalesce(ws_tuesday / cs_tuesday, null) as ratio_tue,\n
  coalesce(ws_wednesday / cs_wednesday, null) as ratio_wed,\n
  coalesce(ws_thursday / cs_thursday, null) as ratio_thu,\n    coalesce(ws_friday
  / cs_friday, null) as ratio_fri,\n    coalesce(ws_saturday / cs_saturday, null)
  as ratio_sat\nwhere s.date.year = 2001 and s.sales_channel in ('WEB',
  'CATALOG')\norder by s.date.week_seq asc\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /unified_sales as s;  # Daily

  Write stats: received 3622 chars / 3622 bytes; tail: …"n ('WEB',
  'CATALOG')\\norder by s.date.week_seq asc\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/unified_sales.preql:s select min(s.date.week_seq) as min_ws, max(s.date.week_seq) as max_ws, s.date.year where s.date.year in (2001,2002) group by s.date.year;`

  ```text
  --> 2:117
    |
  2 | select min(s.date.week_seq) as min_ws, max(s.date.week_seq) as max_ws,
  s.date.year where s.date.year in (2001,2002) group by s.date.year;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re s.date.year in (2001,2002) ??? group by s.date.year;
  ```
- `trilogy file write query02.preql --content import raw/unified_sales as s;

# For web + catalog sales only:
# daily sales = sum of ext_sales_price, split into…at / nullif(cs_sat, 0), null) as ratio_sat
where s.date.year = 2001 and s.sales_channel in ('WEB', 'CATALOG')
order by s.date.week_seq asc
limit 100;`

  ```text
  …
  mon,
  0), null) as ratio_mon,\n    coalesce(ws_tue / nullif(cs_tue, 0), null) as
  ratio_tue,\n    coalesce(ws_wed / nullif(cs_wed, 0), null) as ratio_wed,\n
  coalesce(ws_thu / nullif(cs_thu, 0), null) as ratio_thu,\n    coalesce(ws_fri /
  nullif(cs_fri, 0), null) as ratio_fri,\n    coalesce(ws_sat / nullif(cs_sat,
  0), null) as ratio_sat\nwhere s.date.year = 2001 and s.sales_channel in ('WEB',
  'CATALOG')\norder by s.date.week_seq asc\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /unified_sales as s;  # For we...

  Write stats: received 3221 chars / 3221 bytes; tail: …"n ('WEB',
  'CATALOG')\\norder by s.date.week_seq asc\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw/unified_sales as s;

# For web and catalog sales, compute daily sales (sum of ext_sales_price)
# split … / nullif(s_2001_sat, 0) as ratio_saturday
where s.date.year = 2001 and s.sales_channel in ('WEB', 'CATALOG')
order by s.date.week_seq asc
limit 100;`

  ```text
  …
  mon / nullif(s_2001_mon, 0)
  as ratio_monday,\n    s_2002_tue / nullif(s_2001_tue, 0) as ratio_tuesday,\n
  s_2002_wed / nullif(s_2001_wed, 0) as ratio_wednesday,\n    s_2002_thu /
  nullif(s_2001_thu, 0) as ratio_thursday,\n    s_2002_fri / nullif(s_2001_fri,
  0) as ratio_friday,\n    s_2002_sat / nullif(s_2001_sat, 0) as
  ratio_saturday\nwhere s.date.year = 2001 and s.sales_channel in ('WEB',
  'CATALOG')\norder by s.date.week_seq asc\nlimit 100;") at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * "as"
          * IMPORT_DOT
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /unified_sales as s;  # For we...

  Write stats: received 3259 chars / 3259 bytes; tail: …"n ('WEB',
  'CATALOG')\\norder by s.date.week_seq asc\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.unified_sales as s;

# For web and catalog sales, compute daily sales (sum of ext_sales_price)
# split … / nullif(s_2001_sat, 0) as ratio_saturday
where s.date.year = 2001 and s.sales_channel in ('WEB', 'CATALOG')
order by s.date.week_seq asc
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 153.
  Expected one of:
          * _TERMINATOR

  Location:
  ...TALOG')) by (s.date.week_seq) ??? as s_2001_sun; auto s_2001_mon...

  Write stats: received 3259 chars / 3259 bytes; tail: …"n ('WEB',
  'CATALOG')\\norder by s.date.week_seq asc\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/unified_sales.preql:s auto test_sun <- sum(s.ext_sales_price ? s.date.year = 2001 and s.date.day_of_week = 0 and s.sales_channel in ('WEB','CATALOG')) by (s.date.week_seq) as test_sun;`

  ```text
  --> 2:151
    |
  2 | auto test_sun <- sum(s.ext_sales_price ? s.date.year = 2001 and
  s.date.day_of_week = 0 and s.sales_channel in ('WEB','CATALOG')) by
  (s.date.week_seq) as test_sun;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or window_sql_over
  Location:
  ...TALOG')) by (s.date.week_seq) ??? as test_sun;
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/catalog_sales.preql:cs --import raw/web_sales.preql:ws select cs.sold_date.day_of_week, sum(cs.ext_sales_price) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...week, sum(cs.ext_sales_price) ??? limit 5;
  ```
- `trilogy run --import raw/unified_sales.preql:s select s.sales_channel, count(s.order_id), min(s.date.year), max(s.date.year) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...les_channel, count(s.order_id) ??? , min(s.date.year), max(s.date...
  ```
