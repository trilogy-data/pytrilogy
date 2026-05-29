# Trilogy failure analysis — 20260529-121402

- Run `20260529-121402` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 37 | failed: 4 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 1 | 25% |
| `join-resolution` | 1 | 25% |
| `cli-misuse` | 1 | 25% |
| `syntax-parse` | 1 | 25% |

## Detail

### `undefined-concept`

- `trilogy run --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_sales select web_sales.date, web_sales.ext_sales_price, catalog_sales.sold_date, catalog_sales.ext_sales_price limit 3;`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  web_sales.date. Suggestions: ['web_sales.date.id', 'web_sales.date.year',
  'web_sales.date.date']")
  ```

### `join-resolution`

- `trilogy run --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_sales select web_sales.date.date, web_sales.ext_sales_price, catalog_sales.sold_date.date, catalog_sales.ext_sales_price limit 3;`

  ```text
  Could not resolve connections for query with output
  ['web_sales.date.date<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.ext_sales_price<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.sold_date.date<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.ext_sales_price<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```

### `cli-misuse`

- `trilogy --config trilogy.toml explore raw/unified_sales.preql --show concepts`

  ```text
  No such option: --config
  ```

### `syntax-parse`

- `trilogy run --import raw/unified_sales:unified_sales select unified_sales.date.week_seq, unified_sales.date.year, unified_sales.date.day_of_week, sum(unified…') and unified_sales.date.year in (2001,2002) group by unified_sales.date.week_seq, unified_sales.date.year, unified_sales.date.day_of_week limit 20;`

  ```text
  --> 2:242
    |
  2 | select unified_sales.date.week_seq, unified_sales.date.year,
  unified_sales.date.day_of_week, sum(unified_sales.ext_sales_price) as
  daily_total where unified_sales.sales_channel in ('WEB','CATALOG') and
  unified_sales.date.year in (2001,2002) group by unified_sales.date.week_seq,
  unified_sales.date.year, unified_sales.date.day_of_week limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ales.date.year in (2001,2002) ??? group by unified_sales.date.we...
  ```
