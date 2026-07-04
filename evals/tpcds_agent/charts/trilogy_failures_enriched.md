# Trilogy failure analysis — 20260704-140355

- Run `20260704-140355` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 166 | failed: 10 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 8 | 80% |
| `syntax-parse` | 2 | 20% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_type)` (line 45), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_type) as g order by g desc`.
  ```
- `trilogy run query17.preql`

  ```text
  Unexpected error in query17.preql: (_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(ss_item_id)" if the exact value of "ss_item_id" is not important.

  LINE 131:     "puzzled"."ss_item_id" as "ss_item_id",
                ^
  [SQL:
  WITH
  juicy as (
  SELECT
      "ss_item_items"."I_ITEM_DESC" as "ss_item_desc",
      "ss_item_items"."I_ITEM_ID" as "ss_item_text_id",
      "ss_item_items"."I_ITEM_SK" as "ss_item_id"
  FROM
      "item" as "ss_item_items"
  GROUP BY
      1,
      2,
      3),
  premium as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  abhorrent as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  uneven as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3),
  friendly as (
  SELECT
      "sr_store_returns"."SR_CUSTOMER_SK" as "_sr_filtered_sr_cust_id",
      "sr_store_returns"."SR_ITEM_SK" as "_sr_filtered_sr_item_id",
      "sr_store_returns"."SR_RETURN_QUANTITY" as "sr_return_quantity",
      "sr_store_returns"."SR_TICKET_NUMBER" as "sr_ticket_number"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
  WHERE
      "sr_return_date_date"."D_YEAR" in (2001,2002)
  ),
  wakeful as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "_cs_filtered_cs_cust_id",
      "cs_catalog_sales"."CS_ITEM_SK" as "_cs_filtered_cs_item_id",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" in (2001,2002)
  ),
  puzzled as (
  SELECT
      "premium"."ss_quantity" as "ss_quantity",
      "premium"."ss_ticket_number" as "ss_ticket_number",
      coalesce("juicy"."ss_item_id","premium"."ss_item_id") as "ss_item_id"
  FROM
      "premium"
      INNER JOIN "date_dim" as "ss_date_date" on "premium"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "premium"."ss_store_id" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "juicy" on "premium"."ss_item_id" = "juicy"."ss_item_id"
  WHERE
      "ss_date_date"."D_YEAR" = 2001
  ),
  sweltering as (
  SELECT
      "abhorrent"."ss_customer_id" as "ss_customer_id",
      "abhorrent"."ss_ticket_number" as "ss_ticket_number",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_store_store"."S_STATE" as "ss_store_state"
  FROM
      "abhorrent"
      INNER JOIN "date_dim" as "ss_date_date" on "abhorrent"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "abhorrent"."ss_store_id" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001
  ),
  yummy as (
  SELECT
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_store_store"."S_STATE" as "ss_store_state",
      "uneven"."ss_customer_id" as "ss_customer_id"
  FROM
      "uneven"
      INNER JOIN "date_dim" as "ss_date_date" on "uneven"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "uneven"."ss_store_id" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001
  ),
  kaput as (
  SELECT
      "friendly"."_sr_filtered_sr_cust_id" as "sr_filtered_sr_cust_id",
      "friendly"."_sr_filtered_sr_item_id" as "sr_filtered_sr_item_id",
      "friendly"."sr_return_quantity" as "sr_filtered_sr_return_quantity",
      "friendly"."sr_ticket_number" as "sr_filtered_sr_ticket_number"
  FROM
      "friendly"),
  cheerful as (
  SELECT
      "wakeful"."_cs_filtered_cs_cust_id" as "cs_filtered_cs_cust_id",
      "wakeful"."_cs_filtered_cs_item_id" as "cs_filtered_cs_item_id",
      "wakeful"."cs_quantity" as "cs_filtered_cs_quantity"
  FROM
      "wakeful"),
  waggish as (
  SELECT
      "puzzled"."ss_item_id" as "ss_item_id",
      "puzzled"."ss_quantity" as "_virt_agg_avg_6502671563082303",
      "puzzled"."ss_quantity" as "ss_quantity_avg",
      "puzzled"."ss_ticket_number" as "ss_ticket_number",
      CASE WHEN "puzzled"."ss_quantity" IS NOT NULL THEN 1 ELSE 0 END as "ss_quantity_count",
      stddev_samp("puzzled"."ss_quantity") as "_virt_agg_stddev_9388376019437197",
      stddev_samp("puzzled"."ss_quantity") as "ss_quantity_stddev"
  FROM
      "puzzled"),
  thoughtful as (
  SELECT
      "cheerful"."cs_filtered_cs_cust_id" as "cs_filtered_cs_cust_id",
      "cheerful"."cs_filtered_cs_item_id" as "cs_filtered_cs_item_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      "juicy"."ss_item_desc" as "ss_item_desc",
      "juicy"."ss_item_text_id" as "ss_item_text_id",
      "sweltering"."ss_store_state" as "ss_store_state",
      "sweltering"."ss_ticket_number" as "ss_ticket_number",
      coalesce("juicy"."ss_item_id","thoughtful"."cs_filtered_cs_item_id") as "cs_filtered_cs_item_id",
      coalesce("juicy"."ss_item_id","thoughtful"."cs_filtered_cs_item_id") as "ss_item_id",
      coalesce("sweltering"."ss_customer_id","thoughtful"."cs_filtered_cs_cust_id") as "cs_filtered_cs_cust_id",
      coalesce("sweltering"."ss_customer_id","thoughtful"."cs_filtered_cs_cust_id") as "ss_customer_id"
  FROM
      "sweltering"
      LEFT OUTER JOIN "thoughtful" on "sweltering"."ss_customer_id" = "thoughtful"."cs_filtered_cs_cust_id"
      LEFT OUTER JOIN "juicy" on "thoughtful"."cs_filtered_cs_item_id" = "juicy"."ss_item_id"
  WHERE
      "sweltering"."ss_date_year" = 2001
  ),
  concerned as (
  SELECT
      "juicy"."ss_item_desc" as "ss_item_desc",
      "juicy"."ss_item_text_id" as "ss_item_text_id",
      "yummy"."ss_store_state" as "ss_store_state",
      coalesce("juicy"."ss_item_id","thoughtful"."cs_filtered_cs_item_id") as "cs_filtered_cs_item_id",
      coalesce("juicy"."ss_item_id","thoughtful"."cs_filtered_cs_item_id") as "ss_item_id",
      coalesce("thoughtful"."cs_filtered_cs_cust_id","yummy"."ss_customer_id") as "cs_filtered_cs_cust_id",
      coalesce("thoughtful"."cs_filtered_cs_cust_id","yummy"."ss_customer_id") as "ss_customer_id"
  FROM
      "yummy"
      LEFT OUTER JOIN "thoughtful" on "yummy"."ss_customer_id" = "thoughtful"."cs_filtered_cs_cust_id"
      LEFT OUTER JOIN "juicy" on "thoughtful"."cs_filtered_cs_item_id" = "juicy"."ss_item_id"
  WHERE
      "yummy"."ss_date_year" = 2001
  ),
  divergent as (
  SELECT
      "kaput"."sr_filtered_sr_return_quantity" as "sr_filtered_sr_return_quantity",
      "late"."ss_item_desc" as "ss_item_desc",
      "late"."ss_item_text_id" as "ss_item_text_id",
      "late"."ss_store_state" as "ss_store_state",
      coalesce("kaput"."sr_filtered_sr_cust_id","late"."cs_filtered_cs_cust_id") as "cs_filtered_cs_cust_id",
      coalesce("kaput"."sr_filtered_sr_cust_id","late"."cs_filtered_cs_cust_id","late"."ss_customer_id") as "ss_customer_id",
      coalesce("kaput"."sr_filtered_sr_item_id","late"."cs_filtered_cs_item_id") as "cs_filtered_cs_item_id",
      coalesce("kaput"."sr_filtered_sr_item_id","late"."cs_filtered_cs_item_id","late"."ss_item_id") as "sr_filtered_sr_item_id",
      coalesce("kaput"."sr_filtered_sr_item_id","late"."cs_filtered_cs_item_id","late"."ss_item_id") as "ss_item_id",
      coalesce("kaput"."sr_filtered_sr_ticket_number","late"."ss_ticket_number") as "sr_filtered_sr_ticket_number",
      coalesce("kaput"."sr_filtered_sr_ticket_number","late"."ss_ticket_number") as "ss_ticket_number"
  FROM
      "late"
      LEFT OUTER JOIN "kaput" on "late"."ss_customer_id" = "kaput"."sr_filtered_sr_cust_id" AND "late"."ss_item_id" = "kaput"."sr_filtered_sr_item_id" AND "late"."ss_ticket_number" = "kaput"."sr_filtered_sr_ticket_number"),
  young as (
  SELECT
      "concerned"."ss_store_state" as "ss_store_state",
      avg("cheerful"."cs_filtered_cs_quantity") as "_virt_agg_avg_9241713046341503",
      avg("cheerful"."cs_filtered_cs_quantity") as "cs_quantity_avg",
      coalesce("cheerful"."cs_filtered_cs_item_id","concerned"."cs_filtered_cs_item_id","concerned"."ss_item_id") as "cs_filtered_cs_item_id",
      count("cheerful"."cs_filtered_cs_quantity") as "cs_quantity_count",
      stddev_samp("cheerful"."cs_filtered_cs_quantity") as "_virt_agg_stddev_1400433710865420",
      stddev_samp("cheerful"."cs_filtered_cs_quantity") as "cs_quantity_stddev"
  FROM
      "concerned"
      FULL JOIN "cheerful" on "concerned"."ss_customer_id" is not distinct from "cheerful"."cs_filtered_cs_cust_id" AND "concerned"."ss_item_id" = "cheerful"."cs_filtered_cs_item_id"
  GROUP BY
      1,
      4,
      "concerned"."ss_item_desc",
      "concerned"."ss_item_text_id"),
  busy as (
  SELECT
      "divergent"."sr_filtered_sr_ticket_number" as "sr_filtered_sr_ticket_number",
      "divergent"."ss_item_desc" as "ss_item_desc",
      "divergent"."ss_item_text_id" as "ss_item_text_id",
      "divergent"."ss_store_state" as "ss_store_state",
      avg("divergent"."sr_filtered_sr_return_quantity") as "_virt_agg_avg_8197658586842888",
      avg("divergent"."sr_filtered_sr_return_quantity") as "sr_return_qty_avg",
      coalesce("cheerful"."cs_filtered_cs_item_id","divergent"."cs_filtered_cs_item_id","divergent"."ss_item_id") as "cs_filtered_cs_item_id",
      count("divergent"."sr_filtered_sr_return_quantity") as "sr_return_qty_count",
      stddev_samp("divergent"."sr_filtered_sr_return_quantity") as "_virt_agg_stddev_2130913379988202",
      stddev_samp("divergent"."sr_filtered_sr_return_quantity") as "sr_return_qty_stddev"
  FROM
      "divergent"
      FULL JOIN "cheerful" on "divergent"."ss_customer_id" is not distinct from "cheerful"."cs_filtered_cs_cust_id" AND "divergent"."ss_item_id" = "cheerful"."cs_filtered_cs_item_id"
  GROUP BY
      1,
      2,
      3,
      4,
      7),
  protective as (
  SELECT
      "busy"."_virt_agg_avg_8197658586842888" as "_virt_agg_avg_8197658586842888",
      "busy"."_virt_agg_stddev_2130913379988202" as "_virt_agg_stddev_2130913379988202",
      "busy"."sr_filtered_sr_ticket_number" as "sr_filtered_sr_ticket_number",
      "busy"."sr_return_qty_avg" as "sr_return_qty_avg",
      "busy"."sr_return_qty_stddev" as "sr_return_qty_stddev",
      "busy"."ss_item_desc" as "ss_item_desc",
      "busy"."ss_item_text_id" as "ss_item_text_id",
      "young"."_virt_agg_avg_9241713046341503" as "_virt_agg_avg_9241713046341503",
      "young"."_virt_agg_stddev_1400433710865420" as "_virt_agg_stddev_1400433710865420",
      "young"."cs_quantity_avg" as "cs_quantity_avg",
      "young"."cs_quantity_stddev" as "cs_quantity_stddev",
      coalesce("busy"."cs_filtered_cs_item_id","young"."cs_filtered_cs_item_id") as "cs_filtered_cs_item_id",
      coalesce("busy"."sr_return_qty_count",0) as "sr_return_qty_count",
      coalesce("busy"."ss_store_state","young"."ss_store_state") as "ss_store_state",
      coalesce("young"."cs_quantity_count",0) as "cs_quantity_count"
  FROM
      "busy"
      FULL JOIN "young" on "busy"."cs_filtered_cs_item_id" is not distinct from "young"."cs_filtered_cs_item_id" AND "busy"."ss_store_state" is not distinct from "young"."ss_store_state")
  SELECT
      "protective"."ss_item_text_id" as "item_code",
      "protective"."ss_item_desc" as "item_desc",
      "protective"."ss_store_state" as "store_state",
      "waggish"."ss_quantity_count" as "ss_quantity_count",
      "waggish"."ss_quantity_avg" as "ss_quantity_avg",
      "waggish"."ss_quantity_stddev" as "ss_quantity_stddev",
      "waggish"."_virt_agg_stddev_9388376019437197" / "waggish"."_virt_agg_avg_6502671563082303" as "ss_quantity_cv",
      "protective"."sr_return_qty_count" as "sr_return_qty_count",
      "protective"."sr_return_qty_avg" as "sr_return_qty_avg",
      "protective"."sr_return_qty_stddev" as "sr_return_qty_stddev",
      "protective"."_virt_agg_stddev_2130913379988202" / "protective"."_virt_agg_avg_8197658586842888" as "sr_return_qty_cv",
      "protective"."cs_quantity_count" as "cs_quantity_count",
      "protective"."cs_quantity_avg" as "cs_quantity_avg",
      "protective"."cs_quantity_stddev" as "cs_quantity_stddev",
      "protective"."_virt_agg_stddev_1400433710865420" / "protective"."_virt_agg_avg_9241713046341503" as "cs_quantity_cv"
  FROM
      "protective"
      FULL JOIN "waggish" on "protective"."cs_filtered_cs_item_id" is not distinct from "waggish"."ss_item_id" AND "protective"."sr_filtered_sr_ticket_number" is not distinct from "waggish"."ss_ticket_number"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15
  ORDER BY
      "item_code" asc nulls first,
      "item_desc" asc nulls first,
      "store_state" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 7), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 52), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```

### `syntax-parse`

- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale….line_total) as total_sales
order by combined_sales.last_name nulls first, combined_sales.first_name nulls first, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:30
     |
  11 |     count(store_sales.item.id, store_sales.date.id) as pair_count
     |                              ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       count(store_sales.item.id ??? , store_sales.date.id) as pair...

  Write stats: received 3005 chars / 3005 bytes; tail: …'.first_name nulls first, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query67.preql -c import raw.store_sales as ss;

# Sum of (per-line sales price times quantity, treating null as 0)
auto summed_sales <- su…sc nulls first, ss.date.month_of_year asc nulls first, ss.store.text_id asc nulls first, summed_sales asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | having rnk <= 100
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having rnk <= 100 order by ss....

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1722 chars / 1722 bytes; tail: …'summed_sales asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
