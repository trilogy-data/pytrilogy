# Trilogy failure analysis — 20260626-200411

- Run `20260626-200411` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 283 | failed: 43 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 26 | 60% |
| `syntax-parse` | 12 | 28% |
| `join-resolution` | 3 | 7% |
| `cli-misuse` | 1 | 2% |
| `syntax-missing-alias` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 35). The requested concepts split into 8 disconnected subgraphs: {future_by_dow.amt, weekly_by_dow.ws}; {local._virt_filter_amt_1762200955064157}; {local._virt_filter_amt_3492050890241160}; {local._virt_filter_amt_6363462460050721}; {local._virt_filter_amt_6431509971050632}; {local._virt_filter_amt_8874786677853086}; {local._virt_filter_amt_8938350795280199}; {local._virt_filter_amt_9321633374086492}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'paired.ws': matches ['paired.future_by_dow.ws', 'paired.weekly_by_dow.ws']. Qualify the full path to disambiguate.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Unexpected error in query02.preql: (_duckdb.BinderException) Binder Error: Values list "young" does not have a column named "future_by_dow_dow"

  LINE 91:     "young"."future_by_dow_dow" as "weekly_by_dow_dow"
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  thoughtful as (
  SELECT
      "cheerful"."sales_channel" as "sales_channel",
      "cheerful"."sales_date_id" as "sales_date_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "sales_date_date"."D_WEEK_SEQ" as "_future_by_dow_ws",
      sum("cheerful"."sales_ext_sales_price") as "_future_by_dow_amt"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "cheerful"."sales_channel" in ('WEB','CATALOG')

  GROUP BY
      1,
      "sales_date_date"."D_DOW"),
  questionable as (
  SELECT
      CASE WHEN "sales_date_date"."D_YEAR" = 2001 and "thoughtful"."sales_channel" in ('WEB','CATALOG') THEN "sales_date_date"."D_WEEK_SEQ" ELSE NULL END as "ws_2001"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  young as (
  SELECT
      "vacuous"."_future_by_dow_amt" as "future_by_dow_amt",
      "vacuous"."_future_by_dow_ws" as "future_by_dow_ws"
  FROM
      "vacuous"),
  abundant as (
  SELECT
      "sales_date_date"."D_WEEK_SEQ" as "_weekly_by_dow_ws",
      sum("cheerful"."sales_ext_sales_price") as "_weekly_by_dow_amt"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_date_date"."D_WEEK_SEQ" in (select questionable."ws_2001" from questionable where questionable."ws_2001" is not null)

  GROUP BY
      1,
      "sales_date_date"."D_DOW"),
  yummy as (
  SELECT
      "abundant"."_weekly_by_dow_amt" as "weekly_by_dow_amt",
      "abundant"."_weekly_by_dow_ws" as "weekly_by_dow_ws"
  FROM
      "abundant"),
  juicy as (
  SELECT
      "yummy"."weekly_by_dow_amt" as "weekly_by_dow_amt",
      "yummy"."weekly_by_dow_ws" + 53 as "_virt_func_add_8837422533878483",
      "yummy"."weekly_by_dow_ws" as "weekly_by_dow_ws"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      3),
  sparkling as (
  SELECT
      "juicy"."weekly_by_dow_amt" as "_paired_cur_amt",
      "juicy"."weekly_by_dow_ws" as "weekly_by_dow_ws",
      "young"."future_by_dow_amt" as "_paired_fut_amt",
      "young"."future_by_dow_dow" as "weekly_by_dow_dow"
  FROM
      "young"
      INNER JOIN "juicy" on "young"."future_by_dow_ws" = "juicy"."_virt_func_add_8837422533878483"),
  abhorrent as (
  SELECT
      "sparkling"."_paired_cur_amt" as "paired_cur_amt",
      "sparkling"."_paired_fut_amt" as "paired_fut_amt",
      "sparkling"."weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "sparkling"."weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "sparkling"),
  friendly as (
  SELECT
      "abhorrent"."paired_cur_amt" as "paired_cur_amt",
      "abhorrent"."paired_weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "abhorrent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3),
  sweltering as (
  SELECT
      "abhorrent"."paired_fut_amt" as "paired_fut_amt",
      "abhorrent"."paired_weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "abhorrent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3),
  kaput as (
  SELECT
      "friendly"."paired_cur_amt" as "paired_cur_amt",
      "friendly"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 0 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_6459760649490234",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 1 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_9079722592140517",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 2 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_5008439969774102",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 3 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_8505143577535993",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 4 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_8239082100225053",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 5 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_2171186640092100",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 6 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_2465895417234413"
  FROM
      "friendly"),
  late as (
  SELECT
      "sweltering"."paired_fut_amt" as "paired_fut_amt",
      "sweltering"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 0 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_8581641762286967",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 1 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_5416022404348796",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 2 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_1425603778120943",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 3 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_1686311491722625",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 4 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_9502490305456977",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 5 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_9570212195626604",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 6 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_2354370221966360"
  FROM
      "sweltering"),
  divergent as (
  SELECT
      "kaput"."_virt_filter_cur_amt_2171186640092100" as "_virt_filter_cur_amt_2171186640092100",
      "kaput"."_virt_filter_cur_amt_2465895417234413" as "_virt_filter_cur_amt_2465895417234413",
      "kaput"."_virt_filter_cur_amt_5008439969774102" as "_virt_filter_cur_amt_5008439969774102",
      "kaput"."_virt_filter_cur_amt_6459760649490234" as "_virt_filter_cur_amt_6459760649490234",
      "kaput"."_virt_filter_cur_amt_8239082100225053" as "_virt_filter_cur_amt_8239082100225053",
      "kaput"."_virt_filter_cur_amt_8505143577535993" as "_virt_filter_cur_amt_8505143577535993",
      "kaput"."_virt_filter_cur_amt_9079722592140517" as "_virt_filter_cur_amt_9079722592140517",
      "kaput"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "kaput"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "kaput"."paired_cur_amt"),
  macho as (
  SELECT
      "late"."_virt_filter_fut_amt_1425603778120943" as "_virt_filter_fut_amt_1425603778120943",
      "late"."_virt_filter_fut_amt_1686311491722625" as "_virt_filter_fut_amt_1686311491722625",
      "late"."_virt_filter_fut_amt_2354370221966360" as "_virt_filter_fut_amt_2354370221966360",
      "late"."_virt_filter_fut_amt_5416022404348796" as "_virt_filter_fut_amt_5416022404348796",
      "late"."_virt_filter_fut_amt_8581641762286967" as "_virt_filter_fut_amt_8581641762286967",
      "late"."_virt_filter_fut_amt_9502490305456977" as "_virt_filter_fut_amt_9502490305456977",
      "late"."_virt_filter_fut_amt_9570212195626604" as "_virt_filter_fut_amt_9570212195626604",
      "late"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "late"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "late"."paired_fut_amt"),
  busy as (
  SELECT
      "divergent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      sum("divergent"."_virt_filter_cur_amt_2171186640092100") as "_virt_agg_sum_581800842960578",
      sum("divergent"."_virt_filter_cur_amt_2465895417234413") as "_virt_agg_sum_5320672680328783",
      sum("divergent"."_virt_filter_cur_amt_5008439969774102") as "_virt_agg_sum_6072450088613220",
      sum("divergent"."_virt_filter_cur_amt_6459760649490234") as "_virt_agg_sum_2585744207004663",
      sum("divergent"."_virt_filter_cur_amt_8239082100225053") as "_virt_agg_sum_7645958482999674",
      sum("divergent"."_virt_filter_cur_amt_8505143577535993") as "_virt_agg_sum_5037456888834318",
      sum("divergent"."_virt_filter_cur_amt_9079722592140517") as "_virt_agg_sum_8581507683737275"
  FROM
      "divergent"
  GROUP BY
      1),
  scrawny as (
  SELECT
      "macho"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      sum("macho"."_virt_filter_fut_amt_1425603778120943") as "_virt_agg_sum_9760119131641150",
      sum("macho"."_virt_filter_fut_amt_1686311491722625") as "_virt_agg_sum_8940168903938474",
      sum("macho"."_virt_filter_fut_amt_2354370221966360") as "_virt_agg_sum_8278911901597585",
      sum("macho"."_virt_filter_fut_amt_5416022404348796") as "_virt_agg_sum_5850997887160763",
      sum("macho"."_virt_filter_fut_amt_8581641762286967") as "_virt_agg_sum_4884520900087011",
      sum("macho"."_virt_filter_fut_amt_9502490305456977") as "_virt_agg_sum_6060604332798212",
      sum("macho"."_virt_filter_fut_amt_9570212195626604") as "_virt_agg_sum_8856575310780823"
  FROM
      "macho"
  GROUP BY
      1)
  SELECT
      "scrawny"."paired_weekly_by_dow_ws" as "week_seq",
      round("busy"."_virt_agg_sum_2585744207004663" / "scrawny"."_virt_agg_sum_4884520900087011",2) as "sunday",
      round("busy"."_virt_agg_sum_8581507683737275" / "scrawny"."_virt_agg_sum_5850997887160763",2) as "monday",
      round("busy"."_virt_agg_sum_6072450088613220" / "scrawny"."_virt_agg_sum_9760119131641150",2) as "tuesday",
      round("busy"."_virt_agg_sum_5037456888834318" / "scrawny"."_virt_agg_sum_8940168903938474",2) as "wednesday",
      round("busy"."_virt_agg_sum_7645958482999674" / "scrawny"."_virt_agg_sum_6060604332798212",2) as "thursday",
      round("busy"."_virt_agg_sum_581800842960578" / "scrawny"."_virt_agg_sum_8856575310780823",2) as "friday",
      round("busy"."_virt_agg_sum_5320672680328783" / "scrawny"."_virt_agg_sum_8278911901597585",2) as "saturday"
  FROM
      "busy"
      INNER JOIN "scrawny" on "busy"."paired_weekly_by_dow_ws" = "scrawny"."paired_weekly_by_dow_ws"
  ORDER BY
      "week_seq" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Undefined concept: paired.weekly_by_dow.ws (line 48, col 10, in ORDER BY). Suggestions: ['weekly_by_dow.ws', 'future_by_dow.ws', 'paired.weekly_by_dow.dow', 'weekly_by_dow.dow']
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'paired.ws': matches ['paired.future_by_dow.ws', 'paired.weekly_by_dow.ws']. Qualify the full path to disambiguate.
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Unexpected error in query02.preql: (_duckdb.BinderException) Binder Error: Values list "young" does not have a column named "future_by_dow_dow"

  LINE 91:     "young"."future_by_dow_dow" as "weekly_by_dow_dow"
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  thoughtful as (
  SELECT
      "cheerful"."sales_channel" as "sales_channel",
      "cheerful"."sales_date_id" as "sales_date_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "sales_date_date"."D_WEEK_SEQ" as "_future_by_dow_ws",
      sum("cheerful"."sales_ext_sales_price") as "_future_by_dow_amt"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "cheerful"."sales_channel" in ('WEB','CATALOG')

  GROUP BY
      1,
      "sales_date_date"."D_DOW"),
  questionable as (
  SELECT
      CASE WHEN "sales_date_date"."D_YEAR" = 2001 and "thoughtful"."sales_channel" in ('WEB','CATALOG') THEN "sales_date_date"."D_WEEK_SEQ" ELSE NULL END as "ws_2001"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  young as (
  SELECT
      "vacuous"."_future_by_dow_amt" as "future_by_dow_amt",
      "vacuous"."_future_by_dow_ws" as "future_by_dow_ws"
  FROM
      "vacuous"),
  abundant as (
  SELECT
      "sales_date_date"."D_WEEK_SEQ" as "_weekly_by_dow_ws",
      sum("cheerful"."sales_ext_sales_price") as "_weekly_by_dow_amt"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_date_date"."D_WEEK_SEQ" in (select questionable."ws_2001" from questionable where questionable."ws_2001" is not null)

  GROUP BY
      1,
      "sales_date_date"."D_DOW"),
  yummy as (
  SELECT
      "abundant"."_weekly_by_dow_amt" as "weekly_by_dow_amt",
      "abundant"."_weekly_by_dow_ws" as "weekly_by_dow_ws"
  FROM
      "abundant"),
  juicy as (
  SELECT
      "yummy"."weekly_by_dow_amt" as "weekly_by_dow_amt",
      "yummy"."weekly_by_dow_ws" + 53 as "_virt_func_add_8837422533878483",
      "yummy"."weekly_by_dow_ws" as "weekly_by_dow_ws"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      3),
  sparkling as (
  SELECT
      "juicy"."weekly_by_dow_amt" as "_paired_cur_amt",
      "juicy"."weekly_by_dow_ws" as "weekly_by_dow_ws",
      "young"."future_by_dow_amt" as "_paired_fut_amt",
      "young"."future_by_dow_dow" as "weekly_by_dow_dow"
  FROM
      "young"
      INNER JOIN "juicy" on "young"."future_by_dow_ws" = "juicy"."_virt_func_add_8837422533878483"),
  abhorrent as (
  SELECT
      "sparkling"."_paired_cur_amt" as "paired_cur_amt",
      "sparkling"."_paired_fut_amt" as "paired_fut_amt",
      "sparkling"."weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "sparkling"."weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "sparkling"),
  friendly as (
  SELECT
      "abhorrent"."paired_cur_amt" as "paired_cur_amt",
      "abhorrent"."paired_weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "abhorrent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3),
  sweltering as (
  SELECT
      "abhorrent"."paired_fut_amt" as "paired_fut_amt",
      "abhorrent"."paired_weekly_by_dow_dow" as "paired_weekly_by_dow_dow",
      "abhorrent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3),
  kaput as (
  SELECT
      "friendly"."paired_cur_amt" as "paired_cur_amt",
      "friendly"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 0 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_6459760649490234",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 1 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_9079722592140517",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 2 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_5008439969774102",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 3 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_8505143577535993",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 4 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_8239082100225053",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 5 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_2171186640092100",
      CASE WHEN "friendly"."paired_weekly_by_dow_dow" = 6 THEN "friendly"."paired_cur_amt" ELSE NULL END as "_virt_filter_cur_amt_2465895417234413"
  FROM
      "friendly"),
  late as (
  SELECT
      "sweltering"."paired_fut_amt" as "paired_fut_amt",
      "sweltering"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 0 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_8581641762286967",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 1 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_5416022404348796",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 2 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_1425603778120943",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 3 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_1686311491722625",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 4 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_9502490305456977",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 5 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_9570212195626604",
      CASE WHEN "sweltering"."paired_weekly_by_dow_dow" = 6 THEN "sweltering"."paired_fut_amt" ELSE NULL END as "_virt_filter_fut_amt_2354370221966360"
  FROM
      "sweltering"),
  divergent as (
  SELECT
      "kaput"."_virt_filter_cur_amt_2171186640092100" as "_virt_filter_cur_amt_2171186640092100",
      "kaput"."_virt_filter_cur_amt_2465895417234413" as "_virt_filter_cur_amt_2465895417234413",
      "kaput"."_virt_filter_cur_amt_5008439969774102" as "_virt_filter_cur_amt_5008439969774102",
      "kaput"."_virt_filter_cur_amt_6459760649490234" as "_virt_filter_cur_amt_6459760649490234",
      "kaput"."_virt_filter_cur_amt_8239082100225053" as "_virt_filter_cur_amt_8239082100225053",
      "kaput"."_virt_filter_cur_amt_8505143577535993" as "_virt_filter_cur_amt_8505143577535993",
      "kaput"."_virt_filter_cur_amt_9079722592140517" as "_virt_filter_cur_amt_9079722592140517",
      "kaput"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "kaput"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "kaput"."paired_cur_amt"),
  macho as (
  SELECT
      "late"."_virt_filter_fut_amt_1425603778120943" as "_virt_filter_fut_amt_1425603778120943",
      "late"."_virt_filter_fut_amt_1686311491722625" as "_virt_filter_fut_amt_1686311491722625",
      "late"."_virt_filter_fut_amt_2354370221966360" as "_virt_filter_fut_amt_2354370221966360",
      "late"."_virt_filter_fut_amt_5416022404348796" as "_virt_filter_fut_amt_5416022404348796",
      "late"."_virt_filter_fut_amt_8581641762286967" as "_virt_filter_fut_amt_8581641762286967",
      "late"."_virt_filter_fut_amt_9502490305456977" as "_virt_filter_fut_amt_9502490305456977",
      "late"."_virt_filter_fut_amt_9570212195626604" as "_virt_filter_fut_amt_9570212195626604",
      "late"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws"
  FROM
      "late"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "late"."paired_fut_amt"),
  busy as (
  SELECT
      "divergent"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      sum("divergent"."_virt_filter_cur_amt_2171186640092100") as "_virt_agg_sum_581800842960578",
      sum("divergent"."_virt_filter_cur_amt_2465895417234413") as "_virt_agg_sum_5320672680328783",
      sum("divergent"."_virt_filter_cur_amt_5008439969774102") as "_virt_agg_sum_6072450088613220",
      sum("divergent"."_virt_filter_cur_amt_6459760649490234") as "_virt_agg_sum_2585744207004663",
      sum("divergent"."_virt_filter_cur_amt_8239082100225053") as "_virt_agg_sum_7645958482999674",
      sum("divergent"."_virt_filter_cur_amt_8505143577535993") as "_virt_agg_sum_5037456888834318",
      sum("divergent"."_virt_filter_cur_amt_9079722592140517") as "_virt_agg_sum_8581507683737275"
  FROM
      "divergent"
  GROUP BY
      1),
  scrawny as (
  SELECT
      "macho"."paired_weekly_by_dow_ws" as "paired_weekly_by_dow_ws",
      sum("macho"."_virt_filter_fut_amt_1425603778120943") as "_virt_agg_sum_9760119131641150",
      sum("macho"."_virt_filter_fut_amt_1686311491722625") as "_virt_agg_sum_8940168903938474",
      sum("macho"."_virt_filter_fut_amt_2354370221966360") as "_virt_agg_sum_8278911901597585",
      sum("macho"."_virt_filter_fut_amt_5416022404348796") as "_virt_agg_sum_5850997887160763",
      sum("macho"."_virt_filter_fut_amt_8581641762286967") as "_virt_agg_sum_4884520900087011",
      sum("macho"."_virt_filter_fut_amt_9502490305456977") as "_virt_agg_sum_6060604332798212",
      sum("macho"."_virt_filter_fut_amt_9570212195626604") as "_virt_agg_sum_8856575310780823"
  FROM
      "macho"
  GROUP BY
      1)
  SELECT
      "scrawny"."paired_weekly_by_dow_ws" as "week_seq",
      round("busy"."_virt_agg_sum_2585744207004663" / "scrawny"."_virt_agg_sum_4884520900087011",2) as "sunday",
      round("busy"."_virt_agg_sum_8581507683737275" / "scrawny"."_virt_agg_sum_5850997887160763",2) as "monday",
      round("busy"."_virt_agg_sum_6072450088613220" / "scrawny"."_virt_agg_sum_9760119131641150",2) as "tuesday",
      round("busy"."_virt_agg_sum_5037456888834318" / "scrawny"."_virt_agg_sum_8940168903938474",2) as "wednesday",
      round("busy"."_virt_agg_sum_7645958482999674" / "scrawny"."_virt_agg_sum_6060604332798212",2) as "thursday",
      round("busy"."_virt_agg_sum_581800842960578" / "scrawny"."_virt_agg_sum_8856575310780823",2) as "friday",
      round("busy"."_virt_agg_sum_5320672680328783" / "scrawny"."_virt_agg_sum_8278911901597585",2) as "saturday"
  FROM
      "busy"
      INNER JOIN "scrawny" on "busy"."paired_weekly_by_dow_ws" = "scrawny"."paired_weekly_by_dow_ws"
  ORDER BY
      "week_seq" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query02.preql duckdb`

  ```text
  Syntax error in query02.preql: ORDER BY references 'ratios.ws', which is not in the SELECT projection (line 16). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ratios.ws order by ratios.ws asc`.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Have {'RowsetNode<store_2001.cust_code,store_2001.cust_id,store_2001.fname...3 more>': None} and need store_2001.rev > 0 and web_2001.rev > 0 and divide(parenthetical(subtract(web_2002.rev@Grain<web_2002.cust_id>,web_2001.rev@Grain<web_2001.cust_id>)),web_2001.rev@Grain<web_2001.cust_id>) > divide(parenthetical(subtract(store_2002.rev@Grain<store_2002.cust_id>,store_2001.rev@Grain<store_2001.cust_code,store_2001.cust_id>)),store_2001.rev@Grain<store_2001.cust_code,store_2001.cust_id>)
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 25). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {d.month_of_year, d.year, local.total_count, local.total_sales, s.channel}; {i.brand_id, i.category_id, i.class_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Undefined concept: filtered_sales.channel_sales.channel. Suggestions: ['s.channel', 'channel_sales.channel', 'filtered_sales.channel', 'filtered_sales.total_sales']
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'customer_totals.total_spend', 'local.max_spend', which are not in the SELECT projection (line 35). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --customer_totals.total_spend, --local.max_spend
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.id, count(store_sales.line_item) as cnt where store_sales.date.year between 2000 and 2003 and store_sales.customer.id is not null having sum(store_sales.quantity * store_sales.sales_price) > 118133.26 limit 10;`

  ```text
  Syntax error in stdin: HAVING references 'store_sales.quantity', 'store_sales.sales_price', which are not in the SELECT projection (line 2). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.quantity, --store_sales.sales_price
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.store_sales:store_sales select substring(store_sales.item.desc, 1, 30) as desc_prefix where store_sales.date.year between 2000 and 2003 having count(store_sales.ticket_number) by (substring(store_sales.item.desc, 1, 30), store_sales.date.date) > 4 limit 10;`

  ```text
  Syntax error in stdin: HAVING references 'store_sales.ticket_number', 'local._virt_func_substring_2877672313873438', 'store_sales.date.date', which are not in the SELECT projection (line 2). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.ticket_number, --local._virt_func_substring_2877672313873438, --store_sales.date.date
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query54.preql`

  ```text
  Syntax error in query54.preql: Ambiguous reference 'filtered.cust_id': matches ['filtered.all_eligible.cust_id', 'filtered.ss_window.cust_id']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql duckdb`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'y1999.product_name': matches ['y1999.aggregated.product_name', 'y1999.aggregated.store_sales.item.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 51). The requested concepts split into 2 disconnected subgraphs: {yr2001.amt, yr2001.brand_id, yr2001.category_id, yr2001.class_id, yr2001.manufacturer_id, yr2001.qty}; {yr2002.amt, yr2002.qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select 'store channel' as channel, 'store' || store.text_id as outlet, sum(store_sales.ext_sales_price) as s…return_amount), 0) as returns, sum(store_sales.net_profit) - coalesce(sum(store_sales.return_net_loss), 0) as profit, store_sales.date.date limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: store.text_id (line 2, in SELECT). Suggestions: ['store_sales.store.date.text_id', 'store_sales.store.text_id', 'store_sales.item.text_id', 'store_sales.date.text_id', 'store_sales.return_date.text_id', 'store_sales.time.text_id']
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.channel, store_sales.store.text_id limit 10;`

  ```text
  Syntax error in stdin: Undefined concept: store_sales.channel (line 2, col 8, in SELECT). Suggestions: ['store_sales.promotion.channel_tv', 'store_sales.date.id', 'store_sales.quantity']
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year where all_sales.date.year = 2001 group by all_sales.date.week_seq, all_sales.date.year order by all_sales.date.week_seq asc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re all_sales.date.year = 2001 ??? group by all_sales.date.week_s...
  ```
- `trilogy file write query02.preql -e -c import raw.all_sales as sales;

# Aggregate sales by week_seq and day_of_week for Web & Catalog channels only
rowset w….ws in (select sales.date.week_seq where sales.date.year = 2001 and sales.channel in ('WEB', 'CATALOG'))
order by weekly_by_dow.ws asc nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...dow where weekly_by_dow.ws in ??? (select sales.date.week_seq wh...

  Write stats: received 2224 chars / 2224 bytes; tail: …"B', 'CATALOG'))\\norder by weekly_by_dow.ws asc nulls first\\n;\\n".
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Check returns by return_date in period
where sales.return_date.date between '200…e sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
  and sales.return_channel_dim_text_id is not null
select count(*) as cnt;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...im_text_id is not null select ??? count(*) as cnt;

  Write stats: received 528 chars / 528 bytes; tail: …'turn_channel_dim_text_id is not null\\nselect count(*) as cnt;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Sales arm: sales in the period with non-null sale entity
with sales_data as
wher…s_val) as total_returns,
    sum(combined.profit) as net_profit
order by combined.channel_type nulls first, combined.entity_id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:6
     |
  41 |     (sales_data),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? sales_data),     (returns_data...

  Write stats: received 2177 chars / 2177 bytes; tail: …'_type nulls first, combined.entity_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

with combined as union(
    (
        where sales.date.date between '2000-08-23'::…s_val) as total_returns,
    sum(combined.profit) as net_profit
order by combined.channel_type nulls first, combined.entity_id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ss_sales, returns_val, profit) ???  select     combined.channel_...

  Write stats: received 2164 chars / 2164 bytes; tail: …'_type nulls first, combined.entity_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Check returns by return_date - what channels do returns have?
where sales.return…:date
  and sales.return_channel_dim_text_id is not null
select
    sales.channel,
    sales.return_channel_dim_text_id,
    count() as cnt
limit 50;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:11
    |
  9 |     count() as cnt
    |           ^---
    |
    = expected sum_operator
  Location:
  ...hannel_dim_text_id,     count( ??? ) as cnt limit 50;

  Write stats: received 318 chars / 318 bytes; tail: …'les.return_channel_dim_text_id,\\n    count() as cnt\\nlimit 50;'.
  ```
- `trilogy run - duck_db --import raw.date:date`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ate.quarter_name like '2001%' ??? group by date.quarter_name ord...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale… as total_sales
order by
    combined_sales.last_name nulls first,
    combined_sales.first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...nt(store_sales.ticket_number) ??? by substring(store_sales.item....

  Write stats: received 2935 chars / 2935 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.item:item select item.desc, count(store_sales.ticket_number) as cnt where store_sales.date.year between 2000 and 2003 and count(store_sales.ticket_number) by (substring(item.desc, 1, 30), store_sales.date.date) > 4 group by item.desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...), store_sales.date.date) > 4 ??? group by item.desc limit 10;
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel, al...
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

# channel label
channel_label as
case
  when all_sales.channel = 'STORE' then 'sto…all_sales.return_net_loss, 0)) by rollup channel_label, outlet_id as profit
order by
  channel_label nulls first,
  outlet_id nulls first
limit 100;
`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | channel_label as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...s all_sales;  # channel label ??? channel_label as case   when a...

  Write stats: received 1164 chars / 1164 bytes; tail: …'annel_label nulls first,\\n  outlet_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# channel label
channel_label as
case
  when all_sales.channel = 'STORE' then …(all_sales.return_net_loss, 0)) by rollup channel_label, outlet_id as profit
order by
  channel_label nulls first,
  outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | channel_label as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...s all_sales;  # channel label ??? channel_label as case   when a...

  Write stats: received 1163 chars / 1163 bytes; tail: …'hannel_label nulls first,\\n  outlet_id nulls first\\nlimit 100;'.
  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Could not resolve connections for query with output ['s.channel<Purpose.KEY>Derivation.ROOT>', 'i.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'i.class_id<Purpose.PROPERTY>Derivation.ROOT>', 'i.category_id<Purpose.PROPERTY>Derivation.ROOT>', 'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.total_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.overall_avg_sale<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.cust_id<Purpose.KEY>Derivation.BASIC>', 'local.total_ext_sales_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.eligible_ids<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```

### `cli-misuse`

- `trilogy run - --import raw.date:date select distinct date.quarter_name limit 20;`

  ```text
  'select distinct date.quarter_name limit 20;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `syntax-missing-alias`

- `trilogy run - duck_db --import raw.date:date`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(date.id) as id_count`
  Location:
  ....quarter_name, count(date.id) ??? where date.quarter_name like '...
  ```
