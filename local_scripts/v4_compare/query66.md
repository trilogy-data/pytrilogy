# Query 66

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 22787 | 209 | 48.76 ms |
| reference | 20171 | 147 | 29.88 ms |
| v4 / ref | 1.13x | 1.42x | 1.63x |

## Preql

```
import all_sales as sales;

# Per-channel monthly aggregate. Web uses ext_sales_price + net_paid;
# catalog uses sales_price + net_paid_inc_tax (matches the reference query).
# Store has no warehouse/ship_mode mappings, so the planner uses the
# 2-channel (web+catalog) union for any aggregate that touches them.
def web_month_sum(metric, month) -> sum(metric * sales.quantity ? sales.sales_channel = 'WEB' and sales.date.month_of_year = month)
    by sales.warehouse.id, sales.date.year;
def cat_month_sum(metric, month) -> sum(metric * sales.quantity ? sales.sales_channel = 'CATALOG' and sales.date.month_of_year = month)
    by sales.warehouse.id, sales.date.year;
def channel_total(ws, cs) -> coalesce(ws, 0) + coalesce(cs, 0);
def per_sqft(ws, cs) -> coalesce(ws, 0) / sales.warehouse.square_feet + coalesce(cs, 0) / sales.warehouse.square_feet;
def ws_sales(month) -> @web_month_sum(sales.ext_sales_price, month);
def cs_sales(month) -> @cat_month_sum(sales.sales_price, month);
def ws_net(month) -> @web_month_sum(sales.net_paid, month);
def cs_net(month) -> @cat_month_sum(sales.net_paid_inc_tax, month);
def month_sales(month) -> @channel_total(@ws_sales(month), @cs_sales(month));
def month_net(month) -> @channel_total(@ws_net(month), @cs_net(month));
def month_sales_per_sqft(month) -> @per_sqft(@ws_sales(month), @cs_sales(month));

where
    sales.sales_channel in ('WEB', 'CATALOG')
    and sales.date.year = 2001
    and sales.time.time between 30838 and 59638
    and sales.ship_mode.carrier in ('DHL', 'BARIAN')
    and sales.warehouse.id is not null
select
    --sales.warehouse.id,
    sales.warehouse.name as w_warehouse_name,
    sales.warehouse.square_feet as w_warehouse_sq_ft,
    sales.warehouse.city as w_city,
    sales.warehouse.county as w_county,
    sales.warehouse.state as w_state,
    sales.warehouse.country as w_country,
    'DHL,BARIAN' as ship_carriers,
    sales.date.year as year_,
    @month_sales(1) as jan_sales,
    @month_sales(2) as feb_sales,
    @month_sales(3) as mar_sales,
    @month_sales(4) as apr_sales,
    @month_sales(5) as may_sales,
    @month_sales(6) as jun_sales,
    @month_sales(7) as jul_sales,
    @month_sales(8) as aug_sales,
    @month_sales(9) as sep_sales,
    @month_sales(10) as oct_sales,
    @month_sales(11) as nov_sales,
    @month_sales(12) as dec_sales,
    @month_sales_per_sqft(1) as jan_sales_per_sq_foot,
    @month_sales_per_sqft(2) as feb_sales_per_sq_foot,
    @month_sales_per_sqft(3) as mar_sales_per_sq_foot,
    @month_sales_per_sqft(4) as apr_sales_per_sq_foot,
    @month_sales_per_sqft(5) as may_sales_per_sq_foot,
    @month_sales_per_sqft(6) as jun_sales_per_sq_foot,
    @month_sales_per_sqft(7) as jul_sales_per_sq_foot,
    @month_sales_per_sqft(8) as aug_sales_per_sq_foot,
    @month_sales_per_sqft(9) as sep_sales_per_sq_foot,
    @month_sales_per_sqft(10) as oct_sales_per_sq_foot,
    @month_sales_per_sqft(11) as nov_sales_per_sq_foot,
    @month_sales_per_sqft(12) as dec_sales_per_sq_foot,
    @month_net(1) as jan_net,
    @month_net(2) as feb_net,
    @month_net(3) as mar_net,
    @month_net(4) as apr_net,
    @month_net(5) as may_net,
    @month_net(6) as jun_net,
    @month_net(7) as jul_net,
    @month_net(8) as aug_net,
    @month_net(9) as sep_net,
    @month_net(10) as oct_net,
    @month_net(11) as nov_net,
    @month_net(12) as dec_net,
order by
    w_warehouse_name asc nulls first,
    year_ asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_NET_PAID_INC_TAX" as "sales_net_paid_inc_tax",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SALES_PRICE" as "sales_sales_price",
    "sales_catalog_sales_unified"."CS_WAREHOUSE_SK" as "sales_warehouse_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "sales_ship_mode_ship_mode" on "sales_catalog_sales_unified"."CS_SHIP_MODE_SK" = "sales_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."warehouse" as "sales_warehouse_warehouse" on "sales_catalog_sales_unified"."CS_WAREHOUSE_SK" = "sales_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "sales_catalog_sales_unified"."CS_WAREHOUSE_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "sales_time_time"."T_TIME" BETWEEN 30838 AND 59638

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_NET_PAID_INC_TAX" as "sales_net_paid_inc_tax",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price",
    "sales_web_sales_unified"."WS_WAREHOUSE_SK" as "sales_warehouse_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "sales_ship_mode_ship_mode" on "sales_web_sales_unified"."WS_SHIP_MODE_SK" = "sales_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_web_sales_unified"."WS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
    INNER JOIN "memory"."warehouse" as "sales_warehouse_warehouse" on "sales_web_sales_unified"."WS_WAREHOUSE_SK" = "sales_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "sales_web_sales_unified"."WS_WAREHOUSE_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "sales_time_time"."T_TIME" BETWEEN 30838 AND 59638
),
concerned as (
SELECT
    "sales_warehouse_warehouse"."w_city" as "w_city",
    "sales_warehouse_warehouse"."w_country" as "w_country",
    "sales_warehouse_warehouse"."w_county" as "w_county",
    "sales_warehouse_warehouse"."w_state" as "w_state",
    "sales_warehouse_warehouse"."w_warehouse_name" as "w_warehouse_name",
    "sales_warehouse_warehouse"."w_warehouse_sk" as "sales_warehouse_id",
    "sales_warehouse_warehouse"."w_warehouse_sq_ft" as "w_warehouse_sq_ft"
FROM
    "memory"."warehouse" as "sales_warehouse_warehouse"),
uneven as (
SELECT
    "cheerful"."sales_warehouse_id" as "sales_warehouse_id",
    "sales_date_date"."D_YEAR" as "sales_date_year",
    "sales_warehouse_warehouse"."w_warehouse_sq_ft" as "sales_warehouse_square_feet",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6761496736742249",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_1390508309586151",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4811909514398360",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7452893998515823",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7927419533353604",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3934334814385891",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_41039473777436",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3260910144583005",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2677729335671306",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7614936436189749",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3143814459304520",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_319471464519035",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4969900746967002",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4553214412375650",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9042867492442160",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9573906931545777",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_786159922216741",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8182545824564782",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3528659342931447",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7514497987676013",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_188554763413587",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9519241847948353",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2061365433549155",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4689061987840461",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3692655055376189",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2328396906418239",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_891548790777269",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2520745984741884",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4708623111138392",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8967002870898331",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3012721996579085",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3882217944590141",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8113055632855599",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4042597010133309",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7961556681450013",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5640998453949741",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2212485246453036",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8249258081069440",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7368404058205523",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8526672017232138",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3341659793105746",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2253606158097547",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7001288178406415",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6555636113729081",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8705688610121203",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6541755364775288",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5462539112795867",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9999335580544067"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "sales_warehouse_warehouse" on "cheerful"."sales_warehouse_id" = "sales_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "cheerful"."sales_sales_channel" in ('WEB','CATALOG')

GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    "uneven"."sales_date_year" as "year_",
    "uneven"."sales_warehouse_id" as "sales_warehouse_id",
    ( coalesce("uneven"."_virt_agg_sum_1390508309586151",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_8249258081069440",0) / "uneven"."sales_warehouse_square_feet" ) as "oct_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_2677729335671306",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_8705688610121203",0) / "uneven"."sales_warehouse_square_feet" ) as "jun_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_3143814459304520",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_5462539112795867",0) / "uneven"."sales_warehouse_square_feet" ) as "aug_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_319471464519035",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_9999335580544067",0) / "uneven"."sales_warehouse_square_feet" ) as "sep_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_3260910144583005",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_6555636113729081",0) / "uneven"."sales_warehouse_square_feet" ) as "may_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_3934334814385891",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_2253606158097547",0) / "uneven"."sales_warehouse_square_feet" ) as "mar_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_41039473777436",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_7001288178406415",0) / "uneven"."sales_warehouse_square_feet" ) as "apr_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_4811909514398360",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_7368404058205523",0) / "uneven"."sales_warehouse_square_feet" ) as "nov_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_6761496736742249",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_2212485246453036",0) / "uneven"."sales_warehouse_square_feet" ) as "jan_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_7452893998515823",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_8526672017232138",0) / "uneven"."sales_warehouse_square_feet" ) as "dec_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_7614936436189749",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_6541755364775288",0) / "uneven"."sales_warehouse_square_feet" ) as "jul_sales_per_sq_foot",
    ( coalesce("uneven"."_virt_agg_sum_7927419533353604",0) / "uneven"."sales_warehouse_square_feet" ) + ( coalesce("uneven"."_virt_agg_sum_3341659793105746",0) / "uneven"."sales_warehouse_square_feet" ) as "feb_sales_per_sq_foot",
    :ship_carriers as "ship_carriers",
    coalesce("uneven"."_virt_agg_sum_1390508309586151",0) + coalesce("uneven"."_virt_agg_sum_8249258081069440",0) as "oct_sales",
    coalesce("uneven"."_virt_agg_sum_188554763413587",0) + coalesce("uneven"."_virt_agg_sum_8113055632855599",0) as "jun_net",
    coalesce("uneven"."_virt_agg_sum_2061365433549155",0) + coalesce("uneven"."_virt_agg_sum_7961556681450013",0) as "aug_net",
    coalesce("uneven"."_virt_agg_sum_2677729335671306",0) + coalesce("uneven"."_virt_agg_sum_8705688610121203",0) as "jun_sales",
    coalesce("uneven"."_virt_agg_sum_3143814459304520",0) + coalesce("uneven"."_virt_agg_sum_5462539112795867",0) as "aug_sales",
    coalesce("uneven"."_virt_agg_sum_319471464519035",0) + coalesce("uneven"."_virt_agg_sum_9999335580544067",0) as "sep_sales",
    coalesce("uneven"."_virt_agg_sum_3260910144583005",0) + coalesce("uneven"."_virt_agg_sum_6555636113729081",0) as "may_sales",
    coalesce("uneven"."_virt_agg_sum_3528659342931447",0) + coalesce("uneven"."_virt_agg_sum_3012721996579085",0) as "apr_net",
    coalesce("uneven"."_virt_agg_sum_3934334814385891",0) + coalesce("uneven"."_virt_agg_sum_2253606158097547",0) as "mar_sales",
    coalesce("uneven"."_virt_agg_sum_41039473777436",0) + coalesce("uneven"."_virt_agg_sum_7001288178406415",0) as "apr_sales",
    coalesce("uneven"."_virt_agg_sum_4553214412375650",0) + coalesce("uneven"."_virt_agg_sum_2328396906418239",0) as "oct_net",
    coalesce("uneven"."_virt_agg_sum_4689061987840461",0) + coalesce("uneven"."_virt_agg_sum_5640998453949741",0) as "sep_net",
    coalesce("uneven"."_virt_agg_sum_4811909514398360",0) + coalesce("uneven"."_virt_agg_sum_7368404058205523",0) as "nov_sales",
    coalesce("uneven"."_virt_agg_sum_4969900746967002",0) + coalesce("uneven"."_virt_agg_sum_3692655055376189",0) as "jan_net",
    coalesce("uneven"."_virt_agg_sum_6761496736742249",0) + coalesce("uneven"."_virt_agg_sum_2212485246453036",0) as "jan_sales",
    coalesce("uneven"."_virt_agg_sum_7452893998515823",0) + coalesce("uneven"."_virt_agg_sum_8526672017232138",0) as "dec_sales",
    coalesce("uneven"."_virt_agg_sum_7514497987676013",0) + coalesce("uneven"."_virt_agg_sum_3882217944590141",0) as "may_net",
    coalesce("uneven"."_virt_agg_sum_7614936436189749",0) + coalesce("uneven"."_virt_agg_sum_6541755364775288",0) as "jul_sales",
    coalesce("uneven"."_virt_agg_sum_786159922216741",0) + coalesce("uneven"."_virt_agg_sum_4708623111138392",0) as "feb_net",
    coalesce("uneven"."_virt_agg_sum_7927419533353604",0) + coalesce("uneven"."_virt_agg_sum_3341659793105746",0) as "feb_sales",
    coalesce("uneven"."_virt_agg_sum_8182545824564782",0) + coalesce("uneven"."_virt_agg_sum_8967002870898331",0) as "mar_net",
    coalesce("uneven"."_virt_agg_sum_9042867492442160",0) + coalesce("uneven"."_virt_agg_sum_891548790777269",0) as "nov_net",
    coalesce("uneven"."_virt_agg_sum_9519241847948353",0) + coalesce("uneven"."_virt_agg_sum_4042597010133309",0) as "jul_net",
    coalesce("uneven"."_virt_agg_sum_9573906931545777",0) + coalesce("uneven"."_virt_agg_sum_2520745984741884",0) as "dec_net"
FROM
    "uneven")
SELECT
    "concerned"."w_warehouse_name" as "w_warehouse_name",
    "concerned"."w_warehouse_sq_ft" as "w_warehouse_sq_ft",
    "concerned"."w_city" as "w_city",
    "concerned"."w_county" as "w_county",
    "concerned"."w_state" as "w_state",
    "concerned"."w_country" as "w_country",
    "vacuous"."ship_carriers" as "ship_carriers",
    "vacuous"."year_" as "year_",
    "vacuous"."jan_sales" as "jan_sales",
    "vacuous"."feb_sales" as "feb_sales",
    "vacuous"."mar_sales" as "mar_sales",
    "vacuous"."apr_sales" as "apr_sales",
    "vacuous"."may_sales" as "may_sales",
    "vacuous"."jun_sales" as "jun_sales",
    "vacuous"."jul_sales" as "jul_sales",
    "vacuous"."aug_sales" as "aug_sales",
    "vacuous"."sep_sales" as "sep_sales",
    "vacuous"."oct_sales" as "oct_sales",
    "vacuous"."nov_sales" as "nov_sales",
    "vacuous"."dec_sales" as "dec_sales",
    "vacuous"."jan_sales_per_sq_foot" as "jan_sales_per_sq_foot",
    "vacuous"."feb_sales_per_sq_foot" as "feb_sales_per_sq_foot",
    "vacuous"."mar_sales_per_sq_foot" as "mar_sales_per_sq_foot",
    "vacuous"."apr_sales_per_sq_foot" as "apr_sales_per_sq_foot",
    "vacuous"."may_sales_per_sq_foot" as "may_sales_per_sq_foot",
    "vacuous"."jun_sales_per_sq_foot" as "jun_sales_per_sq_foot",
    "vacuous"."jul_sales_per_sq_foot" as "jul_sales_per_sq_foot",
    "vacuous"."aug_sales_per_sq_foot" as "aug_sales_per_sq_foot",
    "vacuous"."sep_sales_per_sq_foot" as "sep_sales_per_sq_foot",
    "vacuous"."oct_sales_per_sq_foot" as "oct_sales_per_sq_foot",
    "vacuous"."nov_sales_per_sq_foot" as "nov_sales_per_sq_foot",
    "vacuous"."dec_sales_per_sq_foot" as "dec_sales_per_sq_foot",
    "vacuous"."jan_net" as "jan_net",
    "vacuous"."feb_net" as "feb_net",
    "vacuous"."mar_net" as "mar_net",
    "vacuous"."apr_net" as "apr_net",
    "vacuous"."may_net" as "may_net",
    "vacuous"."jun_net" as "jun_net",
    "vacuous"."jul_net" as "jul_net",
    "vacuous"."aug_net" as "aug_net",
    "vacuous"."sep_net" as "sep_net",
    "vacuous"."oct_net" as "oct_net",
    "vacuous"."nov_net" as "nov_net",
    "vacuous"."dec_net" as "dec_net"
FROM
    "vacuous"
    INNER JOIN "concerned" on "vacuous"."sales_warehouse_id" = "concerned"."sales_warehouse_id"
ORDER BY 
    "concerned"."w_warehouse_name" asc nulls first,
    "vacuous"."year_" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_NET_PAID_INC_TAX" as "sales_net_paid_inc_tax",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_catalog_sales_unified"."CS_SALES_PRICE" as "sales_sales_price",
    "sales_catalog_sales_unified"."CS_WAREHOUSE_SK" as "sales_warehouse_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "sales_ship_mode_ship_mode" on "sales_catalog_sales_unified"."CS_SHIP_MODE_SK" = "sales_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_catalog_sales_unified"."CS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_catalog_sales_unified"."CS_WAREHOUSE_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "sales_time_time"."T_TIME" BETWEEN 30838 AND 59638

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_NET_PAID_INC_TAX" as "sales_net_paid_inc_tax",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price",
    "sales_web_sales_unified"."WS_WAREHOUSE_SK" as "sales_warehouse_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."ship_mode" as "sales_ship_mode_ship_mode" on "sales_web_sales_unified"."WS_SHIP_MODE_SK" = "sales_ship_mode_ship_mode"."SM_SHIP_MODE_SK"
    INNER JOIN "memory"."time_dim" as "sales_time_time" on "sales_web_sales_unified"."WS_SOLD_TIME_SK" = "sales_time_time"."T_TIME_SK"
WHERE
    "sales_web_sales_unified"."WS_WAREHOUSE_SK" is not null and "sales_date_date"."D_YEAR" = 2001 and "sales_ship_mode_ship_mode"."SM_CARRIER" in ('DHL','BARIAN') and "sales_time_time"."T_TIME" BETWEEN 30838 AND 59638
),
abundant as (
SELECT
    "cheerful"."sales_warehouse_id" as "sales_warehouse_id",
    "sales_date_date"."D_YEAR" as "sales_date_year",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6761496736742249",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_1390508309586151",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4811909514398360",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7452893998515823",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7927419533353604",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3934334814385891",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_41039473777436",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3260910144583005",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2677729335671306",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7614936436189749",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3143814459304520",
    sum("cheerful"."sales_ext_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_319471464519035",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4969900746967002",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4553214412375650",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9042867492442160",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9573906931545777",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_786159922216741",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8182545824564782",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3528659342931447",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7514497987676013",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_188554763413587",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9519241847948353",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2061365433549155",
    sum("cheerful"."sales_net_paid" * CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4689061987840461",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3692655055376189",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2328396906418239",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_891548790777269",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2520745984741884",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4708623111138392",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8967002870898331",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3012721996579085",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3882217944590141",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8113055632855599",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4042597010133309",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7961556681450013",
    sum("cheerful"."sales_net_paid_inc_tax" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5640998453949741",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 1 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2212485246453036",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 10 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8249258081069440",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 11 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7368404058205523",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 12 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8526672017232138",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 2 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3341659793105746",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 3 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2253606158097547",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 4 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7001288178406415",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 5 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6555636113729081",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 6 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8705688610121203",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 7 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6541755364775288",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 8 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5462539112795867",
    sum("cheerful"."sales_sales_price" * CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MOY" = 9 THEN "cheerful"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9999335580544067"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1,
    2)
SELECT
    "sales_warehouse_warehouse"."w_warehouse_name" as "w_warehouse_name",
    "sales_warehouse_warehouse"."w_warehouse_sq_ft" as "w_warehouse_sq_ft",
    "sales_warehouse_warehouse"."w_city" as "w_city",
    "sales_warehouse_warehouse"."w_county" as "w_county",
    "sales_warehouse_warehouse"."w_state" as "w_state",
    "sales_warehouse_warehouse"."w_country" as "w_country",
    :ship_carriers as "ship_carriers",
    "abundant"."sales_date_year" as "year_",
    coalesce("abundant"."_virt_agg_sum_6761496736742249",0) + coalesce("abundant"."_virt_agg_sum_2212485246453036",0) as "jan_sales",
    coalesce("abundant"."_virt_agg_sum_7927419533353604",0) + coalesce("abundant"."_virt_agg_sum_3341659793105746",0) as "feb_sales",
    coalesce("abundant"."_virt_agg_sum_3934334814385891",0) + coalesce("abundant"."_virt_agg_sum_2253606158097547",0) as "mar_sales",
    coalesce("abundant"."_virt_agg_sum_41039473777436",0) + coalesce("abundant"."_virt_agg_sum_7001288178406415",0) as "apr_sales",
    coalesce("abundant"."_virt_agg_sum_3260910144583005",0) + coalesce("abundant"."_virt_agg_sum_6555636113729081",0) as "may_sales",
    coalesce("abundant"."_virt_agg_sum_2677729335671306",0) + coalesce("abundant"."_virt_agg_sum_8705688610121203",0) as "jun_sales",
    coalesce("abundant"."_virt_agg_sum_7614936436189749",0) + coalesce("abundant"."_virt_agg_sum_6541755364775288",0) as "jul_sales",
    coalesce("abundant"."_virt_agg_sum_3143814459304520",0) + coalesce("abundant"."_virt_agg_sum_5462539112795867",0) as "aug_sales",
    coalesce("abundant"."_virt_agg_sum_319471464519035",0) + coalesce("abundant"."_virt_agg_sum_9999335580544067",0) as "sep_sales",
    coalesce("abundant"."_virt_agg_sum_1390508309586151",0) + coalesce("abundant"."_virt_agg_sum_8249258081069440",0) as "oct_sales",
    coalesce("abundant"."_virt_agg_sum_4811909514398360",0) + coalesce("abundant"."_virt_agg_sum_7368404058205523",0) as "nov_sales",
    coalesce("abundant"."_virt_agg_sum_7452893998515823",0) + coalesce("abundant"."_virt_agg_sum_8526672017232138",0) as "dec_sales",
    ( coalesce("abundant"."_virt_agg_sum_6761496736742249",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_2212485246453036",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "jan_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_7927419533353604",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_3341659793105746",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "feb_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_3934334814385891",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_2253606158097547",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "mar_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_41039473777436",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_7001288178406415",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "apr_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_3260910144583005",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_6555636113729081",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "may_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_2677729335671306",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_8705688610121203",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "jun_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_7614936436189749",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_6541755364775288",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "jul_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_3143814459304520",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_5462539112795867",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "aug_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_319471464519035",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_9999335580544067",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "sep_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_1390508309586151",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_8249258081069440",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "oct_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_4811909514398360",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_7368404058205523",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "nov_sales_per_sq_foot",
    ( coalesce("abundant"."_virt_agg_sum_7452893998515823",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) + ( coalesce("abundant"."_virt_agg_sum_8526672017232138",0) / "sales_warehouse_warehouse"."w_warehouse_sq_ft" ) as "dec_sales_per_sq_foot",
    coalesce("abundant"."_virt_agg_sum_4969900746967002",0) + coalesce("abundant"."_virt_agg_sum_3692655055376189",0) as "jan_net",
    coalesce("abundant"."_virt_agg_sum_786159922216741",0) + coalesce("abundant"."_virt_agg_sum_4708623111138392",0) as "feb_net",
    coalesce("abundant"."_virt_agg_sum_8182545824564782",0) + coalesce("abundant"."_virt_agg_sum_8967002870898331",0) as "mar_net",
    coalesce("abundant"."_virt_agg_sum_3528659342931447",0) + coalesce("abundant"."_virt_agg_sum_3012721996579085",0) as "apr_net",
    coalesce("abundant"."_virt_agg_sum_7514497987676013",0) + coalesce("abundant"."_virt_agg_sum_3882217944590141",0) as "may_net",
    coalesce("abundant"."_virt_agg_sum_188554763413587",0) + coalesce("abundant"."_virt_agg_sum_8113055632855599",0) as "jun_net",
    coalesce("abundant"."_virt_agg_sum_9519241847948353",0) + coalesce("abundant"."_virt_agg_sum_4042597010133309",0) as "jul_net",
    coalesce("abundant"."_virt_agg_sum_2061365433549155",0) + coalesce("abundant"."_virt_agg_sum_7961556681450013",0) as "aug_net",
    coalesce("abundant"."_virt_agg_sum_4689061987840461",0) + coalesce("abundant"."_virt_agg_sum_5640998453949741",0) as "sep_net",
    coalesce("abundant"."_virt_agg_sum_4553214412375650",0) + coalesce("abundant"."_virt_agg_sum_2328396906418239",0) as "oct_net",
    coalesce("abundant"."_virt_agg_sum_9042867492442160",0) + coalesce("abundant"."_virt_agg_sum_891548790777269",0) as "nov_net",
    coalesce("abundant"."_virt_agg_sum_9573906931545777",0) + coalesce("abundant"."_virt_agg_sum_2520745984741884",0) as "dec_net"
FROM
    "abundant"
    INNER JOIN "memory"."warehouse" as "sales_warehouse_warehouse" on "abundant"."sales_warehouse_id" = "sales_warehouse_warehouse"."w_warehouse_sk"
ORDER BY 
    "w_warehouse_name" asc nulls first,
    "year_" asc nulls first
LIMIT (100)
```
