# Query 66

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (5 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (1 distinct)
ref rows: 5 (5 distinct)
only in v4 (showing up to 5 of 1):
  99x  (Decimal('26299715.83'), Decimal('7638050.53'), None, Decimal('54868079.17'), Decimal('22641510.44'), None, Decimal('106225508.71'), Decimal('41657522.60'), None, Decimal('23475492.56'), Decimal('8662936.12'), None, Decimal('33568345.58'), Decimal('10548090.79'), None, Decimal('32021210.46'), Decimal('10693665.44'), None, Decimal('18959386.83'), Decimal('5855507.54'), None, Decimal('32584257.34'), Decimal('10093543.12'), None, Decimal('26922303.63'), Decimal('10971533.79'), None, Decimal('71629295.33'), Decimal('22053508.40'), None, Decimal('64473214.71'), Decimal('25375939.36'), None, Decimal('58899426.26'), Decimal('21388170.51'), None, 'DHL,BARIAN', 'Fairview', 'United States', 'Williamson County', 'TN', None, None, 2001)
only in ref (showing up to 5 of 4):
  1x  (Decimal('21861711.42'), Decimal('11435192.44'), 11.694972872414953, Decimal('46001656.08'), Decimal('19480887.65'), 19.923447182259533, Decimal('80833778.46'), Decimal('26034826.13'), 26.626275589673416, Decimal('21536958.51'), Decimal('9895277.02'), 10.120074228845342, Decimal('25436288.73'), Decimal('9076582.12'), 9.282780523774607, Decimal('18571774.48'), Decimal('9728950.13'), 9.949968786658035, Decimal('24387204.30'), Decimal('7425990.18'), 7.594691052345755, Decimal('21455013.31'), Decimal('10149201.72'), 10.37976749537476, Decimal('27855660.62'), Decimal('12140480.46'), 12.416283362327377, Decimal('85849337.50'), Decimal('34761135.80'), 35.55082630470645, Decimal('65397947.70'), Decimal('26633229.62'), 27.23827338673965, Decimal('52571404.42'), Decimal('15926790.81'), 16.28860969720399, 'DHL,BARIAN', 'Fairview', 'United States', 'Williamson County', 'TN', 'Conventional childr', 977787, 2001)
  1x  (Decimal('23715815.35'), Decimal('8458512.50'), 61.07052864899209, Decimal('54327446.24'), Decimal('19653688.19'), 141.8997876595622, Decimal('93172545.66'), Decimal('36466206.13'), 263.28630313925953, Decimal('18171403.56'), Decimal('7089060.25'), 51.18307233004101, Decimal('22661846.38'), Decimal('8252040.77'), 59.57980108877722, Decimal('26900204.08'), Decimal('9849325.60'), 71.11221047767573, Decimal('26899586.32'), Decimal('8963040.01'), 64.7132213510079, Decimal('23810925.02'), Decimal('11453853.25'), 82.69691308554265, Decimal('18522938.63'), Decimal('5983962.36'), 43.20425662796742, Decimal('82941970.54'), Decimal('32941535.20'), 237.83815052272857, Decimal('65974565.30'), Decimal('24149164.06'), 174.35715979321898, Decimal('61151375.34'), Decimal('21168252.88'), 152.83495696875178, 'DHL,BARIAN', 'Fairview', 'United States', 'Williamson County', 'TN', 'Of course ot', 138504, 2001)
  1x  (Decimal('25089244.62'), Decimal('11993334.38'), 40.760103520231645, Decimal('64424128.56'), Decimal('28757671.52'), 97.73476091108681, Decimal('83800658.75'), Decimal('29098121.13'), 98.89180038879562, Decimal('22618273.54'), Decimal('7551129.58'), 25.66299025971819, Decimal('28126416.02'), Decimal('14432988.09'), 49.05142056538496, Decimal('29163328.04'), Decimal('11299132.32'), 38.400814023830726, Decimal('28649511.95'), Decimal('12757632.53'), 43.35761900068651, Decimal('26969264.36'), Decimal('7497389.58'), 25.480351479394514, Decimal('21815906.75'), Decimal('9927074.85'), 33.7377901523236, Decimal('74438918.97'), Decimal('30986289.21'), 105.30885872852957, Decimal('68410689.03'), Decimal('28160781.49'), 95.70619248781615, Decimal('51095094.08'), Decimal('20570121.48'), 69.9088555678659, 'DHL,BARIAN', 'Fairview', 'United States', 'Williamson County', 'TN', 'Social, royal laws m', 294242, 2001)
  1x  (Decimal('22635230.80'), Decimal('11646382.41'), 18.747174832671746, Decimal('55269874.94'), Decimal('24439140.08'), 39.339669238966316, Decimal('94051489.09'), Decimal('31249597.42'), 50.3024583651249, Decimal('27408635.46'), Decimal('11147987.33'), 17.94490856907381, Decimal('26907031.85'), Decimal('12492732.00'), 20.109543263890902, Decimal('25294787.05'), Decimal('7394966.12'), 11.903672561385886, Decimal('26540617.96'), Decimal('10114697.80'), 16.281623027715806, Decimal('27974558.66'), Decimal('9799455.06'), 15.774176976791354, Decimal('29024201.67'), Decimal('8347273.55'), 13.436601264579853, Decimal('70730359.69'), Decimal('22915920.29'), 36.887743249725546, Decimal('72545003.35'), Decimal('24006772.50'), 38.6436874028144, Decimal('56838368.21'), Decimal('24775048.26'), 39.880380436357314, 'DHL,BARIAN', 'Fairview', 'United States', 'Williamson County', 'TN', 'Terms overcome instr', 621234, 2001)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 23960 | 243 | 69.97 ms |
| reference | 20171 | 147 | 33.16 ms |
| v4 / ref | 1.19x | 1.65x | 2.11x |

## Preql

```
import unified_sales as sales;

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
thoughtful as (
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
quizzical as (
SELECT
    :ship_carriers as "ship_carriers"
),
yummy as (
SELECT
    "sales_date_date"."D_MOY" as "sales_date_month_of_year",
    "sales_date_date"."D_YEAR" as "sales_date_year",
    "sales_warehouse_warehouse"."w_city" as "sales_warehouse_city",
    "sales_warehouse_warehouse"."w_country" as "sales_warehouse_country",
    "sales_warehouse_warehouse"."w_county" as "sales_warehouse_county",
    "sales_warehouse_warehouse"."w_state" as "sales_warehouse_state",
    "sales_warehouse_warehouse"."w_warehouse_name" as "sales_warehouse_name",
    "sales_warehouse_warehouse"."w_warehouse_sq_ft" as "sales_warehouse_square_feet",
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_net_paid" as "sales_net_paid",
    "thoughtful"."sales_net_paid_inc_tax" as "sales_net_paid_inc_tax",
    "thoughtful"."sales_quantity" as "sales_quantity",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel",
    "thoughtful"."sales_sales_price" as "sales_sales_price",
    "thoughtful"."sales_warehouse_id" as "sales_warehouse_id"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "sales_warehouse_warehouse" on "thoughtful"."sales_warehouse_id" = "sales_warehouse_warehouse"."w_warehouse_sk"
WHERE
    "thoughtful"."sales_sales_channel" in ('WEB','CATALOG')
),
abhorrent as (
SELECT
    "yummy"."sales_warehouse_city" as "w_city",
    "yummy"."sales_warehouse_country" as "w_country",
    "yummy"."sales_warehouse_county" as "w_county",
    "yummy"."sales_warehouse_id" as "sales_warehouse_id",
    "yummy"."sales_warehouse_name" as "w_warehouse_name",
    "yummy"."sales_warehouse_square_feet" as "w_warehouse_sq_ft",
    "yummy"."sales_warehouse_state" as "w_state"
FROM
    "yummy"),
juicy as (
SELECT
    "yummy"."sales_date_year" as "sales_date_year",
    "yummy"."sales_warehouse_id" as "sales_warehouse_id",
    "yummy"."sales_warehouse_square_feet" as "sales_warehouse_square_feet",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 1 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6761496736742249",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 10 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_1390508309586151",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 11 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4811909514398360",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 12 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7452893998515823",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 2 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7927419533353604",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 3 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3934334814385891",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 4 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_41039473777436",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 5 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3260910144583005",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 6 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2677729335671306",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 7 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7614936436189749",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 8 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3143814459304520",
    sum("yummy"."sales_ext_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 9 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_319471464519035",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 1 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4969900746967002",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 10 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4553214412375650",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 11 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9042867492442160",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 12 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9573906931545777",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 2 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_786159922216741",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 3 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8182545824564782",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 4 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3528659342931447",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 5 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7514497987676013",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 6 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_188554763413587",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 7 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9519241847948353",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 8 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2061365433549155",
    sum("yummy"."sales_net_paid" * CASE WHEN "yummy"."sales_sales_channel" = 'WEB' and "yummy"."sales_date_month_of_year" = 9 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4689061987840461",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 1 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3692655055376189",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 10 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2328396906418239",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 11 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_891548790777269",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 12 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2520745984741884",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 2 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4708623111138392",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 3 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8967002870898331",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 4 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3012721996579085",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 5 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3882217944590141",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 6 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8113055632855599",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 7 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_4042597010133309",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 8 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7961556681450013",
    sum("yummy"."sales_net_paid_inc_tax" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 9 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5640998453949741",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 1 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2212485246453036",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 10 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8249258081069440",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 11 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7368404058205523",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 12 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8526672017232138",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 2 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_3341659793105746",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 3 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_2253606158097547",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 4 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_7001288178406415",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 5 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6555636113729081",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 6 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_8705688610121203",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 7 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_6541755364775288",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 8 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_5462539112795867",
    sum("yummy"."sales_sales_price" * CASE WHEN "yummy"."sales_sales_channel" = 'CATALOG' and "yummy"."sales_date_month_of_year" = 9 THEN "yummy"."sales_quantity" ELSE NULL END) as "_virt_agg_sum_9999335580544067"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3),
sparkling as (
SELECT
    "juicy"."sales_date_year" as "year_"
FROM
    "juicy"),
young as (
SELECT
    "juicy"."sales_warehouse_id" as "sales_warehouse_id",
    ( coalesce("juicy"."_virt_agg_sum_1390508309586151",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_8249258081069440",0) / "juicy"."sales_warehouse_square_feet" ) as "oct_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_2677729335671306",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_8705688610121203",0) / "juicy"."sales_warehouse_square_feet" ) as "jun_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_3143814459304520",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_5462539112795867",0) / "juicy"."sales_warehouse_square_feet" ) as "aug_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_319471464519035",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_9999335580544067",0) / "juicy"."sales_warehouse_square_feet" ) as "sep_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_3260910144583005",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_6555636113729081",0) / "juicy"."sales_warehouse_square_feet" ) as "may_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_3934334814385891",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_2253606158097547",0) / "juicy"."sales_warehouse_square_feet" ) as "mar_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_41039473777436",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_7001288178406415",0) / "juicy"."sales_warehouse_square_feet" ) as "apr_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_4811909514398360",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_7368404058205523",0) / "juicy"."sales_warehouse_square_feet" ) as "nov_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_6761496736742249",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_2212485246453036",0) / "juicy"."sales_warehouse_square_feet" ) as "jan_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_7452893998515823",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_8526672017232138",0) / "juicy"."sales_warehouse_square_feet" ) as "dec_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_7614936436189749",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_6541755364775288",0) / "juicy"."sales_warehouse_square_feet" ) as "jul_sales_per_sq_foot",
    ( coalesce("juicy"."_virt_agg_sum_7927419533353604",0) / "juicy"."sales_warehouse_square_feet" ) + ( coalesce("juicy"."_virt_agg_sum_3341659793105746",0) / "juicy"."sales_warehouse_square_feet" ) as "feb_sales_per_sq_foot"
FROM
    "juicy"),
concerned as (
SELECT
    "juicy"."sales_warehouse_id" as "sales_warehouse_id",
    coalesce("juicy"."_virt_agg_sum_1390508309586151",0) + coalesce("juicy"."_virt_agg_sum_8249258081069440",0) as "oct_sales",
    coalesce("juicy"."_virt_agg_sum_188554763413587",0) + coalesce("juicy"."_virt_agg_sum_8113055632855599",0) as "jun_net",
    coalesce("juicy"."_virt_agg_sum_2061365433549155",0) + coalesce("juicy"."_virt_agg_sum_7961556681450013",0) as "aug_net",
    coalesce("juicy"."_virt_agg_sum_2677729335671306",0) + coalesce("juicy"."_virt_agg_sum_8705688610121203",0) as "jun_sales",
    coalesce("juicy"."_virt_agg_sum_3143814459304520",0) + coalesce("juicy"."_virt_agg_sum_5462539112795867",0) as "aug_sales",
    coalesce("juicy"."_virt_agg_sum_319471464519035",0) + coalesce("juicy"."_virt_agg_sum_9999335580544067",0) as "sep_sales",
    coalesce("juicy"."_virt_agg_sum_3260910144583005",0) + coalesce("juicy"."_virt_agg_sum_6555636113729081",0) as "may_sales",
    coalesce("juicy"."_virt_agg_sum_3528659342931447",0) + coalesce("juicy"."_virt_agg_sum_3012721996579085",0) as "apr_net",
    coalesce("juicy"."_virt_agg_sum_3934334814385891",0) + coalesce("juicy"."_virt_agg_sum_2253606158097547",0) as "mar_sales",
    coalesce("juicy"."_virt_agg_sum_41039473777436",0) + coalesce("juicy"."_virt_agg_sum_7001288178406415",0) as "apr_sales",
    coalesce("juicy"."_virt_agg_sum_4553214412375650",0) + coalesce("juicy"."_virt_agg_sum_2328396906418239",0) as "oct_net",
    coalesce("juicy"."_virt_agg_sum_4689061987840461",0) + coalesce("juicy"."_virt_agg_sum_5640998453949741",0) as "sep_net",
    coalesce("juicy"."_virt_agg_sum_4811909514398360",0) + coalesce("juicy"."_virt_agg_sum_7368404058205523",0) as "nov_sales",
    coalesce("juicy"."_virt_agg_sum_4969900746967002",0) + coalesce("juicy"."_virt_agg_sum_3692655055376189",0) as "jan_net",
    coalesce("juicy"."_virt_agg_sum_6761496736742249",0) + coalesce("juicy"."_virt_agg_sum_2212485246453036",0) as "jan_sales",
    coalesce("juicy"."_virt_agg_sum_7452893998515823",0) + coalesce("juicy"."_virt_agg_sum_8526672017232138",0) as "dec_sales",
    coalesce("juicy"."_virt_agg_sum_7514497987676013",0) + coalesce("juicy"."_virt_agg_sum_3882217944590141",0) as "may_net",
    coalesce("juicy"."_virt_agg_sum_7614936436189749",0) + coalesce("juicy"."_virt_agg_sum_6541755364775288",0) as "jul_sales",
    coalesce("juicy"."_virt_agg_sum_786159922216741",0) + coalesce("juicy"."_virt_agg_sum_4708623111138392",0) as "feb_net",
    coalesce("juicy"."_virt_agg_sum_7927419533353604",0) + coalesce("juicy"."_virt_agg_sum_3341659793105746",0) as "feb_sales",
    coalesce("juicy"."_virt_agg_sum_8182545824564782",0) + coalesce("juicy"."_virt_agg_sum_8967002870898331",0) as "mar_net",
    coalesce("juicy"."_virt_agg_sum_9042867492442160",0) + coalesce("juicy"."_virt_agg_sum_891548790777269",0) as "nov_net",
    coalesce("juicy"."_virt_agg_sum_9519241847948353",0) + coalesce("juicy"."_virt_agg_sum_4042597010133309",0) as "jul_net",
    coalesce("juicy"."_virt_agg_sum_9573906931545777",0) + coalesce("juicy"."_virt_agg_sum_2520745984741884",0) as "dec_net"
FROM
    "juicy")
SELECT
    "abhorrent"."w_warehouse_name" as "w_warehouse_name",
    "abhorrent"."w_warehouse_sq_ft" as "w_warehouse_sq_ft",
    "abhorrent"."w_city" as "w_city",
    "abhorrent"."w_county" as "w_county",
    "abhorrent"."w_state" as "w_state",
    "abhorrent"."w_country" as "w_country",
    "quizzical"."ship_carriers" as "ship_carriers",
    "sparkling"."year_" as "year_",
    "concerned"."jan_sales" as "jan_sales",
    "concerned"."feb_sales" as "feb_sales",
    "concerned"."mar_sales" as "mar_sales",
    "concerned"."apr_sales" as "apr_sales",
    "concerned"."may_sales" as "may_sales",
    "concerned"."jun_sales" as "jun_sales",
    "concerned"."jul_sales" as "jul_sales",
    "concerned"."aug_sales" as "aug_sales",
    "concerned"."sep_sales" as "sep_sales",
    "concerned"."oct_sales" as "oct_sales",
    "concerned"."nov_sales" as "nov_sales",
    "concerned"."dec_sales" as "dec_sales",
    "young"."jan_sales_per_sq_foot" as "jan_sales_per_sq_foot",
    "young"."feb_sales_per_sq_foot" as "feb_sales_per_sq_foot",
    "young"."mar_sales_per_sq_foot" as "mar_sales_per_sq_foot",
    "young"."apr_sales_per_sq_foot" as "apr_sales_per_sq_foot",
    "young"."may_sales_per_sq_foot" as "may_sales_per_sq_foot",
    "young"."jun_sales_per_sq_foot" as "jun_sales_per_sq_foot",
    "young"."jul_sales_per_sq_foot" as "jul_sales_per_sq_foot",
    "young"."aug_sales_per_sq_foot" as "aug_sales_per_sq_foot",
    "young"."sep_sales_per_sq_foot" as "sep_sales_per_sq_foot",
    "young"."oct_sales_per_sq_foot" as "oct_sales_per_sq_foot",
    "young"."nov_sales_per_sq_foot" as "nov_sales_per_sq_foot",
    "young"."dec_sales_per_sq_foot" as "dec_sales_per_sq_foot",
    "concerned"."jan_net" as "jan_net",
    "concerned"."feb_net" as "feb_net",
    "concerned"."mar_net" as "mar_net",
    "concerned"."apr_net" as "apr_net",
    "concerned"."may_net" as "may_net",
    "concerned"."jun_net" as "jun_net",
    "concerned"."jul_net" as "jul_net",
    "concerned"."aug_net" as "aug_net",
    "concerned"."sep_net" as "sep_net",
    "concerned"."oct_net" as "oct_net",
    "concerned"."nov_net" as "nov_net",
    "concerned"."dec_net" as "dec_net"
FROM
    "concerned"
    INNER JOIN "young" on "concerned"."sales_warehouse_id" = "young"."sales_warehouse_id"
    INNER JOIN "abhorrent" on "concerned"."sales_warehouse_id" = "abhorrent"."sales_warehouse_id"
    FULL JOIN "quizzical" on 1=1
    FULL JOIN "sparkling" on 1=1
ORDER BY 
    "abhorrent"."w_warehouse_name" asc nulls first,
    "sparkling"."year_" asc nulls first
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
