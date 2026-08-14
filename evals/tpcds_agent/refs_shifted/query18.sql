SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       avg(cast(cs_quantity AS decimal(12, 2)))      agg1,
       avg(cast(cs_list_price AS decimal(12, 2)))    agg2,
       avg(cast(cs_coupon_amt AS decimal(12, 2)))    agg3,
       avg(cast(cs_sales_price AS decimal(12, 2)))   agg4,
       avg(cast(cs_net_profit AS decimal(12, 2)))    agg5,
       avg(cast(c_birth_year AS decimal(12, 2)))     agg6,
       avg(cast(cd1.cd_dep_count AS decimal(12, 2))) agg7
FROM catalog_sales,
     customer_demographics cd1,
     customer_demographics cd2,
     customer,
     customer_address,
     date_dim,
     item
WHERE cs_sold_date_sk = d_date_sk
  AND cs_item_sk = i_item_sk
  AND cs_bill_cdemo_sk = cd1.cd_demo_sk
  AND cs_bill_customer_sk = c_customer_sk
  AND cd1.cd_gender = 'M'
  AND cd1.cd_education_status = 'Primary'
  AND c_current_cdemo_sk = cd2.cd_demo_sk
  AND c_current_addr_sk = ca_address_sk
  AND c_birth_month IN (3,
                        4,
                        5,
                        7,
                        10,
                        11)
  AND d_year = 2000
  AND ca_state IN ('KS',
                   'IL',
                   'MN',
                   'FL',
                   'WI',
                   'SD',
                   'KS')
GROUP BY ROLLUP (i_item_id,
    ca_country,
    ca_state,
    ca_county)
ORDER BY ca_country NULLS FIRST,
    ca_state NULLS FIRST,
    ca_county NULLS FIRST,
    i_item_id NULLS FIRST,
    grouping (ca_country) NULLS FIRST,
    grouping (ca_state) NULLS FIRST,
    grouping (ca_county) NULLS FIRST
LIMIT 100;
