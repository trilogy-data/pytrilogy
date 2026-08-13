SELECT ca_zip,
       sum(cs_sales_price)
FROM catalog_sales,
     customer,
     customer_address,
     date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (SUBSTRING(ca_zip, 1, 5) IN ('54975',
                                '69532',
                                '75804',
                                '71087',
                                '68877',
                                '60169',
                                '74289',
                                '50411',
                                '56614')
       OR ca_state IN ('NC',
                       'IL',
                       'MN')
       OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 3
  AND d_year = 2000
GROUP BY ca_zip
ORDER BY ca_zip NULLS FIRST
LIMIT 100;

