SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) AS itemrevenue,
       sum(ws_ext_sales_price)*100.0000/sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales,
     item,
     date_dim
WHERE ws_item_sk = i_item_sk
  AND i_category IN ('Music',
                     'Jewelry',
                     'Shoes')
  AND ws_sold_date_sk = d_date_sk
  AND d_date BETWEEN cast('2001-05-12' AS date) AND cast('2001-06-11' AS date)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100;