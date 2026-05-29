-- Distinct product names of items whose manufacturer-text is shared by at
-- least one item from one of 8 attribute profiles, restricted to manufacturer
-- ids in [1, 500]. The TPC-DS spec's narrow profiles + [738, 778] range
-- yield zero matches at SF=0.1 — these profiles + range were chosen from
-- the actual SF=0.1 item distribution to produce meaningful signal (~75 rows).
SELECT distinct(i_product_name)
FROM item i1
WHERE i_manufact_id BETWEEN 1 AND 500
  AND
    (SELECT count(*) AS item_cnt
     FROM item
     WHERE (i_manufact = i1.i_manufact
            AND ((i_category = 'Books'       AND i_color = 'tan'      AND i_units = 'Oz'     AND i_size = 'N/A')
                 OR (i_category = 'Electronics' AND i_color = 'purple' AND i_units = 'Ton'    AND i_size = 'N/A')
                 OR (i_category = 'Men'         AND i_color = 'misty'  AND i_units = 'Box'    AND i_size = 'medium')
                 OR (i_category = 'Books'       AND i_color = 'medium' AND i_units = 'Tsp'    AND i_size = 'N/A')))
       OR (i_manufact = i1.i_manufact
           AND ((i_category = 'Books'       AND i_color = 'midnight' AND i_units = 'Gram'   AND i_size = 'N/A')
                OR (i_category = 'Books'       AND i_color = 'pale'   AND i_units = 'Pound'  AND i_size = 'N/A')
                OR (i_category = 'Electronics' AND i_color = 'khaki'  AND i_units = 'Pallet' AND i_size = 'N/A')
                OR (i_category = 'Electronics' AND i_color = 'mint'   AND i_units = 'Gross'  AND i_size = 'N/A')))) > 0
ORDER BY i_product_name
LIMIT 100;
