-- Total ext_discount on catalog sales whose per-line discount exceeds 1.3x
-- the per-item average over the 2000-01-27..2000-04-26 window, restricted to
-- manufacturer 48. The TPC-DS spec uses manufacturer 977, which has zero
-- catalog sales in this window at SF=0.1 — 48 is the highest-volume
-- alternative that actually produces signal at our default scale factor.
SELECT sum(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales,
     item,
     date_dim
WHERE i_manufact_id = 48
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt >
    (SELECT 1.3 * avg(cs_ext_discount_amt)
     FROM catalog_sales,
          date_dim
     WHERE cs_item_sk = i_item_sk
       AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
       AND d_date_sk = cs_sold_date_sk)
LIMIT 100;
