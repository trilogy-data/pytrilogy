-- Deliberate divergence from the spec query (still available as PRAGMA tpcds(29)):
-- the spec joins store_sales x store_returns x catalog_sales and sums the store
-- quantities over the resulting pairs, so a purchase matched by three catalog
-- orders counts three times. Summing the catalog side per (billing customer,
-- item) first removes that fan-out, which is what query29.preql and the
-- agent-eval question both ask for. At sf=1 the two agree: no (billing
-- customer, item) pair has more than one catalog line.
WITH catalog_by_customer_item AS (
    SELECT cs_bill_customer_sk AS customer_sk,
           cs_item_sk          AS item_sk,
           sum(cs_quantity)    AS catalog_quantity
    FROM catalog_sales
    JOIN date_dim ordered ON ordered.d_date_sk = cs_sold_date_sk
    WHERE ordered.d_year IN (1999, 2000, 2001)
    GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       sum(ss_quantity) AS store_sales_quantity,
       sum(sr_return_quantity) AS store_returns_quantity,
       sum(catalog_quantity) AS catalog_sales_quantity
FROM store_sales
JOIN date_dim sold ON sold.d_date_sk = ss_sold_date_sk
JOIN store_returns ON sr_customer_sk = ss_customer_sk
                  AND sr_item_sk = ss_item_sk
                  AND sr_ticket_number = ss_ticket_number
JOIN date_dim returned ON returned.d_date_sk = sr_returned_date_sk
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
-- inner: only purchases the customer also ordered from the catalog qualify
JOIN catalog_by_customer_item ON customer_sk = ss_customer_sk
                             AND catalog_by_customer_item.item_sk = ss_item_sk
WHERE sold.d_moy = 9
  AND sold.d_year = 1999
  AND returned.d_moy BETWEEN 9 AND 12
  AND returned.d_year = 1999
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100;
