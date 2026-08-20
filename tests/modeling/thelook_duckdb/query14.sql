-- Ground truth from the base tables; trilogy should serve this from the
-- precomputed daily_sales rollup instead.
SELECT
    CAST(o.created_at AS DATE) AS order_date,
    sum(oi.sale_price) AS revenue
FROM order_items AS oi
JOIN orders AS o ON o.order_id = oi.order_id
GROUP BY 1
ORDER BY 1;
