-- Union-of-branches at the four-key grain: every fact row once, then one
-- extension row per never-ordered user and per never-sold product.
WITH fact AS (
    SELECT
        oi.order_id,
        oi.id,
        oi.user_id,
        oi.product_id,
        oi.sale_price AS revenue,
        oi.sale_price - p.cost AS margin,
        sum(oi.sale_price) OVER (PARTITION BY oi.order_id) AS total_order_revenue
    FROM order_items AS oi
    LEFT JOIN products AS p ON oi.product_id = p.id
)
SELECT * FROM fact
UNION ALL
SELECT NULL, NULL, u.id, NULL, NULL, NULL, NULL
FROM users AS u
WHERE NOT EXISTS (SELECT 1 FROM order_items AS oi WHERE oi.user_id = u.id)
UNION ALL
SELECT NULL, NULL, NULL, p.id, NULL, NULL, NULL
FROM products AS p
WHERE NOT EXISTS (SELECT 1 FROM order_items AS oi WHERE oi.product_id = p.id)
ORDER BY order_id NULLS FIRST, id NULLS FIRST, user_id NULLS FIRST, product_id NULLS FIRST;
