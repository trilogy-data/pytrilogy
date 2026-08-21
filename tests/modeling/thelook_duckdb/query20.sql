-- Recorded pairs aggregated, plus each side's extension rows, margin NULL on
-- both extension families.
SELECT
    u.state,
    p.brand,
    sum(oi.sale_price) AS revenue,
    sum(oi.sale_price - p.cost) AS margin
FROM order_items AS oi
JOIN users AS u ON oi.user_id = u.id
JOIN products AS p ON oi.product_id = p.id
GROUP BY 1, 2
UNION ALL
SELECT DISTINCT u.state, NULL, NULL, NULL
FROM users AS u
WHERE u.id NOT IN (SELECT user_id FROM order_items WHERE user_id IS NOT NULL)
UNION ALL
SELECT DISTINCT NULL, p.brand, NULL, NULL
FROM products AS p
WHERE p.id NOT IN (SELECT product_id FROM order_items WHERE product_id IS NOT NULL)
ORDER BY state NULLS FIRST, brand NULLS FIRST;
