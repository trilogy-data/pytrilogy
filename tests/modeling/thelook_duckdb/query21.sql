SELECT
    oi.user_id,
    oi.product_id,
    sum(oi.sale_price) AS revenue,
    sum(oi.sale_price - p.cost) AS margin
FROM order_items AS oi
LEFT JOIN products AS p ON oi.product_id = p.id
GROUP BY 1, 2
UNION ALL
SELECT u.id, NULL, NULL, NULL
FROM users AS u
WHERE NOT EXISTS (SELECT 1 FROM order_items AS oi WHERE oi.user_id = u.id)
UNION ALL
SELECT NULL, p.id, NULL, NULL
FROM products AS p
WHERE NOT EXISTS (SELECT 1 FROM order_items AS oi WHERE oi.product_id = p.id)
ORDER BY user_id NULLS FIRST, product_id NULLS FIRST;
