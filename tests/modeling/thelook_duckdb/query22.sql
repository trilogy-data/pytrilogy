SELECT
    o.status,
    oi.user_id,
    sum(oi.sale_price) AS revenue,
    sum(oi.sale_price - p.cost) AS margin
FROM order_items AS oi
JOIN orders AS o ON oi.order_id = o.order_id
LEFT JOIN products AS p ON oi.product_id = p.id
GROUP BY 1, 2
UNION ALL
SELECT NULL, u.id, NULL, NULL
FROM users AS u
WHERE NOT EXISTS (SELECT 1 FROM order_items AS oi WHERE oi.user_id = u.id)
ORDER BY status NULLS FIRST, user_id NULLS FIRST;
