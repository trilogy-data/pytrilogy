SELECT
    u.state,
    p.department,
    sum(oi.sale_price - p.cost) AS margin
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
GROUP BY u.state, p.department
ORDER BY margin DESC, u.state, p.department
LIMIT 10;
