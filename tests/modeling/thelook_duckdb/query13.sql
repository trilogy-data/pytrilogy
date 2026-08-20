SELECT
    u.state,
    sum(oi.sale_price) AS revenue
FROM order_items AS oi
JOIN products AS p ON p.id = oi.product_id
JOIN users AS u ON u.id = oi.user_id
WHERE p.brand = 'Brand 01'
GROUP BY u.state
ORDER BY u.state;
