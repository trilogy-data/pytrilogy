SELECT DISTINCT
    u.state,
    p.brand
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
ORDER BY u.state, p.brand;
