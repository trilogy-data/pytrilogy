SELECT
    o.order_id,
    u.state,
    sum(oi.sale_price) AS revenue
FROM orders AS o
JOIN users AS u ON u.id = o.user_id
LEFT JOIN order_items AS oi ON oi.order_id = o.order_id
GROUP BY o.order_id, u.state;
