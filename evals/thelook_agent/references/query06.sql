SELECT
    u.id,
    u.state,
    sum(oi.sale_price) AS lifetime_revenue
FROM users AS u
LEFT JOIN order_items AS oi ON oi.user_id = u.id
GROUP BY u.id, u.state;
