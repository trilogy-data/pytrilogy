SELECT
    u.traffic_source,
    p.department,
    sum(oi.sale_price) AS revenue
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
GROUP BY u.traffic_source, p.department;
