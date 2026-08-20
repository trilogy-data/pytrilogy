SELECT
    oi.user_id,
    oi.product_id,
    u.state,
    p.brand,
    sum(oi.sale_price) AS revenue,
    count(oi.id) AS sale_line_count
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;
