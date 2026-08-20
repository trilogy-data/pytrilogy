SELECT
    oi.id,
    oi.order_id,
    u.state,
    p.brand,
    oi.sale_price,
    oi.sale_price - p.cost AS item_margin
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
ORDER BY oi.id
LIMIT 25;
