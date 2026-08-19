SELECT
    u.age,
    p.category,
    avg(oi.sale_price) AS average_sale_price
FROM order_items AS oi
JOIN users AS u ON u.id = oi.user_id
JOIN products AS p ON p.id = oi.product_id
GROUP BY u.age, p.category
ORDER BY u.age, p.category;
