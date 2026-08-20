SELECT
    p.id,
    p.brand,
    sum(oi.sale_price) AS lifetime_revenue
FROM products AS p
LEFT JOIN order_items AS oi ON oi.product_id = p.id
GROUP BY p.id, p.brand;
