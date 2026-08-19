SELECT
    user_id,
    product_id,
    sum(sale_price) AS revenue
FROM order_items
GROUP BY 1, 2
ORDER BY 1, 2;
