-- Fact-anchored output keeps user-side extension rows: never-ordered users
-- surface per-state as (NULL order_id, state, NULL revenue).
SELECT
    o.order_id,
    u.state,
    sum(oi.sale_price) AS revenue
FROM users AS u
LEFT JOIN orders AS o ON o.user_id = u.id
LEFT JOIN order_items AS oi ON oi.order_id = o.order_id
GROUP BY o.order_id, u.state
ORDER BY o.order_id, u.state;
