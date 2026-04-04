-- The "source of truth" this demo project reads from. In a real deployment
-- this is your ingested raw data; environments never rewrite root tables.
CREATE TABLE IF NOT EXISTS raw_orders AS
SELECT * FROM (VALUES
    (1, 'widget', 10, TIMESTAMP '2024-01-01 00:00:00'),
    (2, 'widget', 20, TIMESTAMP '2024-01-02 00:00:00'),
    (3, 'gadget', 30, TIMESTAMP '2024-01-03 00:00:00'),
    (4, 'gadget', 40, TIMESTAMP '2024-01-04 00:00:00')
) t(order_id, product, amount, updated_at);
