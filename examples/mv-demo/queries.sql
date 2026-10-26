-- Raw orders (each request is scoped to one namespace / tenant).
SELECT order_id, product, quantity, amount_cents, ts
FROM orders ORDER BY ts, order_id;

-- Final sales totals. The MV stores partial aggregates per source ADD.
SELECT product, SUM(orders) AS orders, SUM(units) AS units,
       SUM(revenue_cents) AS revenue_cents
FROM sales GROUP BY product ORDER BY product;

-- Revenue in currency units.
SELECT product, SUM(revenue_cents) / 100.0 AS revenue
FROM sales GROUP BY product ORDER BY revenue DESC;

-- See the stored partial groups and the write-time now() column.
SELECT product, orders, units, revenue_cents, inserted_at
FROM sales ORDER BY product, ts;

-- Discover queryable tables.
SELECT table_name FROM information_schema.tables;
