-- ─────────────────────────────────────────────────────────────
-- manual_dml.sql
-- Run these statements inside PostgreSQL (psql or any client)
-- to trigger CDC events that flow through the pipeline.
-- ─────────────────────────────────────────────────────────────

-- 1. INSERT a new customer
INSERT INTO customers (customer_name, email, updated_at)
VALUES ('Praveen Kumar', 'praveen@example.com', NOW());

-- 2. INSERT another customer
INSERT INTO customers (customer_name, email, updated_at)
VALUES ('Arjun Sharma', 'arjun@example.com', NOW());

-- 3. UPDATE an existing customer's email
UPDATE customers
SET    email      = 'praveen.new@example.com',
       updated_at = NOW()
WHERE  customer_name = 'Praveen Kumar';

-- 4. DELETE a customer
DELETE FROM customers
WHERE  customer_name = 'Arjun Sharma';

-- 5. Check what was captured by the CDC trigger
SELECT * FROM cdc_events ORDER BY event_id;
