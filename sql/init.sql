-- ─────────────────────────────────────────────────────────────────────────
-- init.sql
-- Runs automatically when the PostgreSQL container starts for the first time.
-- Creates the customers table and the CDC trigger.
-- NO pre-inserted rows – only real DML you run manually will flow through
-- the pipeline, preventing phantom data from landing in ADLS.
-- ─────────────────────────────────────────────────────────────────────────

-- Create the customers table (empty skeleton – no seed data)
CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    email         VARCHAR(150),
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- Create the CDC audit log table
-- Every INSERT / UPDATE / DELETE on customers writes one row here.
CREATE TABLE IF NOT EXISTS cdc_events (
    event_id      SERIAL PRIMARY KEY,
    operation     VARCHAR(10),       -- INSERT | UPDATE | DELETE
    customer_id   INT,
    customer_name VARCHAR(100),
    email         VARCHAR(150),
    updated_at    TIMESTAMP,
    captured_at   TIMESTAMP DEFAULT NOW()
);

-- Trigger function: fires after any DML on customers
CREATE OR REPLACE FUNCTION capture_cdc_event()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO cdc_events (operation, customer_id, customer_name, email, updated_at)
        VALUES ('DELETE', OLD.customer_id, OLD.customer_name, OLD.email, OLD.updated_at);
        RETURN OLD;
    ELSE
        INSERT INTO cdc_events (operation, customer_id, customer_name, email, updated_at)
        VALUES (TG_OP, NEW.customer_id, NEW.customer_name, NEW.email, NEW.updated_at);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the customers table
DROP TRIGGER IF EXISTS cdc_trigger ON customers;
CREATE TRIGGER cdc_trigger
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION capture_cdc_event();
