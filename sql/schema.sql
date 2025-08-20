CREATE TABLE staging_orders (
    id SERIAL PRIMARY KEY,
    instrument VARCHAR(20) NOT NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('B', 'S')),
    timestamp BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    order_type INTEGER NOT NULL CHECK (order_type IN (0, 1, 2)),
    price DECIMAL(18, 2) NOT NULL,
    volume INTEGER NOT NULL,
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE active_orders (
    order_id BIGINT PRIMARY KEY,
    instrument VARCHAR(20) NOT NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('B', 'S')),
    initial_volume INTEGER NOT NULL,
    remaining_volume INTEGER NOT NULL,
    price DECIMAL(18, 2) NOT NULL,
    created_timestamp BIGINT NOT NULL,
    last_updated_timestamp BIGINT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_active_orders_instrument ON active_orders(instrument);
CREATE INDEX idx_active_orders_operation ON active_orders(operation);
CREATE INDEX idx_active_orders_price ON active_orders(price);
CREATE INDEX idx_active_orders_is_active ON active_orders(is_active);
CREATE INDEX idx_staging_processed ON staging_orders(processed);