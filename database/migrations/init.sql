CREATE TABLE IF NOT EXISTS stock_events (
    id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(64) NOT NULL,
    qty INTEGER NOT NULL,
    warehouse VARCHAR(64) NOT NULL,
    event_type VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_wh_sku ON stock_events (warehouse, sku);

CREATE TABLE IF NOT EXISTS stock_agg (
    warehouse VARCHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    qty INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (warehouse, sku)
);
