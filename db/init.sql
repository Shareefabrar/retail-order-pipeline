CREATE TABLE IF NOT EXISTS live_metrics (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    total_revenue NUMERIC,
    order_count   INT,
    top_sku       TEXT,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS daily_summary (
    summary_date  DATE PRIMARY KEY,
    total_orders  INT,
    total_revenue NUMERIC,
    avg_order_value NUMERIC
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    detected_date DATE,
    metric        TEXT,
    value         NUMERIC,
    expected_low  NUMERIC,
    expected_high NUMERIC,
    flagged_at    TIMESTAMP DEFAULT now()
);
