
-- Create merchant transaction count table
CREATE TABLE IF NOT EXISTS merchant_txn_count (
    merchant_id VARCHAR PRIMARY KEY,
    txn_count INT
);

-- Create customer transaction stats table
CREATE TABLE IF NOT EXISTS customer_txn_stats (
    customer_id VARCHAR,
    merchant_id VARCHAR,
    txn_count INT,
    total_amount DOUBLE PRECISION,
    PRIMARY KEY (customer_id, merchant_id)
);

-- Track detected patterns to avoid duplicate actions
CREATE TABLE IF NOT EXISTS detected_patterns (
    pattern_id VARCHAR,
    customer_id VARCHAR,
    merchant_id VARCHAR,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pattern_id, customer_id, merchant_id)
);
