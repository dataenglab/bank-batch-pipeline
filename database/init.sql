-- Create tables for processed data
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_dob DATE,
    cust_gender VARCHAR(10),
    cust_location VARCHAR(100),
    cust_account_balance DECIMAL(15,2),
    transaction_date DATE,
    transaction_time TIME,
    transaction_amount DECIMAL(15,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated tables for ML ready data
CREATE TABLE IF NOT EXISTS daily_transactions_agg (
    aggregation_date DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_amount DECIMAL(15,2),
    avg_transaction_amount DECIMAL(15,2),
    unique_customers INTEGER
);

CREATE TABLE IF NOT EXISTS customer_behavior_agg (
    customer_id VARCHAR(50),
    aggregation_date DATE,
    total_transactions INTEGER,
    total_spent DECIMAL(15,2),
    avg_transaction_amount DECIMAL(15,2),
    PRIMARY KEY (customer_id, aggregation_date)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_customer_behavior_date ON customer_behavior_agg(aggregation_date);