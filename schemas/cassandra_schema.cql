-- ===============================================================================
-- AstraDB Schema for Fintech Onboarding Analytics ETL Pipeline
-- ===============================================================================
-- 
-- INSTRUCTIONS FOR ASTRADB CONSOLE:
-- 1. Go to https://astra.datastax.com/
-- 2. Open your 'big_data' database
-- 3. Click on "CQL Console"
-- 4. Copy and paste this ENTIRE file into the console
-- 5. Click "Execute" to create all tables
-- 6. Run: source venv/bin/activate && python run_astradb_pipeline.py
-- 
-- NOTE: Keyspace creation is commented out since it already exists in AstraDB
-- ===============================================================================

-- KEYSPACE (already exists in AstraDB)
-- CREATE KEYSPACE IF NOT EXISTS fintech_analytics
-- WITH REPLICATION = {
--     'class': 'NetworkTopologyStrategy',
--     'datacenter1': 1
-- };

USE fintech_analytics;

-- User Segments Table
-- Stores user classification and transaction summary
CREATE TABLE IF NOT EXISTS user_segments (
    user_id TEXT PRIMARY KEY,
    user_segment TEXT,
    total_transactions INT,
    unique_transaction_types INT,
    transaction_days INT,
    first_transaction_dt TIMESTAMP,
    last_transaction_dt TIMESTAMP
) WITH COMMENT = 'User segmentation based on transaction behavior';

-- User Metrics Table  
-- Comprehensive table with all calculated metrics per user
CREATE TABLE IF NOT EXISTS user_metrics (
    user_id TEXT PRIMARY KEY,
    first_login_dt TIMESTAMP,
    week_year TEXT,
    user_segment TEXT,
    total_transactions INT,
    transaction_days INT,
    
    -- Drop metric
    drop_flag INT,
    return_flag INT,
    return_dt TIMESTAMP,
    
    -- Activation metric
    activation_validated INT,
    activacion_dt TIMESTAMP,
    
    -- Habit metric
    habit_validated INT,
    total_transactions_period INT,
    unique_transaction_days INT,
    
    -- Setup metric
    setup_achieved INT,
    setup_dt TIMESTAMP,
    
    -- Derived fields
    onboarding_end_dt TIMESTAMP,
    first_login_month INT,
    first_login_week INT
) WITH COMMENT = 'Comprehensive user metrics for onboarding analysis';

-- Funnel Analysis Table
-- Aggregated data for funnel analysis by different dimensions
CREATE TABLE IF NOT EXISTS funnel_analysis (
    dimension_type TEXT,
    dimension_value TEXT,
    total_users BIGINT,
    stage_1_registered BIGINT,
    stage_2_returned BIGINT,
    stage_3_activated BIGINT,
    stage_4_setup BIGINT,
    stage_5_habit BIGINT,
    PRIMARY KEY (dimension_type, dimension_value)
) WITH COMMENT = 'Funnel analysis by different dimensions (segment, month, week)';

-- Monthly Metrics Table
-- Time-series analysis by month
CREATE TABLE IF NOT EXISTS monthly_metrics (
    year_month TEXT,
    user_segment TEXT,
    total_users BIGINT,
    returned_users BIGINT,
    activated_users BIGINT,
    setup_completed_users BIGINT,
    habit_achieved_users BIGINT,
    drop_rate DOUBLE,
    activation_rate DOUBLE,
    setup_rate DOUBLE,
    habit_rate DOUBLE,
    PRIMARY KEY (year_month, user_segment)
) WITH COMMENT = 'Monthly aggregated metrics by user segment';

-- Weekly Metrics Table  
-- Time-series analysis by week
CREATE TABLE IF NOT EXISTS weekly_metrics (
    year_week TEXT,
    user_segment TEXT,
    total_users BIGINT,
    returned_users BIGINT,
    activated_users BIGINT,
    setup_completed_users BIGINT,
    habit_achieved_users BIGINT,
    drop_rate DOUBLE,
    activation_rate DOUBLE,
    setup_rate DOUBLE,
    habit_rate DOUBLE,
    PRIMARY KEY (year_week, user_segment)
) WITH COMMENT = 'Weekly aggregated metrics by user segment';

-- Transaction Summary Table
-- Summary of transaction patterns for analysis
CREATE TABLE IF NOT EXISTS transaction_summary (
    user_id TEXT,
    transaction_date DATE,
    transaction_count INT,
    payment_transactions INT,
    collection_transactions INT,
    unique_transaction_types INT,
    PRIMARY KEY (user_id, transaction_date)
) WITH COMMENT = 'Daily transaction summary per user'
AND CLUSTERING ORDER BY (transaction_date DESC);

-- Secondary indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_user_metrics_segment ON user_metrics (user_segment);
CREATE INDEX IF NOT EXISTS idx_user_metrics_month ON user_metrics (first_login_month);
CREATE INDEX IF NOT EXISTS idx_user_metrics_week ON user_metrics (first_login_week);
CREATE INDEX IF NOT EXISTS idx_user_metrics_drop ON user_metrics (drop_flag);
CREATE INDEX IF NOT EXISTS idx_user_metrics_activation ON user_metrics (activation_validated);
CREATE INDEX IF NOT EXISTS idx_user_metrics_habit ON user_metrics (habit_validated);
CREATE INDEX IF NOT EXISTS idx_user_metrics_setup ON user_metrics (setup_achieved);

-- Views for common analytical queries
-- Note: Cassandra doesn't support views in the same way as traditional RDBMS
-- These are documented query patterns that should be implemented in the application layer
