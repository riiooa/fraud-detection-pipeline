-- ============================================
-- MEDALLION ARCHITECTURE - BRONZE LAYER
-- Raw data ingestion from Kafka
-- ============================================

CREATE DATABASE IF NOT EXISTS FRAUD_DB;
CREATE SCHEMA IF NOT EXISTS FRAUD_DB.BRONZE;
USE SCHEMA FRAUD_DB.BRONZE;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS FRAUD_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ============================================
-- BRONZE TABLE: Raw Transactions
-- Stores complete raw payload from Kafka
-- ============================================
CREATE OR REPLACE TABLE bronze_raw_transactions (
    raw_payload VARIANT NOT NULL,           -- Complete JSON from Kafka
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_partition INTEGER,
    source_offset INTEGER,
    data_quality_flags VARIANT,              -- Track quality issues
    processing_stage VARCHAR(20) DEFAULT 'BRONZE'
)
COMMENT = 'Bronze layer: Raw transaction data from Kafka';

-- ============================================
-- BRONZE STREAM for Silver processing
-- ============================================
CREATE OR REPLACE STREAM bronze_stream 
    ON TABLE bronze_raw_transactions
    COMMENT = 'Stream for Bronze → Silver processing';

-- ============================================
-- BRONZE DATA PROFILE TABLE
-- Stores profiling results
-- ============================================
CREATE OR REPLACE TABLE bronze_data_profile (
    profile_date DATE DEFAULT CURRENT_DATE(),
    total_records NUMBER,
    null_percentage VARIANT,
    unique_values VARIANT,
    amount_stats VARIANT,
    top_locations VARIANT,
    quality_score NUMBER(5,2),
    profile_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);