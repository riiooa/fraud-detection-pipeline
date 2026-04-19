-- ============================================
-- MEDALLION ARCHITECTURE - SILVER LAYER
-- Cleansed, enriched, validated data
-- ============================================
    
    
CREATE SCHEMA IF NOT EXISTS FRAUD_DB.SILVER;
    
USE SCHEMA FRAUD_DB.SILVER;
-- ============================================
-- SILVER TABLE: Enriched Transactions
-- ============================================
CREATE OR REPLACE TABLE FRAUD_DB.SILVER.silver_enriched_transactions (
    transaction_id STRING PRIMARY KEY,
    user_id STRING,
    name STRING,                    -- Will be masked
    credit_card STRING,             -- Will be masked
    amount NUMBER(15,2),
    location STRING,
    timestamp TIMESTAMP_NTZ,
    device_id STRING,
    ip_address STRING,              -- Will be masked
    merchant STRING,
    category STRING,
    
    -- Enriched fields
    is_fraud BOOLEAN,
    fraud_reason STRING,
    risk_score NUMBER(5,2),
    location_risk_level STRING,
    hour_of_day NUMBER(2),
    day_of_week NUMBER(1),
    
    -- Metadata
    bronze_ingestion_time TIMESTAMP_NTZ,
    silver_processed_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_quality_score NUMBER(5,2)
);
    
    
-- ============================================
-- STREAM for Bronze → Silver processing
-- ============================================
CREATE OR REPLACE STREAM FRAUD_DB.SILVER.silver_stream 
    ON TABLE FRAUD_DB.BRONZE.bronze_raw_transactions;
    
-- ============================================
-- TASK: Auto-process Bronze → Silver every minute
-- ============================================
CREATE OR REPLACE TASK FRAUD_DB.SILVER.process_bronze_to_silver
    WAREHOUSE = FRAUD_WH
    SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('FRAUD_DB.SILVER.silver_stream')
AS
    INSERT INTO FRAUD_DB.SILVER.silver_enriched_transactions
    SELECT 
        raw_payload:transaction_id::STRING,
        raw_payload:user_id::STRING,
        raw_payload:name::STRING,
        raw_payload:credit_card::STRING,
        raw_payload:amount::NUMBER(15,2),
        raw_payload:location::STRING,
        TO_TIMESTAMP(raw_payload:timestamp::STRING),
        raw_payload:device_id::STRING,
        raw_payload:ip_address::STRING,
        raw_payload:merchant::STRING,
        raw_payload:category::STRING,
        
        -- Fraud detection rules
        CASE 
            WHEN raw_payload:amount::NUMBER > 50000000 THEN TRUE
            WHEN raw_payload:is_fraud_simulated::BOOLEAN THEN TRUE
            ELSE FALSE
        END as is_fraud,
        
        -- Fraud reason
        CASE 
            WHEN raw_payload:amount::NUMBER > 50000000 THEN 'High amount (>50M)'
            WHEN raw_payload:is_fraud_simulated::BOOLEAN THEN 'Simulated fraud pattern'
            ELSE NULL
        END as fraud_reason,
        
        -- Risk score (0-100)
        CASE 
            WHEN raw_payload:amount::NUMBER > 50000000 THEN 95
            WHEN raw_payload:amount::NUMBER > 10000000 THEN 70
            WHEN raw_payload:amount::NUMBER > 5000000 THEN 40
            ELSE 10
        END as risk_score,
        
        -- Location risk
        CASE 
            WHEN raw_payload:location::STRING IN ('Jakarta', 'Surabaya') THEN 'HIGH'
            WHEN raw_payload:location::STRING IN ('Bandung', 'Medan') THEN 'MEDIUM'
            ELSE 'LOW'
        END as location_risk_level,
        
        EXTRACT(HOUR FROM TO_TIMESTAMP(raw_payload:timestamp::STRING)) as hour_of_day,
        EXTRACT(DOW FROM TO_TIMESTAMP(raw_payload:timestamp::STRING)) as day_of_week,
        
        ingestion_timestamp as bronze_ingestion_time,
        CURRENT_TIMESTAMP(),
        100 - (CASE WHEN data_quality_flags IS NOT NULL THEN 10 ELSE 0 END) as data_quality_score
        
    FROM FRAUD_DB.SILVER.silver_stream
    WHERE raw_payload:amount::NUMBER > 0;
    
-- Start the task
ALTER TASK FRAUD_DB.SILVER.process_bronze_to_silver RESUME;