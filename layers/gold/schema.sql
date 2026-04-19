-- ============================================
-- MEDALLION ARCHITECTURE - GOLD LAYER
-- Aggregated metrics for Compliance & Dashboard
-- ============================================

USE SCHEMA FRAUD_DB.GOLD;

-- ============================================
-- GOLD TABLE: Daily Fraud Summary
-- ============================================
CREATE OR REPLACE TABLE gold_fraud_daily_summary (
    summary_date DATE PRIMARY KEY,
    total_transactions NUMBER,
    fraud_transactions NUMBER,
    fraud_rate NUMBER(5,2),
    total_amount NUMBER(15,2),
    fraud_amount NUMBER(15,2),
    avg_risk_score NUMBER(5,2),
    top_fraud_location STRING,
    top_fraud_hour NUMBER(2),
    unique_users_affected NUMBER,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- GOLD TABLE: Hourly Fraud Trends
-- ============================================
CREATE OR REPLACE TABLE gold_hourly_fraud_trends (
    hour_start TIMESTAMP_NTZ,
    hour_end TIMESTAMP_NTZ,
    total_transactions NUMBER,
    fraud_transactions NUMBER,
    fraud_rate NUMBER(5,2),
    total_amount NUMBER(15,2),
    PRIMARY KEY (hour_start)
);

-- ============================================
-- GOLD TABLE: Location Risk Analysis
-- ============================================
CREATE OR REPLACE TABLE gold_location_risk (
    location STRING PRIMARY KEY,
    total_transactions NUMBER,
    fraud_transactions NUMBER,
    fraud_rate NUMBER(5,2),
    total_amount NUMBER(15,2),
    risk_level STRING,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STREAM for Silver → Gold processing
-- ============================================
CREATE OR REPLACE STREAM gold_stream 
    ON TABLE FRAUD_DB.SILVER.silver_enriched_transactions;

-- ============================================
-- TASK: Update Daily Summary every 5 minutes
-- ============================================
CREATE OR REPLACE TASK update_gold_daily_summary
    WAREHOUSE = FRAUD_WH
    SCHEDULE = '5 MINUTE'
AS
    MERGE INTO gold_fraud_daily_summary g
    USING (
        SELECT 
            DATE(timestamp) as summary_date,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
            SUM(amount) as total_amount,
            SUM(CASE WHEN is_fraud THEN amount ELSE 0 END) as fraud_amount,
            AVG(risk_score) as avg_risk_score,
            MODE(location) as top_fraud_location,
            MODE(hour_of_day) as top_fraud_hour,
            COUNT(DISTINCT user_id) as unique_users_affected
        FROM FRAUD_DB.SILVER.silver_enriched_transactions
        WHERE DATE(timestamp) = CURRENT_DATE()
        GROUP BY DATE(timestamp)
    ) s ON g.summary_date = s.summary_date
    WHEN MATCHED THEN UPDATE SET
        total_transactions = s.total_transactions,
        fraud_transactions = s.fraud_transactions,
        fraud_rate = s.fraud_rate,
        total_amount = s.total_amount,
        fraud_amount = s.fraud_amount,
        avg_risk_score = s.avg_risk_score,
        top_fraud_location = s.top_fraud_location,
        top_fraud_hour = s.top_fraud_hour,
        unique_users_affected = s.unique_users_affected,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (summary_date, total_transactions, fraud_transactions, fraud_rate,
         total_amount, fraud_amount, avg_risk_score, top_fraud_location,
         top_fraud_hour, unique_users_affected)
    VALUES (
        s.summary_date, s.total_transactions, s.fraud_transactions, s.fraud_rate,
        s.total_amount, s.fraud_amount, s.avg_risk_score, s.top_fraud_location,
        s.top_fraud_hour, s.unique_users_affected
    );

-- ============================================
-- TASK: Update Hourly Trends
-- ============================================
CREATE OR REPLACE TASK update_gold_hourly_trends
    WAREHOUSE = FRAUD_WH
    SCHEDULE = '5 MINUTE'
AS
    MERGE INTO gold_hourly_fraud_trends g
    USING (
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour_start,
            DATE_TRUNC('hour', timestamp) + INTERVAL '1 hour' as hour_end,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
            SUM(amount) as total_amount
        FROM FRAUD_DB.SILVER.silver_enriched_transactions
        WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', timestamp)
    ) s ON g.hour_start = s.hour_start
    WHEN MATCHED THEN UPDATE SET
        total_transactions = s.total_transactions,
        fraud_transactions = s.fraud_transactions,
        fraud_rate = s.fraud_rate,
        total_amount = s.total_amount
    WHEN NOT MATCHED THEN INSERT
        (hour_start, hour_end, total_transactions, fraud_transactions, fraud_rate, total_amount)
    VALUES (
        s.hour_start, s.hour_end, s.total_transactions, s.fraud_transactions, s.fraud_rate, s.total_amount
    );

-- ============================================
-- TASK: Update Location Risk
-- ============================================
CREATE OR REPLACE TASK update_gold_location_risk
    WAREHOUSE = FRAUD_WH
    SCHEDULE = '5 MINUTE'
AS
    MERGE INTO gold_location_risk g
    USING (
        SELECT 
            location,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
            SUM(amount) as total_amount,
            CASE 
                WHEN SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 10 THEN 'CRITICAL'
                WHEN SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 5 THEN 'HIGH'
                WHEN SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 2 THEN 'MEDIUM'
                ELSE 'LOW'
            END as risk_level
        FROM FRAUD_DB.SILVER.silver_enriched_transactions
        WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL '7 days'
        GROUP BY location
    ) s ON g.location = s.location
    WHEN MATCHED THEN UPDATE SET
        total_transactions = s.total_transactions,
        fraud_transactions = s.fraud_transactions,
        fraud_rate = s.fraud_rate,
        total_amount = s.total_amount,
        risk_level = s.risk_level,
        last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (location, total_transactions, fraud_transactions, fraud_rate, total_amount, risk_level)
    VALUES (
        s.location, s.total_transactions, s.fraud_transactions, s.fraud_rate, s.total_amount, s.risk_level
    );

-- ============================================
-- COMPLIANCE VIEW for EDA
-- ============================================
CREATE OR REPLACE VIEW compliance_eda_view AS
SELECT 
    s.transaction_id,
    s.amount,
    s.location,
    s.timestamp,
    s.is_fraud,
    s.risk_score,
    s.location_risk_level,
    s.hour_of_day,
    s.day_of_week,
    d.fraud_rate as daily_fraud_rate,
    l.risk_level as location_risk_trend
FROM FRAUD_DB.SILVER.silver_enriched_transactions s
LEFT JOIN gold_fraud_daily_summary d ON DATE(s.timestamp) = d.summary_date
LEFT JOIN gold_location_risk l ON s.location = l.location;

-- Start all tasks
ALTER TASK update_gold_daily_summary RESUME;
ALTER TASK update_gold_hourly_trends RESUME;
ALTER TASK update_gold_location_risk RESUME;