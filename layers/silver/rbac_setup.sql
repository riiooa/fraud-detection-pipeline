CREATE SCHEMA IF NOT EXISTS FRAUD_DB.GOLD;

-- ============================================
-- RBAC Setup for PDPA Compliance
-- Roles: FRAUD_ANALYST, DATA_AUDITOR, ADMIN
-- ============================================

USE ROLE ACCOUNTADMIN;

-- ============================================
-- 1. Create Roles
-- ============================================
CREATE ROLE IF NOT EXISTS FRAUD_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_AUDITOR;
CREATE ROLE IF NOT EXISTS FRAUD_ADMIN;

-- ============================================
-- 2. Grant Warehouse Access
-- ============================================
GRANT USAGE ON WAREHOUSE FRAUD_WH TO ROLE FRAUD_ANALYST;
GRANT USAGE ON WAREHOUSE FRAUD_WH TO ROLE DATA_AUDITOR;
GRANT USAGE ON WAREHOUSE FRAUD_WH TO ROLE FRAUD_ADMIN;

-- ============================================
-- 3. Grant Schema Access
-- ============================================
GRANT USAGE ON DATABASE FRAUD_DB TO ROLE FRAUD_ANALYST;
GRANT USAGE ON DATABASE FRAUD_DB TO ROLE DATA_AUDITOR;
GRANT USAGE ON DATABASE FRAUD_DB TO ROLE FRAUD_ADMIN;

GRANT USAGE ON SCHEMA FRAUD_DB.BRONZE TO ROLE FRAUD_ANALYST;
GRANT USAGE ON SCHEMA FRAUD_DB.SILVER TO ROLE FRAUD_ANALYST;
GRANT USAGE ON SCHEMA FRAUD_DB.GOLD TO ROLE FRAUD_ANALYST;

GRANT USAGE ON SCHEMA FRAUD_DB.BRONZE TO ROLE DATA_AUDITOR;
GRANT USAGE ON SCHEMA FRAUD_DB.SILVER TO ROLE DATA_AUDITOR;
GRANT USAGE ON SCHEMA FRAUD_DB.GOLD TO ROLE DATA_AUDITOR;

-- ============================================
-- 4. Table Permissions
-- ============================================
-- FRAUD_ANALYST: SELECT only (with masking)
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DB.SILVER TO ROLE FRAUD_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DB.GOLD TO ROLE FRAUD_ANALYST;

-- DATA_AUDITOR: SELECT without masking (full access for audit)
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DB.SILVER TO ROLE DATA_AUDITOR;
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DB.GOLD TO ROLE DATA_AUDITOR;

-- FRAUD_ADMIN: Full access
GRANT ALL ON ALL TABLES IN SCHEMA FRAUD_DB.BRONZE TO ROLE FRAUD_ADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA FRAUD_DB.SILVER TO ROLE FRAUD_ADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA FRAUD_DB.GOLD TO ROLE FRAUD_ADMIN;

-- ============================================
-- 5. Create Test Users
-- ============================================
CREATE USER IF NOT EXISTS analyst_user 
    PASSWORD = 'Analyst@2024' 
    DEFAULT_ROLE = FRAUD_ANALYST;

CREATE USER IF NOT EXISTS auditor_user 
    PASSWORD = 'Auditor@2024' 
    DEFAULT_ROLE = DATA_AUDITOR;

-- ============================================
-- 6. Grant Roles
-- ============================================
GRANT ROLE FRAUD_ANALYST TO USER analyst_user;
GRANT ROLE DATA_AUDITOR TO USER auditor_user;

-- ============================================
-- 7. Verify Setup
-- ============================================
SHOW GRANTS TO ROLE FRAUD_ANALYST;
SHOW GRANTS TO ROLE DATA_AUDITOR
