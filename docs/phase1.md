# NeoVault Pay — Real-Time Fraud Detection & Compliance Pipeline

**Project Name:** NeoVault Pay Real-time Fraud Detection & Compliance Pipeline  
**Engineer:** Rio (AI Solutions Architect / Data Engineer)  
**Tech Stack:** Confluent Cloud (Kafka), Python, Snowflake (Medallion Architecture)  
**Architecture:** Medallion Architecture (Bronze, Silver, Gold)  
**Compliance Standard:** PDPA (Singapore) / UU PDP (Indonesia)  
**Date:** April 2026  
**Status:** ✅ Phase 1 Complete

---

## 1. Executive Summary

Phase 1 successfully implemented a near real-time data pipeline using the **Medallion Architecture** on Snowflake. The pipeline streams raw transaction data from Kafka, performs data enrichment, enforces data privacy policies (Dynamic Data Masking), and produces business aggregations ready for use in compliance dashboards.

---

## 2. System Architecture

The system uses an automated **Change Data Capture (CDC)** approach with Snowflake Streams and Tasks to ensure cost efficiency and low latency.

### 2.1 Ingestion Layer (Kafka)

- **Producer:** Uses Python Faker to simulate credit card transactions in real-time (0.5 seconds/event).
- **Cluster:** Confluent Cloud Basic Cluster with topic `fraud-transactions` (3 partitions, 24-hour retention).
- **Consumer:** Python-based consumer that streams ingestion directly into the Bronze Snowflake sink table.

### 2.2 Bronze Layer (Raw Storage)

- **Purpose:** Stores data as-is (raw) in JSON format (`VARIANT`).
- **Data Quality:** Automated Profiling Report to monitor null percentages, amount distribution, and initial outlier identification.
- **Audit:** Profiling results stored in the `bronze_data_profile` table.

### 2.3 Silver Layer (Cleansing & Privacy)

- **Process:** Automated using `STREAM` on the Bronze table and a scheduled `TASK` (every 1 minute).
- **Enrichment:**
  - Data type transformation (Timestamp, Number).
  - Risk Score determination (0–100) and Location Risk Level assignment.
  - Fraud Detection Logic based on transaction amount (>50M) and simulation patterns.
- **Data Governance (PDPA) — Dynamic Masking:**

  | Field | Treatment |
  |---|---|
  | `name` | Displays initials only — example: `R****a` |
  | `credit_card` | Displays last 4 digits only |
  | `ip_address` | Masks the last octet to protect specific location privacy |

### 2.4 Gold Layer (Business Aggregation)

- **Process:** Incremental Load using `MERGE` statements to optimize compute costs.
- **Key Metrics:**

  | Table / View | Description |
  |---|---|
  | `gold_fraud_daily_summary` | Daily aggregation for fraud rate and potential losses |
  | `gold_hourly_fraud_trends` | Hourly trend analysis for anomaly spike detection |
  | `gold_location_risk` | Geography-based risk mapping over the last 7 days |
  | `compliance_eda_view` | View for Data Scientist investigations without manual joins |

---

## 3. Data Governance & RBAC

The pipeline enforces strict **Role-Based Access Control (RBAC)** to ensure Segregation of Duties:

| Role | Access Rights | Masking Policy |
|---|---|---|
| `FRAUD_ADMIN` | ALL (Full Control) | Unmasked — full data visible |
| `DATA_AUDITOR` | SELECT (Audit Only) | Unmasked — full data visible |
| `FRAUD_ANALYST` | SELECT (Analysis) | Masked — sensitive data redacted |

---

## 4. Technical Artifacts

### SQL Scripts

```
layers/
├── bronze/
│   └── schema.sql              # DDL for raw tables and profiling
├── silver/
│   ├── schema.sql              # DDL for enriched tables, Stream, and Task
│   ├── masking_policies.sql    # PII security logic
│   └── rbac_setup.sql          # Role and user configuration
└── gold/
    └── schema.sql              # Aggregation tables and analytics views
```

### Python Scripts

```
├── producer.py    # Transaction data simulation
└── consumer.py   # Ingestion into Snowflake
```

---

## 5. Key Achievements (Phase 1)

- [x] Successfully built an end-to-end pipeline from Kafka to Snowflake Gold Layer.
- [x] Implemented PDPA-compliant data privacy with Dynamic Data Masking.
- [x] Full automation using Snowflake Tasks (zero manual intervention).
- [x] Database structure organized according to Enterprise Medallion Architecture standards.

---

*This document covers Phase 1. The next phase will include ML model integration for predictive fraud detection.*