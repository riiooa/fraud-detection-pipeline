# NeoVault Pay — Real-Time Fraud Detection & Compliance Pipeline

**Project Name:** NeoVault Pay Real-time Fraud Detection & Compliance Pipeline  
**Engineer:** Rioal (Data Engineer & AI Solutions Architect)  
**Tech Stack:** HashiCorp Vault, Pytest, Great Expectations, Jupyter Notebook, Snowflake  
**Architecture:** Medallion Architecture (Bronze, Silver, Gold)  
**Compliance Standard:** PDPA (Singapore) / UU PDP (Indonesia)  
**Date:** April 2026  
**Status:** ✅ Phase 2 Complete

---

## 1. Executive Summary

Phase 2 strengthened the fraud detection pipeline with enterprise-grade security, in-depth exploratory data analysis, and advanced compliance validation. Key additions include centralized secret management via HashiCorp Vault, real-time anomaly detection using Z-Score logic, and full PDPA audit verification — achieving a system latency of under 500ms at peak load.

---

## 2. Tech Stack Expansion

In Phase 2, the project ecosystem was enhanced with the following integrations:

| Category | Tools |
|---|---|
| Security | HashiCorp Vault (Secret Management) |
| Quality & Testing | Pytest, Great Expectations (Data Validation) |
| Analytics | Jupyter Notebook, Matplotlib / Seaborn (EDA) |
| Compliance | Snowflake RBAC & Data Masking (PDPA Standards) |

---

## 3. Key Activities & Implementations

### 3.1 Robust Security with HashiCorp Vault

Replaced hardcoded credentials with centralized Secret Management using Vault to meet banking industry security standards.

- **Implementation:** Vault Server deployment via Docker.
- **Scope:** Encryption of Snowflake credentials and Kafka API Keys.
- **Result:** Improved security posture; the application retrieves secrets only at runtime via the Vault API.

### 3.2 Exploratory Data Analysis (EDA) & Gold Layer Analytics

Conducted in-depth analysis on the Gold Layer to generate actionable business insights.

- **Highlights:**
  - Time-series analysis of fraud transaction trends.
  - Identification of geographic locations with the highest fraud risk.
  - Visualization of transaction amount distributions and feature correlation matrices.
- **Output:** Ready-to-use analytics dashboard for the Compliance team.

### 3.3 Data Profiling & Anomaly Detection

Implemented real-time anomaly detection logic on the Silver Layer.

- **Mechanism:** Z-Score (3 Sigma) calculation to detect outlier transactions on the `amount` column.
- **Alerting:** System automatically logs anomalous transactions into `silver_outlier_log` for further investigation.

### 3.4 PDPA Compliance & Audit Logging

Ensured sensitive data (PII — Personally Identifiable Information) is protected in accordance with PDPA (Singapore) regulations.

- **Audit Trail:** Query history tracked through `snowflake.account_usage`.
- **RBAC Testing:** Verified access differences between the `FRAUD_ANALYST` role (masked data) and `DATA_AUDITOR` role (full access).
- **Result:** 100% compliance across all sensitive data masking tests.

### 3.5 Latency Optimization & SLA

Optimized the data pipeline to achieve near real-time performance.

- **Metrics:** End-to-end latency measurement (Kafka → Gold).
- **Optimization:** Batch size tuning and Snappy compression implementation.
- **Achievement:** Latency reduced to **< 500ms** (average 346ms under peak load).

### 3.6 Reliability & Unit Testing

Built a fault-tolerant system resilient to infrastructure failures.

- **Unit Tests:** Test coverage >80% using `pytest`.
- **Fault Tolerance:** Retry logic for Snowflake timeouts and dead letter queue handling for corrupted Kafka messages.

---

## 4. Final Performance Benchmark (Load Test)

Final testing was conducted by simulating high traffic to validate system stability.

| Metric | Result |
|---|---|
| Total Events Processed | 10,000 |
| Throughput | 71.02 events/sec |
| Average Latency | 346 ms |
| Data Integrity (Bronze) | 100% (10,000 / 10,000) |
| Success Rate | 100% (Robust batching) |

---

## 5. Deliverables

```
├── security/
│   └── vault_client.py              # Vault integration module
├── notebooks/
│   └── 03_gold_eda.ipynb            # Research documentation and data visualizations
└── results/
    ├── load_test_report.json        # Official system performance report
    ├── eda_plots/                   # Fraud trend visualization gallery
    ├── DEPLOYMENT.md                # System deployment guide
    └── TROUBLESHOOTING.md           # Operational troubleshooting guide
```

---

## 6. Key Learnings

- **Data Understanding:** Understanding fraud patterns is not just about numbers — it is about behavioral analysis.
- **Operational Excellence:** Audit logs and secret management are critical in building credible financial applications.
- **Medallion Strategy:** The layered data approach proved highly effective for simplifying debugging and compliance reporting.

---

*This document covers Phase 2. The next phase will include ML model integration for predictive fraud detection.*