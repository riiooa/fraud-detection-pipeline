# NeoVault Pay — Real-Time Fraud Detection & Compliance Pipeline

A production-grade, near real-time fraud detection pipeline built on a **Medallion Architecture** (Bronze → Silver → Gold), designed to meet enterprise data governance and financial compliance standards (PDPA / UU PDP).

---

## Overview

This project implements an end-to-end data engineering pipeline that ingests raw credit card transaction events from **Apache Kafka**, processes and enriches them through multiple data layers in **Snowflake**, and surfaces aggregated business intelligence for fraud analysis and compliance reporting.

The system is fully automated using Snowflake Streams and Tasks, with centralized secret management via HashiCorp Vault and dynamic PII masking enforced at the database layer.

---

## Architecture

```
Confluent Cloud (Kafka)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│                    Snowflake                          │
│                                                       │
│  [Bronze Layer]  →  [Silver Layer]  →  [Gold Layer]  │
│  Raw Storage        Enrichment &        Business      │
│  (VARIANT JSON)     PII Masking         Aggregation   │
└───────────────────────────────────────────────────────┘
        │
        ▼
Compliance Dashboard / Data Scientist Access
```

| Layer | Purpose |
|---|---|
| **Bronze** | Stores raw JSON payloads as-is; automated data profiling |
| **Silver** | Data cleansing, type casting, risk scoring, dynamic PII masking |
| **Gold** | Incremental business aggregations for fraud trends and compliance reporting |

---

## Tech Stack

| Category | Technology |
|---|---|
| Streaming | Confluent Cloud (Apache Kafka) |
| Data Warehouse | Snowflake (Medallion Architecture) |
| Secret Management | HashiCorp Vault |
| Data Simulation | Python, Faker |
| Quality & Testing | Pytest, Great Expectations |
| Analytics & EDA | Jupyter Notebook, Matplotlib, Seaborn |
| Compliance Standard | PDPA (Singapore) / UU PDP (Indonesia) |

---

## Project Structure

```
fraud-detection-pipeline/
├── consumer/
│   └── consumer.py                  # Kafka consumer — streams data into Bronze layer
├── producer/
│   └── producer.py                  # Simulates real-time credit card transactions
├── layers/
│   ├── bronze/
│   │   └── schema.sql               # DDL for raw tables and data profiling
│   ├── silver/
│   │   ├── schema.sql               # DDL for enriched tables, Stream, and Task
│   │   ├── masking_policies.sql     # Dynamic Data Masking policies (PII)
│   │   └── rbac_setup.sql           # Role and user configuration
│   └── gold/
│       └── schema.sql               # Aggregation tables and analytics views
├── security/
│   └── vault_client.py              # HashiCorp Vault integration module
├── notebooks/
│   └── gold_eda.ipynb            # Exploratory Data Analysis on Gold Layer
├── scripts/
│   └── load_test.py                 # Load testing script (10,000 events)
├── results/
│   ├── load_test_report.json        # Official system performance report
│   └── eda_plots/                   # Fraud trend visualization gallery
├── docs/
│   ├── phase1.md                    # Phase 1 documentation
│   └── phase2.md                    # Phase 2 documentation
├── DEPLOYMENT.md                    # Step-by-step deployment guide
├── TROUBLESHOOTING.md               # Common issues and solutions
├── requirements.txt
├── .env.example
└── README.md
```

---

## Key Features

- **Real-Time Ingestion** — Kafka consumer streams transaction events into Snowflake at sub-second intervals.
- **Automated CDC Pipeline** — Snowflake Streams and Tasks propagate changes across layers without manual intervention.
- **Dynamic Data Masking** — PII fields (`name`, `credit_card`, `ip_address`) are masked at the database layer based on role, ensuring PDPA compliance.
- **Anomaly Detection** — Z-Score (3 Sigma) logic flags outlier transactions in real-time and logs them to `silver_outlier_log`.
- **Risk Scoring** — Each transaction is assigned a Risk Score (0–100) and a Location Risk Level at the Silver Layer.
- **RBAC Enforcement** — Strict role-based access control separates duties between `FRAUD_ADMIN`, `DATA_AUDITOR`, and `FRAUD_ANALYST`.
- **Centralized Secret Management** — All credentials are stored and retrieved via HashiCorp Vault; no hardcoded secrets in source code.

---

## Data Governance & RBAC

| Role | Access Rights | Data Visibility |
|---|---|---|
| `FRAUD_ADMIN` | ALL (Full Control) | Unmasked |
| `DATA_AUDITOR` | SELECT (Audit Only) | Unmasked |
| `FRAUD_ANALYST` | SELECT (Analysis) | Masked (PII redacted) |

---

## Performance Benchmarks

Results from final load test simulating peak traffic (10,000 events):

| Metric | Result |
|---|---|
| Total Events Processed | 10,000 |
| Throughput | 71.02 events/sec |
| Average Latency (end-to-end) | 346 ms |
| Data Integrity (Bronze Layer) | 100% |
| Success Rate | 100% |

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/USERNAME/fraud-detection-pipeline.git
cd fraud-detection-pipeline
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your Snowflake, Kafka, and Vault credentials
```

### 3. Start infrastructure

```bash
docker-compose up -d
```

### 4. Register secrets in Vault

```bash
vault kv put secret/snowflake account="YOUR_ACC" user="YOUR_USER" password="YOUR_PASS"
```

### 5. Install dependencies and run the pipeline

```bash
pip install -r requirements.txt
python consumer/consumer.py
```

For full deployment instructions, refer to [DEPLOYMENT.md](./DEPLOYMENT.md).  
For common issues, refer to [TROUBLESHOOTING.md](./TROUBLESHOOTING.md).

---

## Documentation

| Document | Description |
|---|---|
| [Phase 1 Documentation](./docs/phase1.md) | Medallion architecture implementation, Bronze to Gold pipeline |
| [Phase 2 Documentation](./docs/phase2.md) | Security hardening, EDA, anomaly detection, compliance audit |
| [Deployment Guide](./DEPLOYMENT.md) | Step-by-step setup and deployment instructions |
| [Troubleshooting Guide](./TROUBLESHOOTING.md) | Common errors and their solutions |

---

## Compliance

This pipeline is designed and tested in accordance with:

- **PDPA** — Personal Data Protection Act (Singapore)
- **UU PDP** — Undang-Undang Pelindungan Data Pribadi (Indonesia)

All PII fields are protected through Dynamic Data Masking at the database layer, with access governed by role-based policies and a full query audit trail via `snowflake.account_usage`.

---

## Author

**Rio Al Fandi**  
Data Engineer 
*April 2026*