# Pay-Flow Database Optimization

**Data Engineering Capstone Project**  
**Specialization**: Data Engineering  
**Focus**: Data Modeling • Version Control • Python Integration  

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)

## Table of Contents

- [Project Overview](#-project-overview)
- [Dataset Mapping (Olist → Pay-Flow)](#-dataset-mapping-olist--pay-flow))
- [Technologies & Tools](#-technologies--tools)
- [Step-by-Step What This Repository Does](#-step-by-step-what-this-repository-does)
  - [Phase 1: Project Setup & Version Control](#phase-1-project-setup--version-control)
  - [Phase 2: Professional Data Modeling (PostgreSQL)](#phase-2-professional-data-modeling-postgresql)
  - [Phase 3: Python-Driven ETL Automation](#phase-3-python-driven-etl-automation-python)
  - [Phase 4: Analytical Layer & Business Intelligence](#phase-4-analytical-layer--business-intelligence)
  - [Phase 5: Optimization & Fintech Readiness](#phase-5-optimization--fintech-readiness)
- [Repository Structure](#-repository-structure)
- [Quick Start (Local Setup)](#-quick-start-local-setup)
- [Learning Outcomes You Will Gain](#-learning-outcomes-you-will-gain)
- [Dataset Download](#-dataset-download)
- [Connect & Contribute](#-connect--contribute)

---

## 🚀 Project Overview

**Pay-Flow** is a simulated leading fintech platform specializing in cross-border payments, merchant services, and transaction analytics. This repository demonstrates how to build a **production-grade, version-controlled, and Python-automated PostgreSQL database** using the real-world **Brazilian E-Commerce Public Dataset by Olist**.

The project solves three critical enterprise challenges faced by fintech teams:

1. **Code Management** – No more scattered SQL files; everything lives in Git with proper versioning and collaboration.
2. **Analytics Gap** – Raw transaction data is transformed into rich analytical schemas and views (merchant revenue, geographic fraud patterns, payment trends, etc.).
3. **Python Integration** – Fully reproducible ETL pipelines using Python (psycopg2 + pandas) that can be scheduled, tested, and versioned.

**Result**: A clean, optimized, scalable database ready for fraud detection, BI dashboards, and regulatory reporting — exactly what fintech companies demand.

---

## 📊 Dataset Mapping (Olist → Pay-Flow)

| Raw CSV File                        | Target Table       | Key Transformations |
|-------------------------------------|--------------------|---------------------|
| `olist_customers_dataset.csv`       | `customers`        | Direct mapping |
| `olist_sellers_dataset.csv`         | `merchants`        | `seller_id` → `merchant_id` |
| `olist_orders_dataset.csv`          | `transactions`     | Core order data |
| `olist_order_items_dataset.csv`     | `transactions`     | Adds `seller_id`, `price`, `freight_value` |
| `olist_order_payments_dataset.csv`  | `transactions`     | Adds `payment_type`, `payment_value` |
| Other 4 files                       | Not used           | — |

---

## 🛠 Technologies & Tools

- **Database**: PostgreSQL 16 (with proper indexing & constraints)
- **Language**: Python 3.11 (pandas + psycopg2 for ETL automation)
- **Version Control**: Git + GitHub (branching strategy for SQL & Python)
- **Schema Design**: 3NF + Star-Schema analytical layer
- **Automation**: Python scripts that generate, migrate, and populate tables

---

## 📋 Step-by-Step What This Repository Does

### Phase 1: Project Setup & Version Control
1. Clone the repo
2. All SQL scripts are stored in `/sql/` with semantic versioning (`v1.0.0__create_customers.sql`)
3. Git branching strategy (`main`, `feature/`, `release/`) enforced

### Phase 2: Professional Data Modeling (PostgreSQL)
1. **DDL Scripts** (`/sql/ddl/`)
   - Create `customers`, `merchants`, and `transactions` tables
   - Proper primary keys, foreign keys, indexes, and constraints
2. **Data Dictionary** included as Markdown + SQL comments

### Phase 3: Python-Driven ETL Automation (`/python/`)
1. `etl_load_data.py` → Reads all CSVs and loads into PostgreSQL
2. `etl_merge_transactions.py` → Combines orders + items + payments into one enriched `transactions` table
3. `etl_create_analytical_views.py` → Builds materialized views and analytical layer
4. Fully reproducible — run `python run_etl.py` to recreate everything from scratch

### Phase 4: Analytical Layer & Business Intelligence
- **Views created**:
  - `vw_merchant_revenue` → Highest revenue merchants
  - `vw_geographic_fraud_patterns` → Fraud signals by state/city
  - `vw_payment_trends` → Payment method adoption over time
  - `vw_customer_lifetime_value` → CLV calculations
- Optimized for complex fintech queries (sub-second response on millions of rows)

### Phase 5: Optimization & Fintech Readiness
- Indexes on high-frequency columns (`order_id`, `merchant_id`, `customer_id`, `order_purchase_timestamp`)
- Partitioning-ready design
- Column-level comments for compliance audits

---

## 🏗 Repository Structure
```pay-flow-database-optimization/
├── sql/
│   ├── ddl/                 # Table creation scripts
│   ├── views/               # Analytical views
│   └── migrations/          # Versioned migration files
├── python/
│   ├── etl/                 # Data loading & merging scripts
│   ├── analytics/           # View creation scripts
│   └── utils/               # Database connection helpers
├── dataset/                    # Raw Olist CSVs (gitignore'd — download link below)
├── docs/
│   ├── data_dictionary.md
│   └── schema_diagram.md
├── .github/workflows/       # CI/CD for SQL validation (optional)
├── run_etl.py               # One-click full pipeline
├── requirements.txt
├── README.md                # ← You are here
└── LICENSE
```

## 🚀 Quick Start (Local Setup)

1. **Clone the repo**
   ```bash
   git clone https://github.com/yourusername/pay-flow-database-optimization.git
   cd pay-flow-database-optimization
   ```
2. **Create PostgreSQL database**
   ```SQL
   CREATE DATABASE payflow_db;
   ```
3. **Install Python dependencies**
   ```Bash
   pip install -r requirements.txt
   ```
4. **Configure connection** (edit
   ```python/utils/db_config.py```
   or use environment variables)

5. **Run the full pipelineBash
   ```python run_etl.py```
   
6. **Explore**
-   Connect with pgAdmin / DBeaver
-   Query the analytical views



## 📚 Learning Outcomes You Will Gain

-   Professional PostgreSQL data modeling for fintech
-   Git-based collaborative SQL development (exactly how senior data engineers work)
-   Python automation of ETL + schema management
-   Building analytical layers on top of transactional data
-   Real-world documentation & migration practices


## 📥 Dataset Download
Download the full Olist dataset here:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Place the CSVs in the ``data/`` folder.

## 🔗 Connect & Contribute
This repo is part of my Data Engineering Portfolio.
Feel free to fork, open issues, or use it as a template for your own fintech/database projects!

Made with ❤️ for real-world data engineering practice.

### Last Updated: March 2026
**Status:** Production-ready template
text
