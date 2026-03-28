# E-Commerce Sales Pipeline

## Overview
An end-to-end batch data pipeline built using PySpark and DuckDB, processing 
the Olist Brazilian E-Commerce dataset through a medallion architecture 
(bronze/silver/gold). The pipeline ingests raw CSV data, applies data quality 
rules, enriches and aggregates the data into business metrics, and serves 
analytical insights via DuckDB queries. Key metrics include daily revenue 
trends, state-wise revenue, monthly order volume, average order value, payment 
type breakdown, and top product categories.

## Architecture
```
Raw CSV Files (data/raw/)
        ↓
[Bronze Layer] — ingest_raw.py
Ingests raw CSVs as-is into parquet format with audit columns
        ↓
[Silver Layer] — clean_orders.py → enrich_orders.py
Cleans, validates, quarantines bad data, joins orders with
customers and payments
        ↓
[Gold Layer] — gold_metrics.py
Computes 6 business metrics as aggregated parquet files
        ↓
[Analytics Layer] — DuckDB queries on gold parquet files
```

## Tech Stack
| Tool | Purpose |
|------|---------|
| PySpark | Distributed data processing across bronze, silver and gold layers |
| DuckDB | Lightweight analytical SQL engine for querying gold parquet files |
| Pathlib | Cross-platform file path handling throughout the pipeline |
| Python logging | Structured pipeline logging with named module loggers |
| pytest | Unit testing for transformation functions |

## Project Structure
```
ecommerce-sales-pipeline/
│
├── data/
│   ├── raw/          ← original CSVs, never modified
│   ├── bronze/       ← parquet, ingested as-is
│   ├── silver/       ← parquet, cleaned and enriched
│   ├── gold/         ← parquet, aggregated business metrics
│   └── quarantine/   ← rejected rows with rejection reason
│
├── src/
│   ├── ingestion/
│   │   └── ingest_raw.py
│   ├── transformation/
│   │   ├── clean_orders.py
│   │   └── enrich_orders.py
│   ├── aggregation/
│   │   └── gold_metrics.py
│   └── utils/
│       └── spark_session.py
│
├── queries/
│   ├── analytics.sql
│   └── run_analytics.py
│
├── tests/
│   └── test_transformations.py
├── logs/
├── main.py
├── requirements.txt
└── README.md
```

## How to Run

### Prerequisites
- Python 3.8+
- Java JDK 8 or 11 (required for PySpark)

### Steps

1. Clone the repository
```bash
git clone https://github.com/Muthuraja0312/ecommerce-sales-pipeline.git
cd ecommerce-sales-pipeline
```

2. Create and activate virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Download the dataset
Download the Olist Brazilian E-Commerce dataset from Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Place the CSV files in `data/raw/`

5. Run the pipeline
```bash
# Run full pipeline
python3 main.py --stage all

# Run individual stages
python3 main.py --stage bronze
python3 main.py --stage silver
python3 main.py --stage gold
```

6. Run analytics queries
```bash
python3 queries/run_analytics.py
```

## Data Quality and Failure Handling

The silver layer applies the following data quality rules to the orders dataset:

| Issue | Detection | Action |
|-------|-----------|--------|
| Null order_id | `order_id IS NULL` | Row dropped — cannot identify order |
| Duplicate orders | Duplicate `order_id` | Deduplicated — keep first occurrence |
| Invalid order status | Value not in known status list | Quarantined with reason `invalid_status` |
| Delivered order missing delivery date | `status = delivered AND delivery_date IS NULL` | Quarantined with reason `delivered_missing_date` |
| Incorrect data types | Timestamp columns stored as string | Cast using `to_timestamp()` in silver layer |

Rejected rows are written to `data/quarantine/` with a `rejection_reason` 
column instead of being silently dropped. This prevents data loss and allows 
investigation of upstream data quality issues.

## Gold Layer Outputs

| Metric | Description | Output Path |
|--------|-------------|-------------|
| Daily revenue | Total revenue per day, ordered by date | `data/gold/daily_revenue/` |
| Revenue by state | Total revenue per Brazilian state | `data/gold/revenue_per_state/` |
| Average order value | Mean spend per order across all orders | `data/gold/average_order_value/` |
| Revenue by payment type | Revenue breakdown by payment method | `data/gold/revenue_per_payment_type/` |
| Monthly order volume | Order count per month showing growth trend | `data/gold/monthly_order_volume/` |
| Top 10 product categories | Highest revenue generating categories | `data/gold/top_product_category/` |

## Key Pipeline Features

- **Stage-based execution** — run bronze, silver, gold independently 
using `--stage` argument. Each stage reads from the previous layer's 
parquet files making reruns safe and efficient.
- **Quarantine pattern** — bad rows are never silently dropped. They are 
routed to a quarantine folder with a rejection reason for investigation.
- **Audit columns** — every bronze record carries `ingested_at` timestamp 
and `source_file` name for full traceability.
- **Structured logging** — every module logs with its full import path 
making it easy to trace which stage produced which log line.

## AWS Migration Path

This pipeline is designed to migrate to AWS with minimal code changes:

| Local | AWS Equivalent |
|-------|---------------|
| `data/raw/` | S3 bucket (raw prefix) |
| `data/bronze/` | S3 bucket (bronze prefix) |
| `data/silver/` | S3 bucket (silver prefix) |
| `data/gold/` | S3 bucket (gold prefix) |
| PySpark local | AWS Glue (serverless Spark) |
| DuckDB queries | Amazon Athena |
| `main.py` | AWS Step Functions |
| File system logs | Amazon CloudWatch |

PySpark code written for local mode runs on AWS Glue with minimal 
configuration changes — the distributed computing logic remains identical.

## Dataset
[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— 100k orders across 9 tables covering the period 2016–2018.