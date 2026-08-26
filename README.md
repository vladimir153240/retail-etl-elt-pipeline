# GloboRetail Data Pipeline — Hybrid ETL + ELT

A production-style data pipeline that extracts retail sales and product data from AWS S3, validates and transforms it in Python, and loads it into a Snowflake star schema with materialized analytical views. Orchestrated end-to-end with Apache Airflow.

---
## What this project demonstrates

- **Hybrid ETL/ELT architecture** — transformation in Python (cheap, in-memory) and
  loading plus dimensional modelling in Snowflake, rather than paying warehouse
  compute to clean data.
- **Data contracts** — Pandera schemas at both boundaries: input validation filters
  bad rows to a date-stamped S3 quarantine zone for audit; output validation raises,
  because a failure there means a transform bug, not bad source data.
- **Dimensional modelling** — a star schema with a conformed date dimension and a
  deliberately denormalized fact table, plus four materialized views in a
  presentation layer.
- **Orchestration with a failure guard** — a five-task Airflow DAG containing an
  explicit check that blocks the ELT phase if the ETL produced empty output, so a
  silent upstream failure cannot truncate the warehouse tables.

Built as the final project for the SoftUni Data Warehousing & ETL course.


## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  S3 Raw Zone                                                                │
│  sales_data.csv  │  product_data.json                                       │
└────────┬────────────────────┬────────────────────────────────────────────── ┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Python ETL  (Airflow task: run_python_etl)                                 │
│                                                                             │
│  extract → input validation → transform → output validation → save          │
│                    │                                                        │
│                    └──► S3 Quarantine (rejected rows, date-stamped)         │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                         ┌───────────▼───────────┐
                         │  S3 Processed Zone     │
                         │  sales_data.parquet    │
                         │  product_data.parquet  │
                         └───────────┬────────────┘
                                     │
                         ┌───────────▼───────────┐
                         │  S3 Guard Check        │
                         │  (Airflow task)        │
                         │  assert rows > 0       │
                         └───────────┬────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────────┐
│  Snowflake ELT  (three sequential Airflow tasks)                            │
│                                                                             │
│  CLEANSED layer          STAR layer               PRESENTATION layer        │
│  ─────────────           ──────────               ──────────────────        │
│  SALES_CLEAN    ──────►  DIM_DATE        ──────►  MV_SALES_BY_REGION_MONTH │
│  PRODUCTS_CLEAN          DIM_PRODUCT              MV_TOP_PRODUCTS_BY_REVENUE│
│                          FACT_SALES               MV_REVENUE_TREND          │
│                          (denormalized)            MV_CATEGORY_PERFORMANCE  │
└─────────────────────────────────────────────────────────────────────────────┘
```

The pipeline follows a hybrid pattern: Python handles extraction, validation, and transformation; Snowflake handles the ELT load and dimensional modelling. This separation keeps compute costs low — transformation happens in memory rather than in Snowflake, and Snowflake only runs SQL against already-clean data.

Parquet is used in the processed zone to preserve datetime types across the S3-to-Snowflake boundary. Without `USE_LOGICAL_TYPE = TRUE` in the Snowflake file format, pandas datetime columns stored as INT64 nanoseconds would be loaded as raw integers.

---

## Tech Stack

- **Python 3.11** — ETL logic, data validation, S3 interaction
- **pandas** — data transformation and Parquet I/O
- **Pandera** — schema-based DataFrame validation (functional API)
- **PyArrow / s3fs** — Parquet serialization and S3 filesystem access
- **Apache Airflow** — orchestration via Astro Runtime 3.0 (Docker-based)
- **AWS S3** — raw data landing zone, processed zone, and quarantine zone
- **Snowflake** — cleansed tables, star schema, materialized views

---

## Project Structure

```
exam_project/
├── dags/
│   └── retail_etl_dag.py          # Airflow DAG — orchestrates the full pipeline
├── include/
│   ├── config.yaml                # S3 paths, Snowflake parameters, pipeline file names
│   ├── logger.py                  # Shared logging utility with duplicate-handler guard
│   ├── etl/
│   │   ├── extract_s3.py          # Reads raw CSV and JSON files from S3
│   │   ├── transform.py           # Column normalization, type casting, derived columns
│   │   └── load_s3_parquet.py     # Saves transformed data and rejected rows to S3
│   ├── validations/
│   │   ├── input_schemas.py       # Pandera schemas validated against original messy columns
│   │   ├── output_schemas.py      # Pandera schemas validated against clean transformed columns
│   │   ├── validate_inputs.py     # Input validation — filters invalid rows, does not raise
│   │   └── validate_outputs.py    # Output validation — raises on any transform failure
│   └── sql/
│       ├── setup_snowflake.txt    # One-time infrastructure setup — run manually in Snowflake UI
│       ├── elt_load.sql           # TRUNCATE + COPY INTO cleansed tables from S3
│       ├── elt_star_schema.sql    # Builds DIM_DATE, DIM_PRODUCT, FACT_SALES
│       └── elt_mvs.sql            # Creates four materialized views in PRESENTATION schema
└── requirements.txt
```

---

## Pipeline Flow

The DAG `retail_etl_dag` runs five tasks in sequence:

**Task 1 — `run_python_etl`**
Extracts `sales_data.csv` and `product_data.json` from the S3 raw zone. Validates inputs with Pandera against the original messy column names — rows with negative prices, zero quantities, or invalid order statuses are filtered out and written to the S3 quarantine zone with a date-stamped filename for audit. Valid rows are transformed: column names are normalized, dates are parsed, `revenue` and `discounted_price` are computed, and `price_category` is assigned using fixed business-defined bins (Low: ≤€50, Medium: €50–200, High: >€200). Output schemas are then validated — any failure here indicates a transform bug and raises immediately. Clean data is saved as Parquet to the S3 processed zone.

**Task 2 — `check_s3_output`**
Reads the saved Parquet files from S3 and asserts they contain rows. This guard prevents the ELT phase from running if the ETL produced empty files — which would cause the downstream TRUNCATE to wipe Snowflake tables and load nothing.

**Task 3 — `elt_load`**
Truncates `CLEANSED.SALES_CLEAN` and `CLEANSED.PRODUCTS_CLEAN`, then runs `COPY INTO` to load fresh Parquet data from S3. `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` handles any column casing differences between the Parquet schema and the Snowflake table definition.

**Task 4 — `elt_star_schema`**
Rebuilds the star schema with `CREATE OR REPLACE TABLE`. `FACT_SALES` is denormalized — it includes `CATEGORY`, `BRAND`, `RATING`, and `RATING_CATEGORY` from products via a LEFT JOIN at creation time. This is required because Snowflake Standard Edition materialized views do not support JOINs, so all reporting attributes must already be present in the fact table.

**Task 5 — `elt_mvs`**
Recreates four materialized views in the `PRESENTATION` schema. All views read from `STAR.FACT_SALES` only (single-table, no JOINs), which is a requirement of Snowflake Standard Edition MVs.

---

## Data Model

### CLEANSED layer
Landing tables for validated, typed data loaded directly from S3 Parquet files. `NUMBER(10,2)` is used for financial columns to avoid floating-point precision errors.

### STAR layer

| Table | Description |
|---|---|
| `DIM_DATE` | One row per unique sale date. `DATE_KEY` uses YYYYMMDD integer format — standard surrogate key for date dimensions. |
| `DIM_PRODUCT` | Product dimension from `PRODUCTS_CLEAN`. One row per product. |
| `FACT_SALES` | Central fact table with one row per transaction. Denormalized to include `CATEGORY`, `BRAND`, `RATING`, and `RATING_CATEGORY` from products for MV compatibility. |

### PRESENTATION layer

| View | Description |
|---|---|
| `MV_SALES_BY_REGION_MONTH` | Revenue and order count grouped by region and calendar month |
| `MV_TOP_PRODUCTS_BY_REVENUE` | Total and average revenue per product with category and brand context |
| `MV_REVENUE_TREND` | Monthly revenue trend in chronological order |
| `MV_CATEGORY_PERFORMANCE` | Revenue, order count, and price range broken down by price category |

---

## Setup

### Prerequisites
- WSL Ubuntu with Docker installed
- Astro CLI installed
- AWS S3 bucket with source files uploaded to `<bucket>/exam/raw/`
- Snowflake account with `ACCOUNTADMIN` access

### One-time Snowflake infrastructure setup
Run `include/sql/setup_snowflake.txt` manually in the Snowflake UI. This creates the role, warehouse, database, schemas, Parquet file format, S3 external stage, and cleansed landing tables. This script is idempotent — all statements use `CREATE IF NOT EXISTS` or `CREATE OR REPLACE`.

### Start Airflow
```bash
cd ~/exam_project
astro dev start
```

Airflow UI is available at `http://exam-project.localhost:6563` (default credentials: `admin` / `admin`).

### Configure Airflow connections

In Airflow UI → Admin → Connections, add two connections:

| Conn ID | Type | Details |
|---|---|---|
| `aws_conn_id` | Amazon Web Services | AWS Access Key ID and Secret Access Key |
| `snowflake_conn_id` | Snowflake | Account, login, password, database (`EXAM_PROJECT`), warehouse (`COMPUTE_WH`), role (`ACCOUNTADMIN`) |

### Trigger the pipeline
Unpause `retail_etl_dag` in the Airflow UI and click Trigger. The full run completes in approximately 2–3 minutes.

---

## Expected Results

| Layer | Table / View | Rows |
|---|---|---|
| CLEANSED | SALES_CLEAN | 2,450 |
| CLEANSED | PRODUCTS_CLEAN | 149 |
| STAR | DIM_DATE | 81 |
| STAR | DIM_PRODUCT | 149 |
| STAR | FACT_SALES | 2,450 |
| PRESENTATION | MV_SALES_BY_REGION_MONTH | 7 |
| PRESENTATION | MV_TOP_PRODUCTS_BY_REVENUE | 99 |
| PRESENTATION | MV_REVENUE_TREND | 3 |
| PRESENTATION | MV_CATEGORY_PERFORMANCE | 3 |

50 sales rows with negative prices are rejected at input validation and written to `s3://<bucket>/exam/quarantine/sales_rejected_<date>.parquet`. 1 product row with a null launch date is dropped during transformation.
