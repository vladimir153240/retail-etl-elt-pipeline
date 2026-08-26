from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.etl.extract_s3 import extract_sales, extract_products, load_config
from include.etl.transform import transform_sales, transform_products
from include.etl.load_s3_parquet import save_sales_to_s3, save_products_to_s3
from include.validations.validate_inputs import validate_sales_input, validate_products_input
from include.validations.validate_outputs import validate_sales_output, validate_products_output
from include.logger import setup_logger

logger = setup_logger(__name__)


# ---------------------------------------------------------------------------
# read_sql — reads SQL file content at DAG parse time
# Path: dags/retail_etl_dag.py → project root → include/sql/<filename>
# ---------------------------------------------------------------------------
def read_sql(filename: str) -> str:
    sql_path = Path(__file__).parent.parent / "include" / "sql" / filename
    return sql_path.read_text()


default_args = {
    "owner": "vlado",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id = "retail_etl_dag",
    description = "GloboRetail hybrid ETL (Python/S3) + ELT (Snowflake) pipeline",
    schedule  = "@daily",
    start_date = datetime(2026, 7, 16),
    catchup = False,
    default_args = default_args,
    tags = ["retail", "etl", "snowflake"],
)
def retail_etl_pipeline():
    """
    Runs all five ETL steps: extract → validate inputs →
    # transform → validate outputs → save to S3 processed zone
    """
    @task()
    def run_python_etl() -> dict:
        config = load_config()

        # Extract raw data from S3
        sales_df = extract_sales(config)
        products_df = extract_products(config)

        # Input validation — filters bad rows, does not raise
        valid_sales, invalid_sales = validate_sales_input(sales_df)
        valid_products, invalid_products = validate_products_input(products_df)
        logger.info(f"Sales input: {len(valid_sales)} valid / {len(invalid_sales)} rejected")
        logger.info(f"Products input: {len(valid_products)} valid / {len(invalid_products)} rejected")

        # Transform
        transformed_sales = transform_sales(valid_sales)
        transformed_products = transform_products(valid_products)

        # Output validation - raises on failure (transform bug, not data issue)
        validate_sales_output(transformed_sales)
        validate_products_output(transformed_products)

        # Save to S3 processed zone (Snowflake stage reads from here)
        sales_path = save_sales_to_s3(transformed_sales, config)
        products_path = save_products_to_s3(transformed_products, config)
        logger.info(f"Saved to S3 — sales: {sales_path}")
        logger.info(f"Saved to S3 — products: {products_path}")

        return {"sales": sales_path, "products": products_path}

    # -----------------------------------------------------------------------
    # TASK 2 - ELT: LOAD
    # TRUNCATE + COPY INTO cleansed tables from S3 Parquet files.
    # Snowflake executes this; Python only submits and waits.
    # -----------------------------------------------------------------------
    elt_load = SQLExecuteQueryOperator(
        task_id = "elt_load",
        conn_id = "snowflake_conn_id",
        sql = read_sql("elt_load.sql"),
        split_statements=True,
    )

    # -----------------------------------------------------------------------
    # TASK 3 — ELT: STAR SCHEMA
    # Rebuilds DIM_DATE, DIM_PRODUCT, FACT_SALES from cleansed tables.
    # -----------------------------------------------------------------------
    elt_star_schema = SQLExecuteQueryOperator(
        task_id = "elt_star_schema",
        conn_id = "snowflake_conn_id",
        sql = read_sql("elt_star_schema.sql"),
        split_statements=True,
    )

    # -----------------------------------------------------------------------
    # TASK 4 — ELT: MATERIALIZED VIEWS
    # Recreates all four analytical MVs in the PRESENTATION schema.
    # -----------------------------------------------------------------------
    elt_mvs = SQLExecuteQueryOperator(
        task_id = "elt_mvs",
        conn_id = "snowflake_conn_id",
        sql = read_sql("elt_mvs.sql"),
        split_statements=True,
    )

    # -----------------------------------------------------------------------
    # DAG FLOW
    # Python ETL must complete before any Snowflake task starts.
    # ELT tasks run sequentially: load → star schema → materialized views.
    # -----------------------------------------------------------------------
    run_python_etl() >> elt_load >> elt_star_schema >> elt_mvs

retail_etl_pipeline()