import logging
import os
import yaml
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

def load_config() -> dict:
    """
    Loads config file
    """
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_storage_options(config: dict) -> dict:
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not aws_key or not aws_secret:
        raise EnvironmentError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
    return {"key": aws_key, "secret": aws_secret}


def extract_sales(config: dict) -> pd.DataFrame:
    """
    # Extract sales - reads sales_data.csv from S3 raw folder
    """
    bucket = config["s3"]["bucket"]
    raw_prefix = config["s3"]["raw_prefix"]
    sales_file= config["pipeline"]["sales_file"]
    s3_path= f"s3://{bucket}/{raw_prefix}/{sales_file}"
    storage_options = get_storage_options(config)

    logger.info(f"Extracting sales data from: {s3_path}")

    try:
        sales_df = pd.read_csv(s3_path, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Failed to extract sales data from {s3_path}: {e}")
        raise

    logger.info(f"Successfully extracted sales data — shape: {sales_df.shape}")
    return sales_df


def extract_products(config: dict) -> pd.DataFrame:
    """
    # Extract products — reads product_data.json from S3 raw folder
    """
    bucket = config["s3"]["bucket"]
    raw_prefix = config["s3"]["raw_prefix"]
    products_file = config["pipeline"]["products_file"]
    s3_path = f"s3://{bucket}/{raw_prefix}/{products_file}"
    storage_options = get_storage_options(config)

    logger.info(f"Extracting products data from: {s3_path}")

    try:
        products_df = pd.read_json(s3_path, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Failed to extract products data from {s3_path}: {e}")
        raise

    logger.info(f"Successfully extracted products data — shape: {products_df.shape}")
    return products_df

#Quick validation for loading from S3
if __name__ == "__main__":
    config      = load_config()
    sales_df    = extract_sales(config)
    products_df = extract_products(config)

    print("\n--- SALES ---")
    # print(f"Shape:   {sales_df.shape}")
    # print(f"Columns: {list(sales_df.columns)}")
    print(sales_df.order_status.unique())
    # print(sales_df.head(3))

    print("\n--- PRODUCTS ---")
    # print(f"Shape:   {products_df.shape}")
    # print(f"Columns: {list(products_df.columns)}")
    # print(products_df.head(3))
    print(products_df.category.value_counts())