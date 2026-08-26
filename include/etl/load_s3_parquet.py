import logging
import pandas as pd

from include.etl.extract_s3 import get_storage_options

logger = logging.getLogger(__name__)

def save_sales_to_s3(df: pd.DataFrame, config: dict) -> str:
    """
    Save sales file to s3 as parquet
    :param df:
    :param config:
    :return:
    """
    bucket = config["s3"]["bucket"]
    processed_prefix = config["s3"]["processed_prefix"]
    file_name = config["pipeline"]["sales_file"].replace(".csv", ".parquet")
    s3_path = f"s3://{bucket}/{processed_prefix}/{file_name}"
    storage_options  = get_storage_options(config)

    logger.info(f"Saving sales data to: {s3_path}")

    try:
        df.to_parquet(s3_path, index=False, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Failed to save sales data to {s3_path}: {e}")
        raise

    logger.info(f"Successfully saved sales data - {len(df)} rows → {s3_path}")
    return s3_path


def save_products_to_s3(df: pd.DataFrame, config: dict) -> str:
    """
    Save products file to s3 as parquet
    :param df:
    :param config:
    :return:
    """
    bucket = config["s3"]["bucket"]
    processed_prefix = config["s3"]["processed_prefix"]
    file_name = config["pipeline"]["products_file"].replace(".json", ".parquet")
    s3_path = f"s3://{bucket}/{processed_prefix}/{file_name}"
    storage_options  = get_storage_options(config)

    logger.info(f"Saving products data to: {s3_path}")

    try:
        df.to_parquet(s3_path, index=False, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Failed to save products data to {s3_path}: {e}")
        raise

    logger.info(f"Successfully saved products data - {len(df)} rows → {s3_path}")
    return s3_path
