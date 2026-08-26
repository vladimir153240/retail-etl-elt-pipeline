import logging
import numpy as np
import pandas as pd
from datetime import date

logger = logging.getLogger(__name__)

def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Sales input data
    """

    # Normalize all column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Explicit renames of two columns with clean column names
    df = df.rename(columns={
        "qty": "quantity",
        "time_stamp": "sale_date",
    })

    # Remove duplicates on primary key
    before = len(df)
    df = df.drop_duplicates(subset=["sales_id"], keep="first").reset_index(drop=True)
    logger.info(f"[transform_sales] Dropped {before - len(df)} duplicate rows.")

    # Normalize the region (fill nulls)
    df["region"] = df["region"].str.title().fillna("Unknown")

    # Normalize order_status
    df["order_status"] = df["order_status"].str.title()

    # Parse sale_date string to proper datetime format
    #Source format: "01-01-24 0:00"
    df["sale_date"] = pd.to_datetime(df["sale_date"], format="%d-%m-%y %H:%M")

    # Compute discounted_price: unit price after discount
    df["discounted_price"] = df["price"] * (1 - df["discount"])

    # Compute revenue: full amount after discount applied
    df["revenue"] = df["discounted_price"] * df["quantity"]

    # 9. Assign price category - bins are Low: €0–50 | Medium: €50–150 | High: €150+
    df["price_category"] = pd.cut(
        df["price"],
        bins=[0,50,150, float("inf")],
        labels=["Low", "Medium", "High"],
        include_lowest=True,
    ).astype(str)

    logger.info(f"[transform_sales] Transform complete with shape: {df.shape}")
    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Product input data
    :param df:
    :return:
    """

    # Normalize column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Remove duplicates on primary key
    before = len(df)
    df = df.drop_duplicates(subset=["product_id"], keep="first").reset_index(drop=True)
    logger.info(f"[transform_products] Dropped {before - len(df)} duplicate rows.")

    # Normalize category
    df["category"] = df["category"].str.title()

    #Get the actual "Brand" name
    df["brand"] = df["brand"].str.replace("Brand", "", regex=False).str.strip()

    # Parse launch_date string to proper datetime
    df["launch_date"] = pd.to_datetime(df["launch_date"], errors="coerce")

    # Drop rows where launch_date is still null after parsing
    before = len(df)
    df = df.dropna(subset=["launch_date"]).reset_index(drop=True)
    logger.info(f"[transform_products] Dropped {before - len(df)} rows with null launch_date.")

    # Compute days_since_launch - integer: how old is the product today
    today = pd.Timestamp(date.today())
    df["days_since_launch"] = (today - df["launch_date"]).dt.days

    logger.info(f"[transform_products] Transform complete with shape: {df.shape}")
    return df