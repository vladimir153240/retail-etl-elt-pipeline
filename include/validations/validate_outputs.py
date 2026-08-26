import logging
import pandera.pandas as pa
import pandas as pd

from include.validations.output_schemas import sales_output_schema, products_output_schema

logger = logging.getLogger(__name__)

#Output validation raises on failure: a failed output schema means transform.py has a bug, not a data quality issue.

def validate_sales_output(transformed_sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the validation schema on transformed_sales_df
    :param transformed_sales_df:
    :return:
    """
    try:
        validated_df = sales_output_schema.validate(transformed_sales_df, lazy=True)
        logger.info(f"[sales_output] All {len(validated_df)} rows passed output validation.")
        return validated_df

    except pa.errors.SchemaErrors as exc:
        logger.error("[sales_output] Output validation failed - bug in transform_sales()")
        logger.error(
            f"\n{exc.failure_cases[['column', 'check', 'failure_case']].to_string()}"
        )
        raise


def validate_products_output(transformed_products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the validation schema on transformed_products_df
    :param transformed_products_df:
    :return:
    """
    try:
        validated_df = products_output_schema.validate(transformed_products_df, lazy=True)
        logger.info(f"[products_output] All {len(validated_df)} rows passed output validation.")
        return validated_df

    except pa.errors.SchemaErrors as exc:
        logger.error("[products_output] Output validation failed - bug in transform_products()")
        logger.error(
            f"\n{exc.failure_cases[['column', 'check', 'failure_case']].to_string()}"
        )
        raise