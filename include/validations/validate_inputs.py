import logging
import pandera.pandas as pa
import pandas as pd

from include.validations.input_schemas import sales_input_schema, products_input_schema

logger = logging.getLogger(__name__)

def _validate(df: pd.DataFrame, schema: pa.DataFrameSchema, name: str) -> tuple[pd.DataFrame, pd.DataFrame]:

    try:
        validated_df = schema.validate(df, lazy=True)
        logger.info(f"[{name}] All {len(validated_df)} rows passed validation.")
        return validated_df, pd.DataFrame()

    except pa.errors.SchemaErrors as exc:
        # exc.failure_cases has columns: schema_context, column, check, index, failure_case
        # "index" is the row index in the original DataFrame that failed
        failed_indices = exc.failure_cases["index"].dropna().unique().astype(int)

        valid_df   = df.drop(index=failed_indices).reset_index(drop=True)
        invalid_df = df.loc[failed_indices].reset_index(drop=True)

        logger.warning(
            f"[{name}] {len(failed_indices)} invalid rows rejected. "
            f"{len(valid_df)} rows passed."
        )
        logger.warning(
            f"[{name}] Failure summary:\n"
            f"{exc.failure_cases[['column', 'check', 'failure_case']].to_string()}"
        )

        return valid_df, invalid_df


# Public functions — called from the Airflow DAG
def validate_sales_input(raw_sales_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _validate(raw_sales_df, sales_input_schema, "sales_input")


def validate_products_input(raw_products_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _validate(raw_products_df, products_input_schema, "products_input")
