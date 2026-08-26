import sys
import pandera.pandas as pa
import pandas as pd
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

pd.set_option('display.max_columns', None)

from include.etl.extract_s3 import load_config, extract_sales, extract_products, get_storage_options
from include.validations.validate_inputs import validate_sales_input, validate_products_input
from include.etl.transform import transform_sales, transform_products
from validations.input_schemas import sales_input_schema
from validations.validate_outputs import validate_sales_output, validate_products_output
from include.etl.load_s3_parquet import save_sales_to_s3, save_products_to_s3

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    config = load_config()
    sales_df = extract_sales(config)
    products_df = extract_products(config)

    print("\n--- SALES ---")
    print(f"Shape:   {sales_df.shape}")
    print(f"Columns: {list(sales_df.columns)}")
    print(sales_df.head(10))

    print("\n--- PRODUCTS ---")
    print(f"Shape:   {products_df.shape}")
    print(f"Columns: {list(products_df.columns)}")
    print(products_df.head(10))

    # valid_sales, invalid_sales = validate_sales_input(sales_df)
    # valid_products, invalid_products = validate_products_input(products_df)
    #
    # print(f"\nSales - valid: {len(valid_sales)}, rejected: {len(invalid_sales)}")
    # print(f"Products - valid: {len(valid_products)}, rejected: {len(invalid_products)}")

    # print("\nSample rejected sales rows:")
    # print(invalid_sales[["sales id", "proDuct Id", "Price", "order_status"]].head())


    # try:
    #     sales_input_schema.validate(sales_df, lazy=True)
    # except pa.errors.SchemaErrors as exc:
    #     # Full failure report with row indices
    #     print("\n--- FAILURE CASES WITH ROW INDEX ---")
    #     print(exc.failure_cases[["index", "column", "check", "failure_case"]])
    #
    #     # Cross-reference: see the full original row for each failure
    #     failed_indices = exc.failure_cases["index"].dropna().unique().astype(int)
    #     print("\n--- FULL ORIGINAL ROWS THAT FAILED ---")
    #     print(sales_df.loc[failed_indices].to_string())

    valid_sales, _ = validate_sales_input(sales_df)
    valid_products, _ = validate_products_input(products_df)

    transformed_sales    = transform_sales(valid_sales)
    transformed_products = transform_products(valid_products)

    print("\n--- SALES TRANSFORMED ---")
    print(f"Shape:   {transformed_sales.shape}")
    print(f"Columns: {list(transformed_sales.columns)}")
    print(f"Dtypes:\n{transformed_sales.dtypes}")
    print(transformed_sales[["sales_id", "region", "sale_date", "revenue", "discounted_price", "price_category"]].head())

    print("\n--- PRODUCTS TRANSFORMED ---")
    print(f"Shape:   {transformed_products.shape}")
    print(f"Columns: {list(transformed_products.columns)}")
    print(transformed_products[["product_id", "category", "brand" ,"launch_date", "days_since_launch"]].head())

    # # View statistical summary (min, max, mean, quartiles)
    # print(transformed_sales["price"].describe())
    #
    # # View specific percentiles to understand the spread
    # print(transformed_sales["price"].quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95]))

    final_sales = validate_sales_output(transformed_sales)
    final_products = validate_products_output(transformed_products)

    print(f"\nSales output valid:    {len(final_sales)} rows")
    print(f"Products output valid: {len(final_products)} rows")

    print(final_products.brand.unique())

    #Files already loaded to s3
    # sales_path    = save_sales_to_s3(final_sales, config)
    # products_path = save_products_to_s3(final_products, config)

    # print(f"\nSales saved to:    {sales_path}")
    # print(f"Products saved to: {products_path}")
    #
    # # Read back from S3 to confirm file was written correctly
    # storage_options = get_storage_options(config)
    # sales_check    = pd.read_parquet(sales_path,    storage_options=storage_options)
    # products_check = pd.read_parquet(products_path, storage_options=storage_options)
    #
    # print(f"\nRead-back verification:")
    # print(f"Sales    — shape: {sales_check.shape},    columns: {list(sales_check.columns)}")
    # print(f"Products — shape: {products_check.shape}, columns: {list(products_check.columns)}")












