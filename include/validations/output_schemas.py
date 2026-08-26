import pandera.pandas as pa
from pandera.pandas import DataFrameSchema, Column, Check


# SALES OUTPUT SCHEMA - Validates after transformation.
sales_output_schema = DataFrameSchema(
    columns={
        "sales_id": Column(
            int,
            checks=Check.gt(0, error="sales_id must be > 0 - check rename step in transform_sales()"),
            nullable=False,
        ),
        "product_id": Column(
            int,
            checks=Check.gt(0, error="product_id must be > 0 - check rename step in transform_sales()"),
            nullable=False,
        ),
        "region": Column(
            str,
            nullable=False,
            checks=Check.isin(
                ["North", "South", "East", "West", "Unknown"],
                error="region has unexpected value - check .str.title() and .fillna() in transform_sales()"
            ),
        ),
        "quantity": Column(
            int,
            checks=Check.gt(0,
                            error="quantity must be > 0 - check rename from qty in transform_sales()"),
            nullable=False,
        ),
        "price": Column(
            float,
            checks=Check.gt(0,
                            error="price must be > 0 - negative prices should have been rejected in validate_inputs"),
            nullable=False,
        ),
        "sale_date": Column(
            pa.DateTime,
            nullable=False,
            checks=Check(
                lambda s: s.notna().all(),
                error="sale_date has nulls - check pd.to_datetime() format string '%d-%m-%y %H:%M' in transform_sales()"
            ),
        ),
        "discount": Column(
            float,
            checks=[
                Check.ge(0, error="discount must be >= 0 - check source data or input validation"),
                Check.le(1, error="discount must be <= 1 - value is out of 0-1 range"),
            ],
            nullable=False,
        ),
        "order_status": Column(
            str,
            checks=Check.isin(
                ["Completed", "Shipped", "Pending", "Returned"],
                error="order_status has unexpected value - check .str.title() in transform_sales()"
            ),
            nullable=False,
        ),
        "revenue": Column(
            float,
            checks=Check.gt(0, error="revenue must be > 0 - check formula: discounted_price * quantity  in transform_sales()"),
            nullable=False,
        ),
        "discounted_price": Column(
            float,
            checks=Check.gt(0, error="discounted_price must be > 0 — check formula: price * (1 - discount) in transform_sales()"),
            nullable=False,
        ),
        "price_category": Column(
            str,
            checks=Check.isin(
                ["Low", "Medium", "High"],
                error="price_category has unexpected value - check pd.cut() bins and labels in transform_sales()"
            ),
            nullable=False,
        ),
    },
    strict=False,
    coerce=True,
    name="sales_output_schema",
)


# PRODUCTS OUTPUT SCHEMA - Validations after transformations
products_output_schema = DataFrameSchema(
    columns={
        "product_id": Column(
            int,
            checks=Check.gt(0, error="product_id must be > 0 - check source data"),
            nullable=False,
        ),
        "category": Column(
            str,
            checks=Check.isin(
                ["Grocery", "Clothing", "Electronics", "Sports", "Toys", "Home"],
                error="category has unexpected value - check .str.title() in transform_products()"
            ),
            nullable=False,
        ),
        "brand": Column(str, nullable=False),
        "rating": Column(
            float,
            checks=[
                Check.ge(0.0, error="rating must be >= 0.0"),
                Check.le(5.0, error="rating must be <= 5.0"),
            ],
            nullable=False,
        ),
        "in_stock": Column(bool, nullable=False),
        "launch_date": Column(
            pa.DateTime,
            nullable=False,
            checks=Check(
                lambda s: s.notna().all(),
                error="launch_date has nulls - null rows should have been dropped in transform_products()"
            ),
        ),
        "days_since_launch": Column(
            int,
            checks=Check.ge(0, error="days_since_launch must be >= 0 - check date subtraction in transform_products()"),
            nullable=False,
        ),
    },
    strict=False,
    coerce=True,
    name="products_output_schema",
)