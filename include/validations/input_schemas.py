import logging
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaError

# sales_input_schema - Validation  before any transformation.
sales_input_schema = DataFrameSchema(
    columns={
        "sales id": Column(
            int,
            checks=Check.gt(0, error = "Sales ID must be a positive int"),
            nullable=False),
        "proDuct Id": Column(
            int,
            checks=Check.gt(0, error = "Product ID must be a positive int"),
            nullable=False),
        "Region": Column(
            str,
            nullable=True),
        "qty": Column(
            int,
            checks=Check.gt(0, error ="Quantity must be greater than 0"),
            nullable=False),
        "Price": Column(
            float,
            checks=Check.gt(0, error = "Price must be greater than 0"),
            nullable=False),
        "Time stamp": Column(
            str,
            nullable=False),
        "discount": Column(
            float,
            checks=[Check.ge(0, error = "Discount cannot be negative!"),
                    Check.le(1, error = "Discount cannot be greater than 1.0 (100%)")],
            nullable=False),
        "order_status": Column(
            str,
            checks=Check.isin(["Completed", "Shipped", "Pending", "Returned"],
                              error="Order Status must be within the 4 predefined categories"),
            nullable=False),
    },
    strict=False,   # ignore any extra columns not listed above
    coerce=True,    # attempt dtype conversion before validating
    name="sales_input_schema",
)


# product input schema - validation before any transformation
products_input_schema = DataFrameSchema(
    columns={
        "product_id": Column(
            int,
            checks=Check.gt(0,error= "Product ID must be positive number"),
            nullable=False),
        "category": Column(
            str,
            checks=Check.isin(
            ["Grocery", "Clothing", "Electronics","Sports", "Toys", "Home"],
            error = "Category must be within the predefined categories"),
            nullable=False),
        "brand": Column(
            str,
            nullable=False),
        "rating":Column(
            float,
            checks=[Check.ge(0.0, error = "Rating cannot be negative"),
                    Check.le(5.0,error = "Rating cannot be greater than 5.0")],
            nullable=False),
        "in_stock": Column(
            bool,
            nullable=False),
        "launch_date": Column(
            str,
            nullable=True),
    },
    strict=False,
    coerce=True,
    name="products_input_schema",
)