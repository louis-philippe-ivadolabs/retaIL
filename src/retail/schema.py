import numpy as np
import pandera as pa

model_input_schema = pa.DataFrameSchema({
    "effective_price": pa.Column(float, checks=pa.Check.ge(0.0)),
    "sales_qty": pa.Column(float, checks=pa.Check.ge(0.0)),
    "sku_id": pa.Column(str),
    "store_id": pa.Column(str),
    "dept_id": pa.Column(str),
    "cat_id": pa.Column(str),
    "transaction_date": pa.Column(np.datetime64),
    "year": pa.Column(int, checks=pa.Check.gt(1900)),
    "month": pa.Column(int, checks=[pa.Check.ge(1), pa.Check.le(12)]),
    "year_week": pa.Column(str)
})


sku_catalog_schema = pa.DataFrameSchema({
    "price": pa.Column(float, checks=pa.Check.ge(0.0)),
    "sku_id": pa.Column(str),
    "store_id": pa.Column(str),
    "dept_id": pa.Column(str),
    "cat_id": pa.Column(str),
    "effective_start_date": pa.Column(np.datetime64),
    "effective_end_date": pa.Column(np.datetime64),
})
