import numpy as np
import pandera as pa
from pandera.typing import Series

# Silver data layer

class CalendarSchema(pa.DataFrameModel):
    day_no: Series[str]  # Days since a reference day in the past
    year: Series[int]
    month: Series[int] = pa.Field(ge=1, le=12)
    year_week: Series[str]


class CatalogSchema(pa.DataFrameModel):
    sku_id: Series[str]
    store_id: Series[str]
    store_group_id: Series[str]
    dept_id: Series[str]
    cat_id: Series[str]
    price: Series[float] = pa.Field(ge=0)
    year_week: Series[str]


class SalesSchema(pa.DataFrameModel):
    sku_id: Series[str]
    store_id: Series[str]
    transaction_date: Series[np.datetime64]
    day_no: Series[str]
    sales_qty: Series[float] = pa.Field(nullable=True)


# Gold data layer

class DemandModelInputSchema(pa.DataFrameModel):
    price: Series[float]
    sales_qty: Series[float]
    sku_id: Series[str]
    store_id: Series[str]
    store_group_id: Series[str]
    dept_id: pa.Column(str)
    cat_id: pa.Column(str)
    transaction_date: Series[np.datetime64]
    year: Series[int]
    month: Series[int]
    year_week: Series[str]
    day_no: Series[str]
