import logging

import pandas as pd
import pandera as pa

from retail.demand.demand_model_data import DemandModelData
from retail.domain import Transformer


class NoSaleSkus(Transformer):
    '''
    Given the items in the catalog, insert sales of 0 for every day of the scope
    '''
    def __init__(self,
                 demand_model_data: DemandModelData,
                 logger: logging.Logger = None):
        self._demand_model_data = demand_model_data
        self._logger = logger or logging.getLogger(__name__)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        yw_date_df = self._demand_model_data.calendar_df.groupby(["year_week", "date"]).size().reset_index()[["year_week", "date"]]
        df = pd.merge(self._demand_model_data.gold_df, yw_date_df, on="year_week", how="right")
        # Find dates that are missing from the sales data but present in the catalog
        df = df[df.date_x != df.date_y]
        df['sales_qty'] = 0
        return pd.concat((self._demand_model_data.gold_df, df))
