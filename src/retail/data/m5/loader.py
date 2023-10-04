import os
import os.path

import pandas as pd
from pandas import DataFrame

from retail.domain import DemandDataset


class M5DataLoader:
    def _get_path_relative_to_this_file(self, relative_to_project: str) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../../', relative_to_project)

    def _load_df(self, file_name: str) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self._get_path_relative_to_this_file('data'), "m5-forecasting-accuracy", file_name))

    def load_demand_dataset(self,
                            sales_file: str) -> DataFrame:
        # See if we have the merge dataset already
        demand_data_path = os.path.join(self._get_path_relative_to_this_file('data'), 'demand.csv')
        if os.path.exists(demand_data_path):
            return pd.read_csv(demand_data_path)

        self._load_raw_sales(
            sales_file
        )._add_calendar(
        )._add_prices(
        )._conform(
        )

        # Save demand dataset
        self.sales_df.to_csv(demand_data_path, index=False)

        return self.sales_df

    def _load_raw_sales(self, sales_file: str):
        key_columns = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
        self.sales_df = self._load_df(sales_file)
        self.sales_df = self.sales_df.melt(id_vars=key_columns, var_name='d',value_name='sales_qty')
        return self

    def _add_calendar(self):
        self.calendar_df = self._load_df("calendar.csv")
        calendar_df = self.calendar_df[['date','d', 'wm_yr_wk']]
        self.sales_df = pd.merge(self.sales_df, self.calendar_df, on='d')
        return self

    def _add_prices(self):
        self.prices_df = self._load_df("sell_prices.csv")
        self.sales_df = pd.merge(self.sales_df, self.prices_df, on=['store_id', 'item_id', 'wm_yr_wk'])
        return self

    def _conform(self):
        self.sales_df = self.sales_df.rename(columns={
                                                "item_id": "sku_id",
                                                "wm_yr_wk": "year_week",
                                                "date": "transaction_date",
                                                "sell_price": "effective_price"
                                              }
                                            )
        self.sales_df.sales_qty = self.sales_df.sales_qty.astype(float)
        self.sales_df.year_week = self.sales_df.year_week.astype(str)
        self.sales_df.transaction_date = pd.to_datetime(self.sales_df.transaction_date)
