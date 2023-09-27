import os
import os.path

import pandas as pd

from retail.domain import DemandDataset


class M5DataLoader:

    def _get_path_relative_to_this_file(self, relative_to_project: str) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../../', relative_to_project)

    def _load_df(self, competition_id: str, file_name: str) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self._get_path_relative_to_this_file('data'), competition_id, file_name))

    def load_demand_dataset(self) -> DemandDataset:
        # See if we have the merge dataset already
        demand_data_path = os.path.join(self._get_path_relative_to_this_file('data'), 'm5-forecasting-accuracy', 'demand.csv')
        if os.path.exists(demand_data_path):
            return DemandDataset(pd.read_csv(demand_data_path))

        calendar_df = self._load_df('m5-forecasting-accuracy', 'calendar.csv')
        sales_train_evaluation_df = self._load_df('m5-forecasting-accuracy', 'sales_train_evaluation.csv')
        # sales_train_validation_df = self._load_df('m5-forecasting-accuracy', 'sales_train_validation.csv')
        sell_prices_df = self._load_df('m5-forecasting-accuracy', 'sell_prices.csv')

        key_columns = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
        sales_train_evaluation_df = sales_train_evaluation_df.melt(id_vars=key_columns, var_name='d', value_name='sales_qty')

        calendar_df = calendar_df[['date', 'd', 'wm_yr_wk']]
        sales_train_evaluation_df = pd.merge(sales_train_evaluation_df, calendar_df, on='d')

        sales_train_evaluation_df = pd.merge(sales_train_evaluation_df, sell_prices_df, on=['store_id', 'item_id', 'wm_yr_wk'])

        sales_train_evaluation_df = sales_train_evaluation_df.rename(columns={
                "item_id": "sku_id",
                "wm_yr_wk": "year_week",
                "date": "transaction_date",
                "sell_price": "effective_price"
            }
        )
        sales_train_evaluation_df.sales_qty = sales_train_evaluation_df.sales_qty.astype(float)
        sales_train_evaluation_df.year_week = sales_train_evaluation_df.year_week.astype(str)

        # Save demand dataset
        sales_train_evaluation_df.to_csv(demand_data_path, index=False)

        return DemandDataset(sales_train_evaluation_df)
