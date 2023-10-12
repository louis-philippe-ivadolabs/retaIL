import os
import os.path

import pandas as pd
import pandera as pa

from retail.data_layer.interfaces import BronzeToSilver

from retail.data_layer.schema import CalendarSchema, CatalogSchema, SalesSchema


class M5DataLoader(BronzeToSilver):

    def __init__(self,
                 sales_file="sales_train_evaluation.csv",
                 catalog_file="sell_prices.csv",
                 calendar_file="calendar.csv"
                 ):
        self._sales_file = sales_file
        self._catalog_file = catalog_file
        self._calendar_file = calendar_file

    def _get_path_relative_to_this_file(self, relative_to_project: str) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../../', relative_to_project)

    def _load_df(self, file_name: str) -> pd.DataFrame:
        return pd.read_csv(
            os.path.join(self._get_path_relative_to_this_file('data'), "m5-forecasting-accuracy-toy", file_name))

    @pa.check_types
    def get_calendar_df(self) -> pa.typing.DataFrame[CalendarSchema]:
        calendar_df = self._load_df(self._calendar_file)
        calendar_df = calendar_df[['d', 'wm_yr_wk', 'month', 'year', 'date']]
        calendar_df = calendar_df.rename(columns={'d': 'day_no',
                                                  'wm_yr_wk': 'year_week'})
        calendar_df.year_week = calendar_df.year_week.astype(str)
        calendar_df.day_no = calendar_df.day_no.astype(str)
        return calendar_df

    @pa.check_types
    def get_catalog_df(self) -> pa.typing.DataFrame[CatalogSchema]:
        catalog_df = self._load_df(self._catalog_file)

        catalog_df = catalog_df.rename(columns={'item_id': 'sku_id',
                                                'wm_yr_wk': 'year_week',
                                                'sell_price': 'price'})
        catalog_df.year_week = catalog_df.year_week.astype(str)

        # Add sku fields (e.g. category, department) from the sales data
        catalog_df = pd.merge(catalog_df, self._get_sku_fields_from_sales(), on=['sku_id'], how='left')
        catalog_df["store_group_id"] = 'whole_banner'

        return catalog_df

    @pa.check_types
    def get_sales_df(self) -> pa.typing.DataFrame[SalesSchema]:
        sales_df = self._load_df(self._sales_file)
        key_columns = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
        sales_df = sales_df.melt(id_vars=key_columns, var_name='day_no', value_name='sales_qty')

        # add transaction_date from calendar
        calendar_df = self.get_calendar_df()
        sales_df = pd.merge(sales_df, calendar_df[['day_no', 'date']], on='day_no', how="left")

        sales_df = sales_df[["item_id", "store_id", "day_no", "sales_qty", "date", "dept_id", "cat_id"]]
        sales_df = sales_df.rename(columns={'item_id': 'sku_id',
                                            'date': 'transaction_date'})
        sales_df.sales_qty = sales_df.sales_qty.astype(float)
        sales_df.transaction_date = pd.to_datetime(sales_df.transaction_date)
        return sales_df

    def _get_sku_fields_from_sales(self) -> pd.DataFrame:
        sales_df = self._load_df(self._sales_file)
        sales_df = sales_df.rename(columns={'item_id': 'sku_id'})
        sku_cols = ['sku_id', 'cat_id', 'dept_id']
        return sales_df.groupby(sku_cols).size().reset_index()[sku_cols]

    def get_silver_datasets(self) -> (pa.typing.DataFrame[SalesSchema],
                                      pa.typing.DataFrame[CatalogSchema],
                                      pa.typing.DataFrame[CalendarSchema],
                                      ):
        return (
            self.get_sales_df(),
            self.get_catalog_df(),
            self.get_calendar_df()
        )

        # TODO: need to save silver and gold data with 3 identifiers (sales, catalog, calendar)
        # See if we have the merge dataset already
        # demand_data_path = os.path.join(self._get_path_relative_to_this_file('data'), 'demand.csv')
        # if os.path.exists(demand_data_path):
        #    return pd.read_csv(demand_data_path)
