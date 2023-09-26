from retail.domain import DemandDataset


class XM5DataLoader:

    @staticmethod
    def load_demand_dataset() -> DemandDataset:
            def get_sales_df(self, sales_file: str) -> pd.DataFrame:
                self._load_raw_sales(
                    sales_file
                )._add_calendar(
                )._add_prices(
                )._conform(
                )
                return self.sales_df

            def _load_raw_sales(self, sales_file: str):
                key_columns = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
                self.sales_df = pd.read_csv(self.root_dir / sales_file)
                self.sales_df = self.sales_df.melt(id_vars=key_columns, var_name='d', value_name='sales_qty')
                return self

            def _add_calendar(self):
                self.calendar_df = pd.read_csv(self.root_dir / "calendar.csv")
                calendar_df = self.calendar_df[['date', 'd', 'wm_yr_wk']]
                self.sales_df = pd.merge(self.sales_df, self.calendar_df, on='d')
                return self

            def _add_prices(self):
                self.prices_df = pd.read_csv(self.root_dir / "sell_prices.csv")
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

        return