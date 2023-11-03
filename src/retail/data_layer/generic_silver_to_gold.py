import pandas as pd
import pandera as pa
from retail.data_layer.interfaces import SilverToGold
from retail.data_layer.schema import SalesSchema, CatalogSchema, CalendarSchema, DemandModelInputSchema


class GenericSilverToGold(SilverToGold):
    @staticmethod
    def _impute_no_sales_skus(
                              gold_df: pa.typing.DataFrame[DemandModelInputSchema],
                              calendar_df: pa.typing.DataFrame[CalendarSchema],
                             ):
        yw_date_df = calendar_df.groupby(["year_week", "date"]).size().reset_index()[["year_week", "date"]]
        df = pd.merge(gold_df, yw_date_df, on="year_week", how="right")
        # Find dates that are missing from the sales data but present in the catalog
        df = df[df.date_x != df.date_y]
        df['sales_qty'] = 0
        return pd.concat((gold_df, df))

    def get_gold_dataset(self,
                         sales_df: pa.typing.DataFrame[SalesSchema],
                         catalog_df: pa.typing.DataFrame[CatalogSchema],
                         calendar_df: pa.typing.DataFrame[CalendarSchema],
                         ) -> pa.typing.DataFrame[DemandModelInputSchema]:

        merge_cols = (calendar_df.columns.difference(sales_df.columns))
        merge_on = ['day_no']
        model_df = pd.merge(sales_df, calendar_df[merge_cols.tolist() + merge_on], on=merge_on)
        merge_cols = (catalog_df.columns.difference(model_df.columns))
        merge_on = ['store_id', 'sku_id', 'year_week']
        model_df = pd.merge(model_df, catalog_df[merge_cols.tolist() + merge_on], on=merge_on)
        model_df = self._impute_no_sales_skus(model_df, calendar_df)
        return model_df
