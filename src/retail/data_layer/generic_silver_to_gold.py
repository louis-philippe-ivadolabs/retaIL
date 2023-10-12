import pandas as pd
import pandera as pa
from retail.data_layer.interfaces import SilverToGold
from retail.schema import SalesSchema, CatalogSchema, CalendarSchema, DemandModelInputSchema


class GenericSilverToGold(SilverToGold):
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
        return model_df
