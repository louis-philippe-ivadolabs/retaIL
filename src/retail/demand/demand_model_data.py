from dataclasses import dataclass
import pandera as pa

from retail.data_layer.interfaces import BronzeToSilver, SilverToGold
from retail.schema import SalesSchema, CatalogSchema, CalendarSchema, DemandModelInputSchema


@dataclass
class DemandModelData:
    sales_df: pa.typing.DataFrame[SalesSchema]
    catalog_df: pa.typing.DataFrame[CatalogSchema]
    calendar_df: pa.typing.DataFrame[CalendarSchema]
    gold_df: pa.typing.DataFrame[DemandModelInputSchema]


class DemandModelDataLoader:
    def __init__(self,
                 bronze_to_silver: BronzeToSilver,
                 silver_to_gold: SilverToGold):
        self._bronze_to_silver = bronze_to_silver
        self._silver_to_gold = silver_to_gold

    def get_demand_model_data(self):
        sales_df, catalog_df, calendar_df = self._bronze_to_silver.get_silver_datasets()
        gold_df = self._silver_to_gold.get_gold_dataset(sales_df, catalog_df, calendar_df)
        return DemandModelData(sales_df=sales_df, catalog_df=catalog_df, calendar_df=calendar_df, gold_df=gold_df)
