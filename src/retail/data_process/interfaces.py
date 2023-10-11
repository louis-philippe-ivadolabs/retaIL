import pandera as pa

from retail.schema import SalesSchema, CatalogSchema, CalendarSchema, DemandModelInputSchema


class BronzeToSilver:
    def get_silver_datasets(self) -> (pa.typing.DataFrame[SalesSchema],
                                       pa.typing.DataFrame[CatalogSchema],
                                       pa.typing.DataFrame[CalendarSchema],
                                       ):
        pass


class SilverToGold:
    def get_gold_dataset(self,
                  sales_df: pa.typing.DataFrame[SalesSchema],
                  catalog_df: pa.typing.DataFrame[CatalogSchema],
                  calendar_df: pa.typing.DataFrame[CalendarSchema],
                  ) -> pa.typing.DataFrame[DemandModelInputSchema]:
        pass


# Find the right place for this class (involving both data and model specs)
class DemandModelDataLoader:
    def __init__(self,
                 bronze_to_silver: BronzeToSilver,
                 silver_to_gold: SilverToGold):
        self._bronze_to_silver = bronze_to_silver
        self._silver_to_gold = silver_to_gold

    def get_gold_dataset(self):
        return self._silver_to_gold.get_gold_dataset(
                    *self._bronze_to_silver.get_silver_datasets())
