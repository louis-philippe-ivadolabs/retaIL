from dataclasses import dataclass

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

