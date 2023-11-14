import logging
from typing import List

import pandas as pd

from retail.domain import Feature
from retail.transformer.interfaces import Aggregator


AggregationLevel = {
    "transaction_date": ['store_id', 'sku_id', 'transaction_date'],
    "year_week": ['store_id', 'sku_id', 'year_week'],
    "year": ['store_id', 'sku_id', 'year']
}


class DemandAggregator(Aggregator):
    def __init__(self,
                 aggregation_level: AggregationLevel,
                 feature_list: List[Feature],
                 logger: logging.Logger = None):
        self._aggregation_level = aggregation_level
        self._feature_list = feature_list
        self._logger = logger or logging.getLogger(__name__)

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        original_n_rows = df.shape[0]
        feature_aggregation_f_dict = dict(
            [(f.name, f.aggregation_f)
             for f in self._feature_list
             if f.name not in self._aggregation_level
             ]
        )
        df = df.groupby(self._aggregation_level).agg(feature_aggregation_f_dict).reset_index()
        self._logger.info(f"Aggregator aggregated {original_n_rows} rows into {df.shape[0]}")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df