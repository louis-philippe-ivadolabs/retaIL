import logging
from typing import List

import pandas as pd
from pandas import DataFrame

from retail.domain import Feature
from retail.transformer.interfaces import FeatureSelector


class DemandFeatureSelector(FeatureSelector):
    def __init__(self,
                 feature_list: List[Feature],
                 logger: logging.Logger = None
                 ):
        self._feature_list = feature_list
        self._logger = logger or logging.getLogger(__name__)

    def fit_transform(self,
                      df: pd.DataFrame,
                      ) -> pd.DataFrame:
        return self.transform(df)

    def transform(self,
                  df: pd.DataFrame,
                  ) -> pd.DataFrame:
        original_n_features = df.shape[1]
        df = df[[f.name for f in self._feature_list]]
        self._logger.info(
            f"DemandFeatureSelector removed {original_n_features - df.shape[1]} features, keeping {df.shape[1]}")
        return df
