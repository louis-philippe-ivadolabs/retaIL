import logging
from typing import List

import pandas as pd

from retail.transformer.interfaces import Featurizer


class LaggedFeature(Featurizer):
    def __init__(self,
                 feature_name: str,
                 transformed_feature_name: str,
                 lag: int,
                 logger: logging.Logger = None):
        self._feature_name = feature_name
        self._transformed_feature_name = transformed_feature_name
        self._lag = lag
        self._logger = logger or logging.getLogger(__name__)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self._transformed_feature_name] = df[self._feature_name].shift(self._lag) #FIXME: not so fast :-)
        return df
