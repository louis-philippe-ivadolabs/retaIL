from abc import abstractmethod
from typing import List

import pandas as pd

from retail.domain import DemandDataset, Feature


class DemandPredictor:

    @abstractmethod
    def transform(self, demand_df: pd.DataFrame):
        pass

    @abstractmethod
    def fit(self, demand_df: pd.DataFrame):
        pass

    @abstractmethod
    def predict(self, demand_df: pd.DataFrame):
        pass

