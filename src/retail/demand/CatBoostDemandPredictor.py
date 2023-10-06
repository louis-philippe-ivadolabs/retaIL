from typing import List

import pandas as pd
from catboost import CatBoostRegressor

from retail.demand.interfaces import DemandPredictor
from retail.domain import Feature


class CatBoostDemandPredictor(DemandPredictor):
    def fit(self,
            train_df: pd.DataFrame,
            target: pd.Series,
            feature_list: List[Feature]
           ):
        cat_features = [f.name for f in feature_list if f.is_categorical]
        self._model = CatBoostRegressor(cat_features=cat_features)
        self._model.fit(train_df, target)

    def predict(self, prediction_df: pd.DataFrame):
        return self._model.predict(prediction_df)
