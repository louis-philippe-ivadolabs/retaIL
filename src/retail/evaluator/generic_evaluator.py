import numpy as np
import pandas as pd

from retail.demand.interfaces import DemandPredictor
from retail.domain import Evaluator


class GenericEvaluator(Evaluator):
    def __init__(self, model: DemandPredictor):
        self._model = model

    def mae(self,
            demand_predictions: pd.Series,
            demand_actuals: pd.Series):
        return np.mean(np.abs(demand_predictions - demand_actuals))

    def __call__(self, preprocessed_prediction_df: pd.DataFrame):
        demand_predictions = self._model.predict(preprocessed_prediction_df.drop('sales_qty', axis=1))
        demand_actuals = preprocessed_prediction_df['sales_qty']
        return {
                 "mae": self.mae(demand_predictions, demand_actuals)
               }