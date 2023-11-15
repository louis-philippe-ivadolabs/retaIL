import logging
from typing import List

import pandas as pd

from retail.demand.interfaces import DemandPredictor
from retail.domain import Feature, Transformer, TransformerFeatureSubset


class GenericDemandPredictor(DemandPredictor):
    def __init__(self,
                 feature_list: List[Feature],
                 demand_predictor: DemandPredictor,
                 transformer_list: List[Transformer],
                 target_transformer_list: List[Transformer],
                 logger: logging.Logger = None
                 ):
        self._feature_list = feature_list
        self._demand_predictor = demand_predictor
        self._transformer_list = transformer_list
        self._target_transformer_list = target_transformer_list
        self._logger = logger or logging.getLogger(__name__)

    def fit_transform(self, sales_df: pd.DataFrame):
        self._logger.info(f"Preprocessing the data (fit_transform, for training). Data shape: {sales_df.shape}")
        for transformer in self._transformer_list:
            if isinstance(transformer, TransformerFeatureSubset):
                sales_df.loc[:, transformer.feature_subset] = transformer.transformer.fit_transform(
                    sales_df[transformer.feature_subset])
            else:
                sales_df = transformer.fit_transform(sales_df)
        for transformer in self._target_transformer_list:
            sales_df.loc[:, "sales_qty"] = transformer.fit_transform(sales_df["sales_qty"])

        self._logger.info(f"Data shape after preprocessing: {sales_df.shape}")
        return sales_df

    def transform(self, sales_df: pd.DataFrame):
        self._logger.info(f"Preprocessing the data (transform, for predicting). Data shape: {sales_df.shape}")
        for transformer in self._transformer_list:
            if isinstance(transformer, TransformerFeatureSubset):
                sales_df.loc[:, transformer.feature_subset] = transformer.transformer.transform(
                    sales_df[transformer.feature_subset])
            else:
                sales_df = transformer.transform(sales_df)
        self._logger.info(f"Data shape after preprocessing: {sales_df.shape}")
        return sales_df

    def fit(self, transformed_sales_df: pd.DataFrame):
        self._demand_predictor.fit(transformed_sales_df.drop("sales_qty", axis=1),
                                   transformed_sales_df["sales_qty"],
                                   self._feature_list)

    def predict(self, pred_df: pd.DataFrame):
        pred = self._demand_predictor.predict(pred_df)
        for transformer in reversed(self._target_transformer_list):
            pred = transformer.inverse_transform(pred)
        return pred

