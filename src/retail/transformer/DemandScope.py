import logging
from typing import List

import pandas as pd

from retail.domain import Feature
from retail.transformer.interfaces import Scope


class DemandScope(Scope):
    def __init__(self,
                 start_date: str = None,
                 end_date: str = None,
                 store_id_list: List[str] = None,
                 cat_id_list: List[str] = None,
                 sku_id_list: List[str] = None,
                 dept_id_list: List[str] = None,
                 logger: logging.Logger = None
                 ):
        self._start_date = start_date
        self._end_date = end_date
        self._store_id_list = store_id_list
        self._cat_id_list = cat_id_list
        self._sku_id_list = sku_id_list
        self._dept_id_list = dept_id_list
        self._logger = logger or logging.getLogger(__name__)

    def transform(self,
                  df: pd.DataFrame,
                  ) -> pd.DataFrame:
        original_n_rows = df.shape[0]
        if self._start_date:
            df = df[df["transaction_date"] >= self._start_date]
        if self._end_date:
            df = df[df["transaction_date"] < self._end_date]
        if self._store_id_list:
            df = df[df["store_id"].isin(self._store_id_list)]
        if self._cat_id_list:
            df = df[df["cat_id"].isin(self._cat_id_list)]
        if self._sku_id_list:
            df = df[df["sku_id"].isin(self._sku_id_list)]
        if self._dept_id_list:
            df = df[df["dept_id"].isin(self._dept_id_list)]
        self._logger.info(
            f"DemandScope removed {original_n_rows - df.shape[0]} Rows. Number of remaining rows: {df.shape[0]}")
        return df
