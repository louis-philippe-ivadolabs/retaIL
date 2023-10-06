from abc import abstractmethod
from typing import Tuple, TypeVar, List, Callable, Dict

import pandas as pd
import pandera as pa

class Dataset:

    def to_df(self) -> pd.DataFrame:
        pass


class EmptyDataset(Dataset):

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame()


class DemandDataset(Dataset):

    def __init__(self, data: pd.DataFrame):
        self._data = data

    def to_df(self) -> pd.DataFrame:
        return self._data


# Create a logical type that represents all sub classe of Dataset
DatasetSubclasses = TypeVar("DatasetSubclasses", bound=Dataset)


class Transformer:

    @abstractmethod
    def transform(self, dataset: DatasetSubclasses) -> DatasetSubclasses:
        pass


class Evaluator:

    @abstractmethod
    def score(self, transformed_sales_df: pd.DataFrame) -> Dict:
        pass


class SplitStrategy:

    @abstractmethod
    def split(self, dataset: DatasetSubclasses) -> Tuple[DatasetSubclasses, DatasetSubclasses, DatasetSubclasses]:
        pass


class Feature:
    def __init__(self,
                 name: str,
                 is_categorical: bool,
                 aggregation_f: Callable,
                 pa_check: pa.Check = None,
                 transformer_list: List[Transformer] = None):
        self.name = name
        self.is_categorical = is_categorical
        self.aggregation_f = aggregation_f
        self.pa_check = pa_check
        self.transformer_list = transformer_list
