from abc import abstractmethod
from typing import Tuple, TypeVar

import pandas as pd


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


class SplitStrategy:

    @abstractmethod
    def split(self, dataset: DatasetSubclasses) -> Tuple[DatasetSubclasses, DatasetSubclasses, DatasetSubclasses]:
        pass

