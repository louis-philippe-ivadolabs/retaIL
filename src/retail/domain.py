from abc import abstractmethod
from typing import Tuple, TypeVar


class Dataset:

    def to_df() -> pd.DataFrame:
        pass


class EmptyDataset(Dataset):
    pass


class DemandDataset(Dataset):
    pass


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

