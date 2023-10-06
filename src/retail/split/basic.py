from typing import Tuple, TypeVar

from retail.domain import SplitStrategy, EmptyDataset, DatasetSubclasses


class NOPSplitStrategy(SplitStrategy):

    def split(self, dataset: DatasetSubclasses) -> Tuple[DatasetSubclasses, DatasetSubclasses, DatasetSubclasses]:
        return dataset, EmptyDataset(), EmptyDataset()


class ScikitLearnBasedSplit(SplitStrategy):

    def split(self, dataset: DatasetSubclasses) -> Tuple[DatasetSubclasses, DatasetSubclasses, DatasetSubclasses]:
        # TODO implement me!
        pass