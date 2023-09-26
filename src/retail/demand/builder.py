from typing import Tuple, List

from retail.demand.interfaces import DemandPredictor
from retail.domain import DemandDataset, Transformer, SplitStrategy
from retail.split.basic import NOPSplitStrategy


class DemandModelBuilder:

    def __init__(self, raw_demand_dataset: DemandDataset):
        self._raw_data = raw_demand_dataset
        self._pipeline = []
        self._split_strategy = NOPSplitStrategy()

    def with_transform_pipeline(self, pipeline: List[Transformer]) -> "DemandModelBuilder":
        self._pipeline.extend(pipeline)
        return self

    def with_split(self, strategy: SplitStrategy) -> "DemandModelBuilder":
        self._split_strategy = strategy
        return self

    def build(self) -> Tuple[DemandPredictor, DemandDataset, DemandDataset, DemandDataset]:
        # Go through the transformers
        dataset = self._raw_data
        for transformer in self._pipeline:
            dataset = transformer.transform(self._raw_data)

        # Split in test, train, validation
        train, test, validation = self._split_strategy.split(dataset)

        # train

        return DemandPredictor(), train, test, validation
