from typing import Tuple, List

from retail.demand.generic_demand_predictor import GenericDemandPredictor
from retail.demand.interfaces import DemandPredictor
from retail.domain import DemandDataset, Transformer, SplitStrategy, Feature
from retail.split.basic import NOPSplitStrategy


class DemandModelBuilder:

    def __init__(self):
        self._transformer_list = []
        self._split_strategy = NOPSplitStrategy()
        self._feature_list = []
        self._demand_predictor = None

    def with_transformer_list(self, transformer_list: List[Transformer]) -> "DemandModelBuilder":
        self._transformer_list.extend(transformer_list)
        return self

    def with_split(self, strategy: SplitStrategy) -> "DemandModelBuilder":
        self._split_strategy = strategy
        return self

    def with_demand_predictor(self, demand_predictor: DemandPredictor):
        self._demand_predictor = demand_predictor
        return self

    def with_feature_list(self, feature_list: List[Feature]):
        self._feature_list = feature_list
        return self

    def build(self) -> GenericDemandPredictor:
        return GenericDemandPredictor(
            self._feature_list,
            self._demand_predictor,
            self._transformer_list
        )
