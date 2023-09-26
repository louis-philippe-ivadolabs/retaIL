from abc import abstractmethod

from retail.domain import DemandDataset


class DemandPredictor:

    @abstractmethod
    def fit(self, demand_dataset: DemandDataset):
        pass

    @abstractmethod
    def predict(self, demand_dataset: DemandDataset):
        pass

