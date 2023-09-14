from abc import ABC, abstractmethod

from data_gold.domain import Observation, Scope, PriceDistribution, SkuStatusDistribution


class ObservationRepository(ABC):

    @abstractmethod
    def find(self, scope: Scope) -> list[Observation]:
        pass


class PriceRepository(ABC):

    @abstractmethod
    def find(self, scope: Scope) -> list[PriceDistribution]:
        pass


class SkuStatusRepository(ABC):

    @abstractmethod
    def find(self, scope: Scope) -> list[SkuStatusDistribution]:
        pass
