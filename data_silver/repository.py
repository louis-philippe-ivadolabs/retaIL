from abc import ABC, abstractmethod

from data_silver.domain import TransactionList


class StoreDetailsRepository(ABC):


    @abstractmethod
    def find_all(self):
        pass

    @abstractmethod
    def find_by_label(self, label: str):
        pass

    @abstractmethod
    def find_by_store_number(self, store_number: str):
        pass


class SkuDetailsRepository(ABC):

    @abstractmethod
    def find_all(self):
        pass

    @abstractmethod
    def find_by_category(self, label: str):
        pass

    @abstractmethod
    def find_by_sku_number(self, sku_number: str):
        pass


class PriceHistoryRepository(ABC):

    @abstractmethod
    def find_by_category(self, label: str):
        pass

    @abstractmethod
    def find_by_sku_and_store_number(self, sku: str, store_number: str):
        pass

    @abstractmethod
    def find_all(self):
        pass



class SkuHistoryRepository(ABC):

    @abstractmethod
    def find_by_category(self, label: str):
        pass

    @abstractmethod
    def find_by_sku(self, sku: str):
        pass

    @abstractmethod
    def find_all(self):
        pass


class TransactionRepository(ABC):

    @abstractmethod
    def find_by_category(self, label: str, start_date: str, end_date: str) -> TransactionList:
        pass
