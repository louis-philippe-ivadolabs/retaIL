from bisect import bisect_right, bisect_left
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple

from pandas import DataFrame


@dataclass(frozen=True)
class SkuDetails:
    sku_number: str
    labels: dict[str, str]
    description: str


@dataclass(frozen=True)
class StoreDetails:
    store_number: str
    labels: dict[str, str]
    description: str


@dataclass
class PriceHistory:
    sku_number: str
    store_number: str

    price_changes : list[tuple[str,float]]

    def find_observed_prices(self, start_date : str, end_date)->list[float]:
        big_number = 99999999999.0
        start = bisect_right(self.price_changes, (start_date,-big_number))
        end = bisect_left(self.price_changes, (end_date,big_number))
        prices = []
        for i in range(start,end):
            prices.append(self.price_changes[i])
        return prices


@dataclass
class SkuHistory:
    sku_number: str
    active_date: str
    fully_discontinued_date: str
    other_events: list[tuple[str, datetime, str]]



@dataclass
class Transaction(NamedTuple):

    transaction_date : str
    store_number : str
    sku_number : str
    sales_qty: float

