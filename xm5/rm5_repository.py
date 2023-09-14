from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import uuid

from pandas import DataFrame

import pandas as pd

from data_silver.domain import SkuDetails, Transaction, PriceHistory, SkuHistory, StoreDetails
from data_silver.repository import SkuDetailsRepository, PriceHistoryRepository, TransactionRepository, \
    SkuHistoryRepository, StoreDetailsRepository


@dataclass
class M5Data:

    calendar_df : DataFrame
    sales_train_validation : DataFrame
    sales_train_evaluation : DataFrame
    sell_prices : DataFrame

    @staticmethod
    def load_from_files(directory : str):
        root = Path(directory)

        calendar_df = pd.read_csv(root / 'calendar.csv')
        sales_train_evaluation = pd.read_csv(root / 'sales_train_evaluation.csv')
        sales_train_validation = pd.read_csv(root / 'sales_train_validation.csv')
        sell_prices = pd.read_csv(root / 'sell_prices.csv')
        return M5Data(calendar_df=calendar_df,sales_train_validation=sales_train_validation,sales_train_evaluation=sales_train_evaluation,sell_prices=sell_prices)



class M5SkuDetailsRepository(SkuDetailsRepository):

    def __init__(self, m5_data : M5Data):
        self.m5_data = m5_data
        self.sku_details_by_category = defaultdict(list)
        self.sku_details_by_sku = {}
        self._preload(self.m5_data.sales_train_evaluation)
        self._preload(self.m5_data.sales_train_validation)

    def _preload(self, sales_df : DataFrame):
        df: DataFrame = sales_df[['item_id', 'dept_id', 'cat_id']]
        df = df.drop_duplicates()
        for record in df.to_records():
            item_id = record['item_id']
            dept_id = record['dept_id']
            cat_id = record['cat_id']
            labels = {}
            labels['dept'] = dept_id
            labels['cat'] = cat_id
            sku_details = SkuDetails(sku_number=item_id, labels=labels, description=item_id)
            self.sku_details_by_category[cat_id].append(sku_details)
            self.sku_details_by_category[dept_id].append(sku_details)
            self.sku_details_by_sku[item_id] = sku_details


    def find_all(self):
        return list(self.sku_details_by_sku.values())


    def find_by_category(self, label: str):
        res = self.sku_details_by_category.get(label,[])
        return res

    def find_by_sku_number(self, sku_number: str):
        return self.sku_details_by_sku.get(sku_number)


class M5SkuHistoryRepository(SkuHistoryRepository):

    def __init__(self,all_sku_details : list[SkuDetails]):
        self.sku_history_by_category = defaultdict(list)
        self.sku_details_by_sku = defaultdict(list)
        for sku_details in all_sku_details:
            sku_history = SkuHistory(sku_number=sku_details.sku_number,active_date='2001-01-01',fully_discontinued_date='2050-01-01',other_events=[])
            self.sku_details_by_sku[sku_details.sku_number] = sku_history
            for l in sku_details.labels.values():
                self.sku_history_by_category[l].append(sku_history)

    def find_by_category(self, label: str):
        return self.sku_history_by_category[label]

    def find_by_sku(self, sku: str):
        return self.sku_details_by_sku[sku]

    def find_all(self):
        return list(self.sku_details_by_sku.values())


class M5PriceHistoryRepository(PriceHistoryRepository):

    def __init__(self, m5_data : M5Data, all_sku_details : list[SkuDetails]):
        self.m5_data = m5_data
        self.price_history_by_sku_and_store = {}
        self.price_history_by_category = defaultdict(list)
        self.sku_details_by_sku = {}
        for sku_details in all_sku_details:
            self.sku_details_by_sku[sku_details.sku_number] = sku_details
        self._preload()


    def _preload(self):
        mapping_date = {}
        for record in self.m5_data.calendar_df.itertuples():
            if record.weekday == 'Saturday':
                mapping_date[record.wm_yr_wk] = record.date
        price_change_by_sku_and_store = defaultdict(list)
        for r in self.m5_data.sell_prices.itertuples():
            price_change_date = mapping_date[r.wm_yr_wk]
            price_change_by_sku_and_store[r.item_id,r.store_id].append((price_change_date,r.sell_price))
        for k,v in price_change_by_sku_and_store.items():
            price_history = PriceHistory(sku_number=k[0],store_number=k[1],price_changes=v)
            self.price_history_by_sku_and_store[price_history.sku_number,price_history.store_number] = price_history
            sku_details = self.sku_details_by_sku[r.item_id]
            for l in sku_details.labels.values():
                self.price_history_by_category[l].append(price_history)


    def find_by_category(self, label: str):
        return self.price_history_by_category[label]

    def find_by_sku_and_store_number(self, sku: str, store_number : str):
        return self.price_history_by_sku_and_store.get((sku, store_number))

    def find_all(self):
        return list(self.sku_details_by_sku.values())

class M5StoreDetailsRepository(StoreDetailsRepository):

    def __init__(self, m5_data : M5Data):
        self.m5_data = m5_data
        self.store_details_by_category = defaultdict(list)
        self.store_details_by_number = {}
        self._preload(self.m5_data.sales_train_evaluation)
        self._preload(self.m5_data.sales_train_validation)

    def _preload(self, sales_df : DataFrame):
        df: DataFrame = sales_df[['store_id','state_id']]
        df = df.drop_duplicates()
        for record in df.to_records():
            store_id = record.store_id
            state_id = record.state_id
            labels = {}
            labels['state'] = state_id
            store_details = StoreDetails(store_number=store_id,labels=labels,description=store_id)
            self.store_details_by_category[state_id].append(store_details)
            self.store_details_by_number[store_id] = store_details

    def find_all(self):
        return list(self.store_details_by_number.values())

    def find_by_label(self, label: str):
        return self.store_details_by_category[label]

    def find_by_store_number(self, store_number: str):
        return self.store_details_by_number.get(store_number)

class M5TransactionRepository(TransactionRepository):

    def __init__(self, m5_data : M5Data):
        self.m5_data = m5_data
        self.transactions_by_category = defaultdict(list)
        calendar_df = self.m5_data.calendar_df[['d', 'date']]
        self.mapping_date = {}
        for record in calendar_df.to_records():
            self.mapping_date[record.d] = record.date

    def _convert(self, sales_df : DataFrame):
        column_names = sales_df.columns
        key_columns =  column_names[0:6]
        melted_df = sales_df.melt(id_vars=key_columns,var_name='d')
        result = []
        for record in melted_df.itertuples():

            transaction_id = uuid.uuid1()
            transaction_datetime = self.mapping_date[record.d]
            sku = record.item_id
            store_number = record.store_id
            sales_qty = record.value
            if sales_qty == 0.0:
                continue
            sales_price = None
            customer_loyalty_number = None

            transaction = Transaction(transaction_id=transaction_id,sku_number=sku,transaction_date=transaction_datetime,store_number=store_number,sales_qty=sales_qty,sales_price=sales_price,customer_loyalty_number=customer_loyalty_number)
            result.append(transaction)

        return result


    def find_by_category(self, label: str, start_date: str, end_date: str) -> list[Transaction]:
        df = self.m5_data.sales_train_validation
        df = df[df.cat_id == label]
        transactions = self._convert(df)

        df = self.m5_data.sales_train_evaluation
        df = df[df.cat_id == label]
        transactions_eval = self._convert(df)
        all = transactions + transactions_eval
        return [t for t in all if t.transaction_date >= start_date and t.transaction_date <= end_date ]

