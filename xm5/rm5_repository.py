from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import uuid

from pandas import DataFrame

import pandas as pd

from data_silver.domain import SkuDetails, PriceHistory, SkuHistory, StoreDetails, Transaction
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
        self.df = None
        self._preprocess()

    def _preprocess(self):
        sales_df = self.m5_data.sales_train_validation
        column_names = sales_df.columns
        key_columns = column_names[0:6]
        melted_df_1 = sales_df.melt(id_vars=key_columns, var_name='d',value_name='sales_qty')

        sales_df = self.m5_data.sales_train_evaluation
        column_names = sales_df.columns
        key_columns = column_names[0:6]
        melted_df_2 = sales_df.melt(id_vars=key_columns, var_name='d',value_name='sales_qty')

        df = pd.concat([melted_df_1,melted_df_2],ignore_index=True)
        calendar_df = self.m5_data.calendar_df[['date','d']]
        df = pd.merge(df,calendar_df,on='d')
        df = df.rename(columns={"item_id": "sku_number", "dept_id": "dept",'cat_id':'cat',
                                'store_id':'store_number','date':'transaction_date'})
        self.df = df[['sku_number','dept','cat','store_number','transaction_date','sales_qty']]

    def find_by_category(self, label_type : str, label: str, start_date: str, end_date: str) -> list[Transaction]:

        df = self.df
        df = df[(df[label_type] == label) & (df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)]
        return df.itertuples()

    def find_by_sku_store_and_date(self, sku_store_dates : list[str,str,str])->list[Transaction]:
        df = self.df
        records = []
        for sku_store_date in sku_store_dates:
            records.append({'sku_number' : sku_store_date[0],'store_number' : sku_store_date[1],'transaction_date' : sku_store_date[2]})
        scope_df = pd.DataFrame(records)
        df = pd.merge(df,scope_df)
        return df.itertuples()




