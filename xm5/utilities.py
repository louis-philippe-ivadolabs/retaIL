from collections import defaultdict

from pandas import DataFrame

from data_gold.domain import SkuGroup, SkuSegmentation, StoreSegmentation, StoreGroup, Period, Horizon
from data_silver.domain import SkuDetails, StoreDetails



def build_sku_segmentation(sku_details: list[SkuDetails], category : str = None):
    sku_groups = {}
    sku_to_sku_group = {}
    for sku in sku_details:
        if category is None or category in sku.labels.values():
            sku_groups[sku.sku_number] = (SkuGroup(sku_group_name=sku.sku_number, skus=[sku.sku_number]))
            sku_to_sku_group[sku.sku_number] = sku.sku_number

    sku_group_segmentation = SkuSegmentation(sku_groups=sku_groups, sku_to_sku_group=sku_to_sku_group)
    return sku_group_segmentation


def build_store_segmentation(store_details: list[StoreDetails]):
    store_groups = {}
    store_to_store_group = {}
    for store in store_details:
        store_to_store_group[store.store_number] = store.store_number
        store_groups[store.store_number] = StoreGroup(store_group_name=store.store_number, stores=[store.store_number])
    return StoreSegmentation(store_groups=store_groups, store_to_store_group=store_to_store_group)


def build_week_horizon(calendar_df: DataFrame, start : str, end : str):
    date_by_weeks = defaultdict(list)
    for record in calendar_df.to_records():
        date_by_weeks[record.wm_yr_wk].append(record.date)

    periods = []
    date_to_period = {}
    period_to_dates = defaultdict(list)

    count = 0
    for k, v in date_by_weeks.items():
        period = Period(index=count,start=v[0], end=v[-1])
        periods.append(period)
        count += 1
        for d in v:
            date_to_period[d] = period
            period_to_dates[period].append(d)
    valid_periods = [p for p in periods if p.start >= start and p.end <= end]
    return Horizon(periods=valid_periods, date_to_period=date_to_period, period_to_dates=period_to_dates,
                   period_attributes={})


def split_horizon(periods : list[Period], end_of_train : str)->tuple[list[Period],list[Period]]:
    return [p for p in periods if p.end <= end_of_train], [p for p in periods if p.start > end_of_train]