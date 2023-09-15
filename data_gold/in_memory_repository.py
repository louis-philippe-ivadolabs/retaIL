from collections import defaultdict

from data_gold.domain import Observation, Scope, SkuGroup, OfferSegmentPeriod, PriceDistribution, StoreGroup
from data_gold.repository import ObservationRepository, PriceRepository, SkuStatusRepository
from data_silver.domain import SkuDetails, PriceHistory, SkuHistory
from data_silver.repository import TransactionRepository, PriceHistoryRepository, SkuHistoryRepository


class BasicObservationRepository(ObservationRepository):

    def __init__(self, transaction_repository: TransactionRepository, all_sku_details: list[SkuDetails]):
        self.transaction_repository = transaction_repository
        self.sku_details_by_sku = {}
        for sku_details in all_sku_details:
            self.sku_details_by_sku[sku_details.sku_number] = sku_details

    def find(self, scope: Scope) -> list[Observation]:
        labels_by_type = defaultdict(set)
        for offer_segment in scope.offer_segments:
            sku_group: SkuGroup = scope.segmentation_scheme.sku_segmentation.sku_groups[offer_segment.sku_group_name]
            for sku in sku_group.skus:
                sku_details = self.sku_details_by_sku[sku]
                for label_type, label_value in sku_details.labels.items():
                    labels_by_type[label_type].add(label_value)

        best_label_type = min(labels_by_type, key=lambda k: len(labels_by_type[k]))

        bookings_by_sku_and_period = defaultdict(float)
        for label in labels_by_type[best_label_type]:
            transaction_list = self.transaction_repository.find_by_category(best_label_type, label, scope.periods[0].start,
                                                                        scope.periods[-1].end)
            for transaction in transaction_list.df.itertuples():
                period = scope.segmentation_scheme.horizon.date_to_period[transaction.transaction_date]
                bookings_by_sku_and_period[transaction.sku_number, period] += transaction.sales_qty

        bookings_by_offer_segment_period = defaultdict(float)
        for offer_segment in scope.offer_segments:
            for period in scope.periods:
                offer_segment_period = OfferSegmentPeriod(sku_group_name=offer_segment.sku_group_name,
                                                          store_group_name=offer_segment.store_group_name,
                                                          period=period)
                sku_group: SkuGroup = scope.segmentation_scheme.sku_segmentation.sku_groups[
                    offer_segment.sku_group_name]
                for sku in sku_group.skus:
                    bookings_by_offer_segment_period[offer_segment_period] += bookings_by_sku_and_period[sku, period]

        result = []
        for k, v in bookings_by_offer_segment_period.items():
            result.append(Observation(offer_segment_period=k, total_sales_qty=v))

        return result

class BasicPriceRepository(PriceRepository):

    def __init__(self, price_history_repository: PriceHistoryRepository, all_sku_details: list[SkuDetails]):
        self.price_history_repository = price_history_repository
        self.sku_details_by_sku = {}
        for sku_details in all_sku_details:
            self.sku_details_by_sku[sku_details.sku_number] = sku_details

    def find(self, scope: Scope) -> list[PriceDistribution]:

        result = []
        for offer_segment in scope.offer_segments:
            sku_group: SkuGroup = scope.segmentation_scheme.sku_segmentation.sku_groups[offer_segment.sku_group_name]
            store_group : StoreGroup = scope.segmentation_scheme.store_segmentation.store_groups[offer_segment.store_group_name]


        occurrences_by_price = defaultdict(int)
        for sku in sku_group.skus:
            for store in store_group.stores:
                price_history = self.price_history_repository.find_by_sku_and_store_number(sku, store)
                for period in scope.periods:
                    prices = price_history.find_observed_prices(period.start, period.end)
                    for price in prices:
                        occurrences_by_price[price] += 1
                    total = sum(v for v in occurrences_by_price.values())
                    probabilities_by_price = {k: v / total for k, v in occurrences_by_price.items()}
                    price_distribution = PriceDistribution(
                        OfferSegmentPeriod(offer_segment.store_group_name, offer_segment.sku_group_name, period),
                        probabilities_by_price)
                    result.append(price_distribution)

        return result

class BasicSkuStatusRepository(SkuStatusRepository):

    def __init__(self, sku_history_repository: SkuHistoryRepository, all_sku_details: list[SkuDetails]):
        self.sku_history_repository = sku_history_repository
        self.sku_details_by_sku = {}
        for sku_details in all_sku_details:
            self.sku_details_by_sku[sku_details.sku_number] = sku_details

    def find(self, scope: Scope) -> list[PriceDistribution]:

        result = []
        for offer_segment in scope.offer_segments:
            sku_group: SkuGroup = scope.segmentation_scheme.sku_segmentation.sku_groups[offer_segment.sku_group_name]
            count = 0
            for sku in sku_group.skus:
                sku_status_history: SkuHistory = self.sku_history_repository.find_by_sku(sku)
                for period in scope.periods:
                    if sku_status_history.active_date <= period.start and sku_status_history.fully_discontinued_date > period.start:
                        count += 1

                    probability = count/len(sku_group.skus)
                    price_distribution = PriceDistribution(
                        OfferSegmentPeriod(offer_segment.store_group_name, offer_segment.sku_group_name, period),
                        probability)
                    result.append(price_distribution)

        return result
