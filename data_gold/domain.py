from dataclasses import dataclass


@dataclass(frozen=True)
class Period:
    index: int
    start: str #yyyy-MM-dd
    end: str #yyyy-MM-dd


@dataclass(frozen=True)
class Horizon:
    periods: list[Period]
    date_to_period: dict[str, Period]
    period_to_dates: dict[Period, list[str]]
    period_attributes: dict[Period, dict[str, str]]


@dataclass(frozen=True)
class SkuGroup:
    sku_group_name: str
    skus: list[str]


@dataclass(frozen=True)
class SkuSegmentation:
    sku_groups: dict[str, SkuGroup]
    sku_to_sku_group: dict[str, str]


@dataclass(frozen=True)
class StoreGroup:
    store_group_name: str
    stores: list[str]


@dataclass(frozen=True)
class StoreSegmentation:
    store_groups: dict[str, StoreGroup]
    store_to_store_group: dict[str, str]


@dataclass(frozen=True)
class SegmentationScheme:
    store_segmentation: StoreSegmentation
    sku_segmentation: SkuSegmentation
    horizon: Horizon

    @property
    def offer_segments(self):
        result = []
        for store_group_name in self.store_segmentation.store_groups:
            for sku_group_name in self.sku_segmentation.sku_groups:
                result.append(OfferSegment(store_group_name, sku_group_name))
        return result



@dataclass(frozen=True)
class OfferSegment:
    store_group_name: str
    sku_group_name: str


@dataclass(frozen=True)
class OfferSegmentPeriod:
    store_group_name: str
    sku_group_name: str
    period: Period

    @property
    def offer_segment(self):
        return OfferSegment(store_group_name=self.store_group_name,sku_group_name=self.sku_group_name)


@dataclass(frozen=True)
class Observation:
    offer_segment_period: OfferSegmentPeriod
    total_sales_qty: float

@dataclass(frozen=True)
class PriceDistribution:
    offer_segment_period: OfferSegmentPeriod
    price_probabilities: dict[float, float]


@dataclass(frozen=True)
class SkuStatusDistribution:
    offer_segment_period: OfferSegmentPeriod
    activity_probability : float


@dataclass(frozen=True)
class Scope:
    segmentation_scheme: SegmentationScheme
    offer_segments: list[OfferSegment]
    periods: list[Period]

    @property
    def offer_segment_periods(self):
        result = []
        for offer_segment in self.offer_segments:
            for period in self.periods:
                result.append(OfferSegmentPeriod(offer_segment.store_group_name,offer_segment.sku_group_name, period))
        return result
