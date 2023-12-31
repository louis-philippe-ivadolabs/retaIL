import math
from abc import ABC
from dataclasses import replace
from numbers import Number
from typing import Any

import pandas as pd
from pandas import DataFrame

from data_gold.domain import Scope, \
    OfferSegmentPeriod
from data_gold.repository import ObservationRepository, PriceRepository, SkuStatusRepository


class Featurizer(ABC):

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        pass


class LagSalesFeaturizer(Featurizer):

    def __init__(self, lag_periods: int, observation_repository: ObservationRepository):
        self.observation_repository = observation_repository
        self.lag_periods = lag_periods
        self.prefix = "lag_sales"

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        adjusted_periods = []
        for period in scope.periods:
            adjusted_periods.append(scope.segmentation_scheme.horizon.periods[period.index-self.lag_periods])
        lag_scope = replace(scope,periods=adjusted_periods)
        observations = self.observation_repository.find(lag_scope)
        lag_by_offer_segment_period = {}
        for obs in observations:
            original_period = scope.segmentation_scheme.horizon.periods[obs.offer_segment_period.period.index+self.lag_periods]
            lag_by_offer_segment_period[replace(obs.offer_segment_period,period=original_period)] = obs.total_sales_qty
        result = {}
        for offer_segment_period in scope.offer_segment_periods:
            features = {}
            lag_value = lag_by_offer_segment_period.get(offer_segment_period)
            if lag_value is None:
                lag_value = -1
            features[self.prefix] = lag_value
            result[offer_segment_period] = features
        return result


class LagPriceFeaturizer(Featurizer):

    def __init__(self, lag_periods: int, price_repository: PriceRepository):
        self.price_repository = price_repository
        self.lag_periods = lag_periods
        self.prefix = "lag_price"

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        adjusted_periods = []
        for period in scope.periods:
            adjusted_periods.append(scope.segmentation_scheme.horizon.periods[period.index - self.lag_periods])
        lag_scope = replace(scope, periods=adjusted_periods)
        price_observations = self.price_repository.find(lag_scope)
        lag_by_offer_segment_period = {}
        for obs in price_observations:
            original_period = scope.segmentation_scheme.horizon.periods[
                obs.offer_segment_period.period.index + self.lag_periods]
            lag_by_offer_segment_period[replace(obs.offer_segment_period, period=original_period)] = sum(k[1]*v for k,v in obs.price_probabilities.items())
        result = {}
        for offer_segment_period in scope.offer_segment_periods:
            features = {}
            lag_value = lag_by_offer_segment_period.get(offer_segment_period)
            if lag_value is None:
                lag_value = -1
            features[self.prefix] = lag_value
            result[offer_segment_period] = features
        return result


class SkuStatusFeaturizer(Featurizer):
    def __init__(self, lag_periods: int, sku_status_repository: SkuStatusRepository):
        self.sku_status_repository = sku_status_repository
        self.lag_periods = lag_periods
        self.prefix = ""

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        adjusted_periods = []
        for period in scope.periods:
            adjusted_periods.append(scope.segmentation_scheme.horizon.periods[period.index + self.lag_periods])
        lag_scope = replace(scope, periods=adjusted_periods)
        sku_status_observations = self.sku_status_repository.find(lag_scope)
        lag_by_offer_segment_period = {}
        for obs in sku_status_observations:
            original_period = scope.segmentation_scheme.horizon.periods[
                obs.offer_segment_period.period.index + self.lag_periods]
            lag_by_offer_segment_period[replace(obs.offer_segment_period, period=original_period)] = obs.activity_probability
        result = {}
        for offer_segment_period in scope.offer_segment_periods:
            features = {}
            lag_value = lag_by_offer_segment_period.get(offer_segment_period)
            features[self.prefix] = lag_value
            result[offer_segment_period] = features


class LogFeaturizer(Featurizer):

    def __init__(self, featurizer: Featurizer):
        self.featurizer = featurizer
        self.min_value = 0.00001

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        features = self.featurizer.build_features(scope)
        result = {}
        for offer_segment_period, features_dict in features.items():
            transformed_features_dict = {}
            for feature_name, feature_value in features_dict.items():
                transformed_value = feature_value
                if isinstance(feature_value,Number):
                    transformed_value = math.log(max(self.min_value, feature_value))
                transformed_features_dict[feature_name] = transformed_value
            result[offer_segment_period] = transformed_features_dict
        return result


class Normalizer(Featurizer):

    def __init__(self, scope: Scope, featurizer: Featurizer, observation_repository: ObservationRepository):
        self.scope = scope
        self.featurizer = featurizer
        self.observation_repository = observation_repository

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        min_value_for_offer_segment = {}
        max_value_for_offer_segment = {}
        all_features_by_offer_segment_period = self.featurizer.build_features(scope)
        for offer_segment_period, features in all_features_by_offer_segment_period.items():
            min_value = min_value_for_offer_segment.get(offer_segment_period.offer_segment)
            if min_value is None:
                min_value = {}
                min_value_for_offer_segment[offer_segment_period.offer_segment] = min_value
            max_value = max_value_for_offer_segment.get(offer_segment_period.offer_segment)
            if max_value is None:
                max_value = {}
                max_value_for_offer_segment[offer_segment_period.offer_segment] = max_value

            for feature_name, feature_value in features.items():
                value = min_value.get(feature_name)
                if value is None or feature_value < value:
                    min_value[feature_name] = feature_value
                value = max_value.get(feature_value)
                if value is None or feature_value > value:
                    max_value[feature_name] = feature_value


        result = {}
        for offer_segment_period, features in all_features_by_offer_segment_period.items():
            transformed_features = {}
            for feature_name, feature_value in features.items():
                min_value = min_value_for_offer_segment[offer_segment_period.offer_segment][feature_name]
                max_value = max_value_for_offer_segment[offer_segment_period.offer_segment][feature_name]
                transformed_features[feature_name] = (feature_value - min_value)/(max_value-min_value)
            result[offer_segment_period,transformed_features]

        return result



class CompositeFeaturizer(Featurizer):

    def __init__(self, featurizers: list[Featurizer]):
        self.featurizers = featurizers

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        result = {}
        for featurizer in self.featurizers:
            features_by_offer_segment_period = featurizer.build_features(scope)
            for offer_segment_period, features in features_by_offer_segment_period.items():
                current = result.get(offer_segment_period)
                if current is None:
                    current = {}
                    result[offer_segment_period] = current
                current.update(features)

        return result

class PrecomputeFeaturizer(Featurizer):

    def __init__(self, precomputed_feature_df : DataFrame):
        self.precomputed_feature_df = precomputed_feature_df
        self.precomputed_map = {}
        feature_columns = [c for c in precomputed_feature_df.columns not in ['store_group_name','sku_group_name','start''end']]
        for record in self.precomputed_feature_df.to_records():
            features = {}
            for feature_colunn in feature_columns:
                features[feature_colunn] = record[feature_colunn]
            self.precomputed_map[record.store_group_name,record.sku_group_name,record.start,record.end] = features

    def build_features(self, scope: Scope) -> dict[OfferSegmentPeriod, dict[str, Any]]:
        result = {}
        for offer_segment_period in scope.offer_segment_periods:
            features = self.precomputed_map.get(offer_segment_period,{})
            result[offer_segment_period] = features

        return result


class FeatureBuilder:

    def __init__(self, observation_repository: ObservationRepository,
                 price_repository: PriceRepository, sku_status_repository: SkuStatusRepository):
        self.observation_repository = observation_repository
        self.price_repository = price_repository
        self.sku_status_repository = sku_status_repository

    def price_lag(self, lags_in_periods: int) -> Featurizer:
        return LagPriceFeaturizer(lag_periods=lags_in_periods, price_repository=self.price_repository,)

    def sales_lag(self, lag: int) -> Featurizer:
        return LagSalesFeaturizer(lag_periods=lag,observation_repository=self.observation_repository)

    def sku_status(self, lag : int) -> Featurizer:
        return SkuStatusFeaturizer(lag_periods=lag,sku_status_repository=self.sku_status_repository)

    def log(self, featurizer: Featurizer) -> Featurizer:
        return LogFeaturizer(featurizer=featurizer)

    def normalize(self, featurizer: Featurizer, scope: Scope):
        return Normalizer(scope=scope,featurizer=featurizer,observation_repository=self.observation_repository)

    def precomputed_features_from_csv_file(selfs, csv_file_name : str ):
        df = pd.read_csv(csv_file_name)
        return PrecomputeFeaturizer(precomputed_feature_df=df)

    def precomputed_features_from_df(selfs, df : DataFrame):
        return PrecomputeFeaturizer(precomputed_feature_df=df)

    def concat(self, featurizers: list[Featurizer]) -> Featurizer:
        return CompositeFeaturizer(featurizers=featurizers)
