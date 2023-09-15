from abc import ABC
from copy import copy
from dataclasses import replace

from catboost import CatBoostRegressor

from data_gold.domain import Scope, OfferSegmentPeriod
from data_gold.repository import ObservationRepository
from model.featurizer import Featurizer


class Forecaster(ABC):

    def train(self, training_scope: Scope):
        pass

    def predict(self, prediction_scope: Scope) -> dict[OfferSegmentPeriod, float]:
        pass


class LagForecaster(Forecaster):

    def __init__(self, lag: int, observation_repository: ObservationRepository):
        self.lag = lag
        self.observation_repository = observation_repository

    def train(self, training_scope: Scope):
        return None

    def predict(self, scope: Scope) -> dict[OfferSegmentPeriod, float]:
        result = {}
        adjusted_periods = []
        horizon = scope.segmentation_scheme.horizon
        for period in scope.periods:
            adjusted_periods.append(horizon.periods[period.index - self.lag])
        observations = self.observation_repository.find(replace(scope,periods=adjusted_periods))
        dict = {}
        for obs in observations:
            dict[obs.offer_segment_period] = obs
        for offer_segment_period in scope.offer_segment_periods:
            lag_period = horizon.periods[offer_segment_period.period.index - self.lag]
            obs = dict[replace(obs.offer_segment_period,period=lag_period)]
            if obs:
                result[offer_segment_period] = observations[0].total_sales_qty
            else:
                result[offer_segment_period] = None

        return result


class CatBoostForecaster(Forecaster):

    def __init__(self, featurizer: Featurizer, observation_repository: ObservationRepository):
        self.featurizer = featurizer
        self.observation_repository = observation_repository
        self.parameters_mapping = None
        self.empty_paraneters = None

    def train(self, training_scope: Scope):
        features_for_offer_segment_periods = self.featurizer.build_features(scope=training_scope)
        observations = self.observation_repository.find(training_scope)
        self.parameters_mapping = {}
        self.empty_paraneters = []
        for offer_segment_period, features in features_for_offer_segment_periods.items():
            for name, value in features.items():
                index = self.parameters_mapping.get(name)
                if index is None:
                    index = len(self.parameters_mapping)
                    categorical = False
                    if isinstance(value, str):
                        categorical = True
                    self.parameters_mapping[name] = (index, categorical)
                    if not categorical:
                        self.empty_paraneters.append(0)
                    else:
                        self.empty_paraneters.append('')

        feature_array_list = []
        for obs in observations:
            offer_segment_period = obs.offer_segment_period
            features = features_for_offer_segment_periods[offer_segment_period]
            features_array = copy(self.empty_paraneters)
            for name, value in features.items():
                if value is None:
                    print(name,offer_segment_period)
                features_array[self.parameters_mapping[name][0]] = value
            feature_array_list.append(features_array)

        cat_features = [v[0] for k,v in self.parameters_mapping.items() if v[1]]
        self.model = CatBoostRegressor(cat_features=cat_features)
        self.model.fit(feature_array_list,[obs.total_sales_qty for obs in observations])

    def predict(self, prediction_scope: Scope) -> dict[OfferSegmentPeriod, float]:
        features_for_offer_segment_periods = self.featurizer.build_features(scope=prediction_scope)
        feature_array_list = []
        for offer_segment_period in prediction_scope.offer_segment_periods:
            features = features_for_offer_segment_periods[offer_segment_period]
            features_array = copy(self.empty_paraneters)
            for name, value in features.items():
                features_array[self.parameters_mapping[name][0]] = value
            feature_array_list.append(features_array)

        predictions = self.model.predict(feature_array_list)
        result = {}
        for i, offer_segment_period in enumerate(prediction_scope.offer_segment_periods):
            result[offer_segment_period] = predictions[i]

        return result





