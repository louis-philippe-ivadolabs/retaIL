from abc import ABC
from copy import copy

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
        for offer_segment_period in scope.offer_segment_periods:
            horizon = scope.segmentation_scheme.horizon
            lag_period = horizon.periods[offer_segment_period.period.index - self.lag]
            scope = Scope(segmentatoin_scheme=scope.segmentation_scheme,
                          offer_segments=[offer_segment_period.offer_segment], period=lag_period)
            observations = self.observation_repository.find(scope)
            if observations:
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
        features_collection = self.featurizer.build_features(scope=training_scope)
        observations = self.observation_repository.find(training_scope)
        self.parameters_mapping = {}
        self.empty_paraneters = []
        for features in features_collection:
            for name, value in features.items():
                index = self.parameters_mapping.get(name)
                if index is None:
                    index = len(self.parameters_mapping)
                    categorical = False
                    if isinstance(name, str):
                        categorical = True
                    self.parameters_mapping[name] = (index, categorical)
                    if not categorical:
                        self.empty_paraneters.append(index, 0)
                    else:
                        self.empty_paraneters.append(index, "")

        feature_array_list = []
        for obs in observations:
            offer_segment_period = obs.offer_segment_period
            features = features_collection[offer_segment_period]
            features_array = copy(self.empty_paraneters)
            for name, value in features.items():
                features_array[self.parameters_mapping[name]] = value
            feature_array_list.append(features_array)

    def predict(self, scope: Scope) -> dict[OfferSegmentPeriod, float]:
        pass
