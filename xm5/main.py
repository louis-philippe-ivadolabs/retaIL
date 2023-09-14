# This is a sample Python script.
import time
from functools import partial

from data_gold.domain import Scope, SegmentationScheme, OfferSegment
from data_gold.in_memory_repository import BasicObservationRepository, BasicPriceRepository, BasicSkuStatusRepository
from model.featurizer import FeatureBuilder
from model.forecaster import CatBoostForecaster, LagForecaster
from model.validator import ForecasterValidator
from xm5.rm5_repository import M5Data, M5SkuDetailsRepository, M5TransactionRepository, M5SkuHistoryRepository, \
    M5PriceHistoryRepository, M5StoreDetailsRepository
from xm5.utilities import build_sku_segmentation, build_store_segmentation, build_week_horizon, split_horizon

if __name__ == '__main__':

    # Initialize repositories
    m5Data = M5Data.load_from_files("/Users/louis-philippebigras/Documents/m5-forecasting-accuracy")
    m5_sku_details_repository = M5SkuDetailsRepository(m5Data)
    sku_details = m5_sku_details_repository.find_all()

    m5_transaction_repository = M5TransactionRepository(m5Data)
    m5_sku_history_repository = M5SkuHistoryRepository(all_sku_details=sku_details)
    m5_price_history_repository = M5PriceHistoryRepository(m5_data=m5Data, all_sku_details=sku_details)
    m5_store_details_repository = M5StoreDetailsRepository(m5_data=m5Data)

    observation_repository = BasicObservationRepository(transaction_repository=m5_transaction_repository,
                                                        all_sku_details=sku_details)
    price_repository = BasicPriceRepository(price_history_repository=m5_price_history_repository,all_sku_details=sku_details)
    sku_status_repository = BasicSkuStatusRepository(sku_history_repository=m5_sku_history_repository, all_sku_details=sku_details)

    #Create a weekly horizon (this could be also a daily horizon)
    build_wk_horizon = partial(build_week_horizon,calendar_df=m5Data.calendar_df)

    # Create segmentation scheme (just for the category HOBBIES, to limit the quanitty of data)
    sku_details = m5_sku_details_repository.find_all()
    valid_categories = set()
    for sku in sku_details:
        valid_categories.add(sku.labels['cat'])
    sku_segmentation = build_sku_segmentation(sku_details,category='HOBBIES')
    store_details = m5_store_details_repository.find_all()
    store_segmentation = build_store_segmentation(store_details)
    horizon = build_wk_horizon(start='2011-01-29',end='2016-06-19')
    segmentation_scheme = SegmentationScheme(sku_segmentation=sku_segmentation, store_segmentation=store_segmentation,
                                             horizon=horizon)

    offer_segments = segmentation_scheme.offer_segments

    # Train models
    training_periods, test_periods = split_horizon(segmentation_scheme.horizon.periods,'2014-01-01')
    waste_periods, training_periods = split_horizon(training_periods,'2012-01-01')
    training_scope = Scope(segmentation_scheme=segmentation_scheme,
                           offer_segments=segmentation_scheme.offer_segments,
                           periods=training_periods)

    observations = observation_repository.find(training_scope)

    fb = FeatureBuilder(observation_repository, price_repository, sku_status_repository)

    price_lag_1 = fb.price_lag(1)
    sales_lag_1 = fb.sales_lag(1)
    sales_lag_2 = fb.sales_lag(2)
    log_price_lag_1 = fb.log(price_lag_1)
 #   normalized_log_price_lag_1 = fb.normalize(log_price_lag_1, training_scope)
    composite_features = fb.concat([price_lag_1, sales_lag_1, sales_lag_2])

    cat_boost_forecaster = CatBoostForecaster(featurizer=composite_features,
                                              observation_repository=observation_repository)
    cat_boost_forecaster.train(training_scope)

    lag_forecaster = LagForecaster(lag=52, observation_repository=observation_repository)
    lag_forecaster.train(training_scope)

    # Validate models
    validation_scope = Scope(segmentation_scheme, offer_segments=offer_segments, periods=test_periods)

    validator = ForecasterValidator()

    predictions = cat_boost_forecaster.predict(validation_scope)
    metrics = validator.validate(cat_boost_forecaster, validation_scope)

    predictions = lag_forecaster.predict(validation_scope)
    metrics = validator.validate(lag_forecaster, validation_scope)
