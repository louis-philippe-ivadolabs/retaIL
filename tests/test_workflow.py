from unittest import TestCase

from retail.data.m5.loader import M5DataLoader
from retail.data_layer.generic_silver_to_gold import GenericSilverToGold
from retail.demand.demand_model_data import DemandModelDataLoader
from retail.demand.cat_boost_demand_predictor import CatBoostDemandPredictor
from retail.demand.builder import DemandModelBuilder
from retail.evaluator.generic_evaluator import GenericEvaluator
from retail.transformer.demand_aggregator import AggregationLevel, DemandAggregator
from retail.transformer.demand_feature_selector import DemandFeatureSelector
from retail.transformer.demand_scope import DemandScope
from retail.domain import Feature, TransformerFeatureSubset
from retail.transformer.lagged_feature import LaggedFeature

from sklearn.preprocessing import MinMaxScaler, FunctionTransformer
import numpy as np

class WorkflowTestCase(TestCase):

    def test_simple_workflow(self):


        feature_list = [
            Feature('price', False, 'mean'),
            Feature('sales_qty', False, 'sum'),
            Feature('store_id', True, 'first'),
            Feature('year_week', False, 'first'),
            Feature('sku_id', True, 'first'),
            Feature('price_lag_1',
                    False,
                    aggregation_f='mean',
                    ),
            Feature('price_lag_2',
                    False,
                    aggregation_f='mean',
                    ),
        ]

        demand_predictor = CatBoostDemandPredictor()

        demand_scope = DemandScope(
            start_date='2012-01-01',
            end_date='2014-01-01',
            cat_id_list=["HOBBIES"]
        )

        lagged_features = [LaggedFeature('price',
                                         'price_lag_1',
                                         lag=1),
                           LaggedFeature('price',
                                         'price_lag_2',
                                         lag=2)
                           ]

        feature_selector = DemandFeatureSelector(feature_list)

        aggregator = DemandAggregator(AggregationLevel["year_week"], feature_list)

        log_transformer = FunctionTransformer(np.log1p, np.expm1)

        model = DemandModelBuilder(
        ).with_demand_predictor(
            demand_predictor
        ).with_feature_list(
            feature_list
        ).with_transformer_list(
            [
                demand_scope,
                *lagged_features,
                aggregator,
                feature_selector,
                TransformerFeatureSubset(MinMaxScaler(clip=True), ["sales_qty", "price"]),
                TransformerFeatureSubset(log_transformer, ["price"])
            ]
        ).with_target_transformer_list(
            [
                log_transformer
            ]
        ).build()

        m5_data = DemandModelDataLoader(M5DataLoader(), GenericSilverToGold()).get_demand_model_data()

        transformed_demand = model.fit_transform(m5_data)

        model.fit(transformed_demand)

        m5_demand_validation = DemandModelDataLoader(M5DataLoader(sales_file="sales_train_validation.csv"),
                                                     GenericSilverToGold()).get_demand_model_data()
        transformed_m5_evaluation = model.transform(m5_demand_validation)
        print(GenericEvaluator(model)(transformed_m5_evaluation))
