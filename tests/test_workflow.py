from unittest import TestCase

from retail.data.m5.loader import M5DataLoader
from retail.data_layer.generic_silver_to_gold import GenericSilverToGold
from retail.demand.demand_model_data import DemandModelDataLoader
from retail.demand.CatBoostDemandPredictor import CatBoostDemandPredictor
from retail.demand.builder import DemandModelBuilder
from retail.evaluator.generic_evaluator import GenericEvaluator
from retail.transformer.DemandAggregator import AggregationLevel, DemandAggregator
from retail.transformer.DemandFeatureSelector import DemandFeatureSelector
from retail.transformer.DemandScope import DemandScope
from retail.domain import Feature
from retail.transformer.LaggedFeature import LaggedFeature
from retail.transformer.NoSaleSkus import NoSaleSkus


class WorkflowTestCase(TestCase):

    def test_simple_workflow(self):
        m5_data = DemandModelDataLoader(M5DataLoader(), GenericSilverToGold()).get_demand_model_data()


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

        no_sale_skus = NoSaleSkus(m5_data)

        model = DemandModelBuilder(
        ).with_demand_predictor(
            demand_predictor
        ).with_feature_list(
            feature_list
        ).with_transformer_list(
            [
                demand_scope,
                no_sale_skus,
                *lagged_features,
                aggregator,
                feature_selector
            ]
        ).build()

        transformed_demand = model.transform(m5_data.gold_df)

        model.fit(transformed_demand)

        m5_demand_validation = DemandModelDataLoader(M5DataLoader(sales_file="sales_train_validation.csv"),
                                                     GenericSilverToGold()).get_demand_model_data()
        transformed_m5_evaluation = model.transform(m5_demand_validation.gold_df)
        print(GenericEvaluator(model)(transformed_m5_evaluation))
