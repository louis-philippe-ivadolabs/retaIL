from unittest import TestCase

from retail.data.m5.loader import M5DataLoader
from retail.demand.CatBoostDemandPredictor import CatBoostDemandPredictor
from retail.demand.builder import DemandModelBuilder
from retail.evaluator.generic_evaluator import GenericEvaluator
from retail.schema import model_input_schema
from retail.transformer.DemandAggregator import AggregationLevel, DemandAggregator
from retail.transformer.DemandFeatureSelector import DemandFeatureSelector
from retail.transformer.DemandScope import DemandScope
from retail.domain import Feature
from retail.transformer.LaggedFeature import LaggedFeature


class WorkflowTestCase(TestCase):

    def test_simple_workflow(self):
        feature_list = [
            Feature('effective_price', False, 'mean'),
            Feature('sales_qty', False, 'sum'),
            Feature('store_id', True, 'first'),
            Feature('year_week', False, 'first'),
            Feature('sku_id', True, 'first'),
            Feature('effective_price_lag_1',
                    False,
                    aggregation_f='mean',
                    ),
            Feature('effective_price_lag_2',
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

        lagged_features = [LaggedFeature('effective_price',
                                       'effective_price_lag_1',
                                       lag=1),
                          LaggedFeature('effective_price',
                                       'effective_price_lag_2',
                                       lag=2)
                          ]


        feature_selector = DemandFeatureSelector(feature_list)

        aggregator = DemandAggregator(AggregationLevel["year_week"], feature_list)

        model = DemandModelBuilder(
        ).with_demand_predictor(
            demand_predictor
        ).with_feature_list(
            feature_list
        ).with_transformer_list(
            [
                *lagged_features,
                demand_scope_train,
                aggregator,
                feature_selector
            ]
        ).build()

        # Get a Demand Dataset
        m5_demand = M5DataLoader().load_demand_dataset("sales_train_validation.csv")

        m5_demand = model_input_schema.validate(m5_demand)

        transformed_demand = model.transform(m5_demand)

        model.fit(transformed_demand)

        m5_evaluation = M5DataLoader().load_demand_dataset("sales_train_evaluation.csv")
        transformed_m5_evaluation = model.transform(m5_evaluation)
        print(GenericEvaluator(model)(transformed_m5_evaluation))