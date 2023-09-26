from unittest import TestCase

from retail.data.xm5.loader import XM5DataLoader
from retail.demand.builder import DemandModelBuilder


class WorkflowTestCase(TestCase):

    def test_simple_workflow(self):
        # Get a Demand Dataset
        xm5_demand = XM5DataLoader.load_demand_dataset()

        # Configure the pipeline
        pipeline = [

        ]

        # Create the Demand Model
        predictor, training_data, test_data, validation_data = (DemandModelBuilder(xm5_demand)
                                                                .with_transform_pipeline(pipeline)
        .with_split()
                                                                .build())

