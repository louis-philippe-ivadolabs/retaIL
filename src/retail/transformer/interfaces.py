from retail.domain import Dataset, Transformer


class Feature(Transformer):
    def transform(self, dataset: Dataset) -> Dataset:
        pass


class Scope(Transformer):

    def transform(self, dataset: Dataset) -> Dataset:
        pass


class Aggregator(Transformer):

    def transform(self, dataset: Dataset) -> Dataset:
        pass

