from retail.domain import Dataset, Transformer


class Feature(Transformer):
    def fit_transform(self, dataset: Dataset) -> Dataset:
        pass

    def transform(self, dataset: Dataset) -> Dataset:
        pass


class FeatureSelector(Transformer):
    def fit_transform(self, dataset: Dataset) -> Dataset:
        pass

    def transform(self, dataset: Dataset) -> Dataset:
        pass


class Scope(Transformer):
    def fit_transform(self, dataset: Dataset) -> Dataset:
        pass

    def transform(self, dataset: Dataset) -> Dataset:
        pass


class Aggregator(Transformer):
    def fit_transform(self, dataset: Dataset) -> Dataset:
        pass

    def transform(self, dataset: Dataset) -> Dataset:
        pass


class Featurizer(Transformer):
    def fit_transform(self, dataset: Dataset) -> Dataset:
        pass

    def transform(self, dataset: Dataset) -> Dataset:
        pass


