from typing import Any

from data_gold.domain import Scope
from model.forecaster import Forecaster


class ForecasterValidator:

    def validate(self, forecaster: Forecaster, validation_scope: Scope) -> dict[str, Any]:
        pass
