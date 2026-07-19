from __future__ import annotations

import math

from ddsketch import DDSketch


class Q3DDSketch:

    __slots__ = (
        "relative_accuracy",
        "_sketch",
        "count",
        "min_value",
        "max_value",
    )

    def __init__(self, relative_accuracy: float = 0.01) -> None:
        relative_accuracy = float(relative_accuracy)
        if not 0.0 < relative_accuracy < 1.0:
            raise ValueError("relative_accuracy must be in (0, 1)")

        self.relative_accuracy = relative_accuracy
        self._sketch = DDSketch(relative_accuracy)
        self.count = 0
        self.min_value = math.inf
        self.max_value = -math.inf

    def add(self, value: float) -> None:
        value = float(value)
        self._sketch.add(value)
        self.count += 1
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)

    def merge(self, other: "Q3DDSketch") -> None:
        if not isinstance(other, Q3DDSketch):
            raise TypeError("can only merge another Q3DDSketch")
        if self.relative_accuracy != other.relative_accuracy:
            raise ValueError(
                "cannot merge DDSketches with different relative accuracy"
            )
        if other.count == 0:
            return

        self._sketch.merge(other._sketch)
        self.count += other.count
        self.min_value = min(self.min_value, other.min_value)
        self.max_value = max(self.max_value, other.max_value)

    def quantile(self, quantile: float) -> float | None:
        if self.count == 0:
            return None
        value = self._sketch.get_quantile_value(float(quantile))
        if value is None:
            return None
        return min(max(float(value), self.min_value), self.max_value)
