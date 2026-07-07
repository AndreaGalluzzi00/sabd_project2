
from __future__ import annotations

import math

# Sotto questa soglia il valore è indistinguibile da zero per lo sketch
# (i ritardi del dataset sono minuti interi, la soglia non è mai rilevante
# in pratica ma evita log(0) su input degeneri).
_ZERO_EPSILON = 1e-9


class DelayDDSketch:

    __slots__ = (
        "alpha",
        "_gamma",
        "_log_gamma",
        "_pos",
        "_neg",
        "_zero_count",
        "count",
        "min_value",
        "max_value",
    )

    def __init__(self, alpha: float = 0.01):
        if not 0.0 < alpha < 1.0:
            raise ValueError("alpha must be in (0, 1)")

        self.alpha = alpha
        self._gamma = (1.0 + alpha) / (1.0 - alpha)
        self._log_gamma = math.log(self._gamma)

        self._pos: dict[int, int] = {}   # bucket index -> count (valori > 0)
        self._neg: dict[int, int] = {}   # bucket index su |v| -> count (valori < 0)
        self._zero_count = 0

        self.count = 0
        self.min_value = math.inf
        self.max_value = -math.inf

    # ── Mapping bucket ↔ valore ───────────────────────────────────────────────

    def _bucket_index(self, magnitude: float) -> int:
        return math.ceil(math.log(magnitude) / self._log_gamma)

    def _bucket_value(self, index: int) -> float:
        # Rappresentante del bucket i = (γ^(i-1), γ^i]: errore relativo ≤ α.
        return 2.0 * self._gamma ** index / (self._gamma + 1.0)

    # ── Inserimento e merge ───────────────────────────────────────────────────

    def add(self, value: float) -> None:
        value = float(value)

        if value > _ZERO_EPSILON:
            index = self._bucket_index(value)
            self._pos[index] = self._pos.get(index, 0) + 1
        elif value < -_ZERO_EPSILON:
            index = self._bucket_index(-value)
            self._neg[index] = self._neg.get(index, 0) + 1
        else:
            self._zero_count += 1

        self.count += 1
        if value < self.min_value:
            self.min_value = value
        if value > self.max_value:
            self.max_value = value

    def merge(self, other: "DelayDDSketch") -> None:
        for index, cnt in other._pos.items():
            self._pos[index] = self._pos.get(index, 0) + cnt
        for index, cnt in other._neg.items():
            self._neg[index] = self._neg.get(index, 0) + cnt

        self._zero_count += other._zero_count
        self.count += other.count
        self.min_value = min(self.min_value, other.min_value)
        self.max_value = max(self.max_value, other.max_value)

    # ── Interrogazione ────────────────────────────────────────────────────────

    def quantile(self, q: float) -> float | None:
        if self.count == 0:
            return None
        if q <= 0.0:
            return self.min_value
        if q >= 1.0:
            return self.max_value

        rank = q * (self.count - 1)
        cumulative = 0

        # Ordine crescente di valore: negativi dal più piccolo (|v| massimo,
        # indice massimo) al più grande, poi gli zeri, poi i positivi.
        for index in sorted(self._neg, reverse=True):
            cumulative += self._neg[index]
            if cumulative > rank:
                return self._clamp(-self._bucket_value(index))

        cumulative += self._zero_count
        if cumulative > rank:
            return self._clamp(0.0)

        for index in sorted(self._pos):
            cumulative += self._pos[index]
            if cumulative > rank:
                return self._clamp(self._bucket_value(index))

        return self.max_value

    def _clamp(self, value: float) -> float:
        return min(max(value, self.min_value), self.max_value)

    def bucket_count(self) -> int:
        return len(self._pos) + len(self._neg) + (1 if self._zero_count else 0)
