"""
DDSketch – quantile sketch approssimato con garanzia di errore relativo.

Implementazione pura-Python di DDSketch (Masson, Rim, Lee – "DDSketch: A Fast
and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees", VLDB '19,
riferimento [6] della specifica), usata da Q3 come accumulatore incrementale:
i percentili sono calcolati senza ordinare né accumulare i singoli valori.

Principio: l'asse dei valori è partizionato in bucket geometrici con ratio
γ = (1+α)/(1-α); il valore v > 0 finisce nel bucket ⌈log_γ(v)⌉ e ogni bucket
mantiene solo un contatore. Il rappresentante del bucket i, 2·γ^i/(γ+1),
approssima ogni valore del bucket con errore relativo ≤ α. Il quantile q si
ottiene scorrendo i contatori in ordine di valore fino alla posizione
q·(n-1). Due sketch si fondono sommando i contatori (fully mergeable), quindi
l'accumulatore è compatibile con la merge() delle window di Flink.

DEP_DELAY può essere negativo (voli in anticipo): si usano due store
speculari (negativo su |v| e positivo) più un contatore per gli zeri, come
nell'implementazione di riferimento Datadog.

Memoria: O(log(max|v|/min|v|)/α) bucket. Con α = 0.01 e ritardi in minuti
nell'ordine di [-100, +3000] servono al più qualche centinaio di contatori
interi per gruppo (compagnia × fascia), contro le decine di migliaia di
valori grezzi di una finestra globale.

min/max/count sono mantenuti esatti: la specifica li richiede esatti e
servono anche a limitare (clamp) i quantili restituiti.
"""
from __future__ import annotations

import math

# Sotto questa soglia il valore è indistinguibile da zero per lo sketch
# (i ritardi del dataset sono minuti interi, la soglia non è mai rilevante
# in pratica ma evita log(0) su input degeneri).
_ZERO_EPSILON = 1e-9


class DelayDDSketch:
    """Sketch dei quantili di DEP_DELAY per un gruppo (compagnia, fascia)."""

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
        """Quantile approssimato q ∈ [0, 1]; None se lo sketch è vuoto."""
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
        """Numero di contatori vivi: la 'memoria' dello sketch (per il report)."""
        return len(self._pos) + len(self._neg) + (1 if self._zero_count else 0)
