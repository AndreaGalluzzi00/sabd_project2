from __future__ import annotations

import random


SUPPORTED_DISTRIBUTIONS = {
    "uniform",
    "exponential",
}


def validate_holdback_distribution(
    distribution: str,
    max_delay_seconds: float,
    mean_seconds: float,
) -> None:
    if distribution not in SUPPORTED_DISTRIBUTIONS:
        raise ValueError(
            f"Unsupported holdback_distribution: {distribution}. "
            f"Supported values are: {sorted(SUPPORTED_DISTRIBUTIONS)}"
        )

    if max_delay_seconds < 0:
        raise ValueError("holdback_delay must be greater than or equal to zero")

    if distribution == "exponential" and mean_seconds <= 0:
        raise ValueError(
            "holdback_mean_seconds must be greater than zero when "
            "holdback_distribution is 'exponential'"
        )


def sample_holdback_delay_seconds(
    distribution: str,
    max_delay_seconds: float,
    mean_seconds: float,
) -> float:
    if max_delay_seconds <= 0:
        return 0.0

    if distribution == "uniform":
        return random.uniform(0.0, max_delay_seconds)

    if distribution == "exponential":
        delay = random.expovariate(1.0 / mean_seconds)
        return min(delay, max_delay_seconds)

    raise ValueError(f"Unsupported holdback_distribution: {distribution}")