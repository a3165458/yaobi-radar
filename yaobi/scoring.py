"""Scoring engine for identifying potential 'yaobi' futures contracts."""

from __future__ import annotations

from dataclasses import dataclass
from math import copysign

from .config import ScoringConfig


@dataclass(slots=True)
class ScanMetrics:
    """Normalized inputs required by the scoring engine."""

    symbol: str
    price: float
    change_5m: float
    change_15m: float
    change_1h: float
    price_change_24h: float
    current_5m_volume: float
    previous_30m_volumes: list[float]
    current_open_interest: float
    previous_open_interest: float | None
    funding_rate: float
    quote_volume_24h: float
    funding_rate_change: float = 0.0
    consecutive_direction: int = 0
    liquidation_notional: float | None = None


@dataclass(slots=True)
class ScoreBreakdown:
    """Detailed scoring output for downstream alerting and storage."""

    total_score: int
    volume_score: int
    price_score: int
    oi_score: int
    funding_score: int
    liquidity_score: int
    adjustment_score: int
    alert_level: str
    volume_ratio: float
    max_price_change: float
    oi_change_pct: float


def _ema(values: list[float]) -> float:
    """Compute a short EMA for the previous 30 minutes of 5m volume."""

    if not values:
        return 0.0

    smoothing = 2 / (len(values) + 1)
    ema_value = values[0]
    for value in values[1:]:
        ema_value = (value * smoothing) + (ema_value * (1 - smoothing))
    return ema_value


def _scale(weight: int, ratio: float) -> int:
    """Scale a component weight by a fixed ratio and round to the nearest integer."""

    return int(round(weight * ratio))


def _volume_score(volume_ratio: float, weight: int) -> int:
    if volume_ratio > 10:
        return weight
    if volume_ratio > 5:
        return _scale(weight, 0.8)
    if volume_ratio > 3:
        return _scale(weight, 0.6)
    if volume_ratio > 2:
        return _scale(weight, 0.4)
    if volume_ratio > 1.5:
        return _scale(weight, 0.2)
    return 0


def _price_score(max_price_change: float, weight: int) -> int:
    if max_price_change > 15:
        return weight
    if max_price_change > 10:
        return _scale(weight, 0.8)
    if max_price_change > 8:
        return _scale(weight, 0.6)
    if max_price_change > 5:
        return _scale(weight, 0.4)
    if max_price_change > 3:
        return _scale(weight, 0.2)
    return 0


def _oi_score(abs_oi_change: float, weight: int) -> int:
    if abs_oi_change > 50:
        return weight
    if abs_oi_change > 30:
        return _scale(weight, 0.8)
    if abs_oi_change > 15:
        return _scale(weight, 0.6)
    if abs_oi_change > 5:
        return _scale(weight, 0.4)
    if abs_oi_change > 2:
        return _scale(weight, 0.2)
    return 0


def _funding_score(abs_funding_rate_pct: float, weight: int) -> int:
    if abs_funding_rate_pct > 0.05:
        return weight
    if abs_funding_rate_pct > 0.03:
        return _scale(weight, 0.8)
    if abs_funding_rate_pct > 0.01:
        return _scale(weight, 0.6)
    if abs_funding_rate_pct > 0.005:
        return _scale(weight, 0.3)
    return 0


def _liquidity_score(quote_volume_24h: float, weight: int) -> int:
    if quote_volume_24h < 10_000_000:
        return weight
    if 10_000_000 <= quote_volume_24h < 100_000_000:
        return _scale(weight, 0.8)
    if 100_000_000 <= quote_volume_24h < 1_000_000_000:
        return _scale(weight, 0.6)
    if quote_volume_24h >= 1_000_000_000:
        return _scale(weight, 0.2)
    return 0


def _alert_level(score: int, config: ScoringConfig) -> str:
    """Map a total score to the required alert level."""

    if score >= config.critical_threshold:
        return "critical"
    if score >= config.alert_threshold:
        return "warning"
    if score >= 30:
        return "info"
    return "ignore"


def calculate_score(metrics: ScanMetrics, config: ScoringConfig) -> ScoreBreakdown:
    """Calculate the full yaobi score from normalized market metrics."""

    baseline_volume = _ema(metrics.previous_30m_volumes)
    volume_ratio = (
        metrics.current_5m_volume / baseline_volume if baseline_volume > 0 else 0.0
    )
    max_price_change = max(
        abs(metrics.change_5m),
        abs(metrics.change_15m),
        abs(metrics.change_1h),
    )

    if metrics.previous_open_interest and metrics.previous_open_interest > 0:
        oi_change_pct = (
            (metrics.current_open_interest - metrics.previous_open_interest)
            / metrics.previous_open_interest
            * 100
        )
    else:
        oi_change_pct = 0.0

    volume_score = _volume_score(volume_ratio, config.volume_weight)
    price_score = _price_score(max_price_change, config.price_weight)
    oi_score = _oi_score(abs(oi_change_pct), config.oi_weight)
    funding_score = _funding_score(abs(metrics.funding_rate) * 100, config.funding_weight)
    liquidity_score = _liquidity_score(metrics.quote_volume_24h, config.liquidity_weight)

    adjustment_score = 0

    # Refined Price-OI Divergence analysis
    if metrics.change_5m > 0:
        if oi_change_pct > 2:  # Price Up + OI Up (Strong Long Trend)
            adjustment_score += 5
    elif metrics.change_5m < 0:
        if oi_change_pct > 2:  # Price Down + OI Up (Strong Short Trend)
            adjustment_score += 3
        elif oi_change_pct < -2:  # Price Down + OI Down (Long Liquidation Panic)
            adjustment_score -= 5

    # Funding rate acceleration scoring
    abs_funding_change = abs(metrics.funding_rate_change) * 100  # Convert to percentage
    if abs_funding_change > 0.02:
        adjustment_score += 5
    elif abs_funding_change > 0.01:
        adjustment_score += 2

    # Consecutive candles in one direction add persistence to the move.
    if metrics.consecutive_direction >= 3:
        adjustment_score += 2

    # Liquidation spikes are optional because not every deployment will collect them.
    if metrics.liquidation_notional is not None and metrics.liquidation_notional > 0:
        adjustment_score += 3

    total_score = max(
        0,
        min(
            100,
            volume_score
            + price_score
            + oi_score
            + funding_score
            + liquidity_score
            + adjustment_score,
        ),
    )

    return ScoreBreakdown(
        total_score=total_score,
        volume_score=volume_score,
        price_score=price_score,
        oi_score=oi_score,
        funding_score=funding_score,
        liquidity_score=liquidity_score,
        adjustment_score=adjustment_score,
        alert_level=_alert_level(total_score, config),
        volume_ratio=volume_ratio,
        max_price_change=max_price_change,
        oi_change_pct=oi_change_pct,
    )


def classify_direction(changes: list[float]) -> int:
    """Return how many of the latest candles share the same direction."""

    if not changes:
        return 0

    direction = 0
    count = 0
    for change in changes:
        if change == 0:
            break
        current = int(copysign(1, change))
        if direction == 0:
            direction = current
        if current != direction:
            break
        count += 1
    return count
