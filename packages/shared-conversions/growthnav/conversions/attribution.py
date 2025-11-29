"""
Attribution - assign conversions to ad platforms.

Supports multiple attribution models:
- Last-click (default): Credit to last touchpoint
- First-click: Credit to first touchpoint
- Linear: Equal credit to all touchpoints
- Time-decay: More credit to recent touchpoints
- Position-based: 40% first, 40% last, 20% middle
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from growthnav.conversions.schema import (
    AttributionModel,
    Conversion,
)


@dataclass
class AdClick:
    """Represents an ad click/impression for attribution matching."""

    platform: str  # "google_ads", "meta", "reddit", etc.
    click_id: str  # gclid, fbclid, etc.
    campaign_id: str | None = None
    ad_id: str | None = None
    timestamp: datetime = None
    user_id: str | None = None


@dataclass
class AttributionResult:
    """Result of attribution for a single conversion."""

    conversion: Conversion
    attributed: bool
    platform: str | None = None
    campaign_id: str | None = None
    ad_id: str | None = None
    model: AttributionModel | None = None
    weight: float = 1.0
    touchpoints: list[AdClick] = None

    def __post_init__(self):
        if self.touchpoints is None:
            self.touchpoints = []


def attribute_conversions(
    conversions: list[Conversion],
    clicks: list[AdClick],
    model: AttributionModel = AttributionModel.LAST_CLICK,
    lookback_days: int = 30,
) -> list[AttributionResult]:
    """
    Attribute conversions to ad clicks.

    Args:
        conversions: List of conversions to attribute
        clicks: List of ad clicks to match against
        model: Attribution model to use
        lookback_days: Maximum days between click and conversion

    Returns:
        List of AttributionResult objects
    """
    results = []
    lookback = timedelta(days=lookback_days)

    for conversion in conversions:
        # Find matching clicks
        matching_clicks = _find_matching_clicks(conversion, clicks, lookback)

        if not matching_clicks:
            # No attribution possible
            results.append(AttributionResult(
                conversion=conversion,
                attributed=False,
            ))
            continue

        # Apply attribution model
        if model == AttributionModel.LAST_CLICK:
            result = _last_click_attribution(conversion, matching_clicks)
        elif model == AttributionModel.FIRST_CLICK:
            result = _first_click_attribution(conversion, matching_clicks)
        elif model == AttributionModel.LINEAR:
            result = _linear_attribution(conversion, matching_clicks)
        elif model == AttributionModel.TIME_DECAY:
            result = _time_decay_attribution(conversion, matching_clicks)
        elif model == AttributionModel.POSITION_BASED:
            result = _position_based_attribution(conversion, matching_clicks)
        else:
            result = _last_click_attribution(conversion, matching_clicks)

        results.append(result)

    return results


def _find_matching_clicks(
    conversion: Conversion,
    clicks: list[AdClick],
    lookback: timedelta,
) -> list[AdClick]:
    """Find clicks that match the conversion within lookback window."""
    matching = []

    for click in clicks:
        # Check time window
        if click.timestamp and conversion.timestamp:
            if conversion.timestamp - click.timestamp > lookback:
                continue
            if click.timestamp > conversion.timestamp:
                continue  # Click after conversion

        # Check click ID match
        if conversion.gclid and click.click_id == conversion.gclid or conversion.fbclid and click.click_id == conversion.fbclid or conversion.ttclid and click.click_id == conversion.ttclid or conversion.msclkid and click.click_id == conversion.msclkid or conversion.user_id and click.user_id == conversion.user_id:
            matching.append(click)

    # Sort by timestamp (oldest first)
    matching.sort(key=lambda c: c.timestamp or datetime.min)

    return matching


def _last_click_attribution(
    conversion: Conversion,
    clicks: list[AdClick],
) -> AttributionResult:
    """Attribute to the last click before conversion."""
    last_click = clicks[-1]

    # Update conversion with attribution
    conversion.attributed_platform = last_click.platform
    conversion.attributed_campaign_id = last_click.campaign_id
    conversion.attributed_ad_id = last_click.ad_id
    conversion.attribution_model = AttributionModel.LAST_CLICK
    conversion.attribution_weight = 1.0

    return AttributionResult(
        conversion=conversion,
        attributed=True,
        platform=last_click.platform,
        campaign_id=last_click.campaign_id,
        ad_id=last_click.ad_id,
        model=AttributionModel.LAST_CLICK,
        weight=1.0,
        touchpoints=clicks,
    )


def _first_click_attribution(
    conversion: Conversion,
    clicks: list[AdClick],
) -> AttributionResult:
    """Attribute to the first click in the path."""
    first_click = clicks[0]

    conversion.attributed_platform = first_click.platform
    conversion.attributed_campaign_id = first_click.campaign_id
    conversion.attributed_ad_id = first_click.ad_id
    conversion.attribution_model = AttributionModel.FIRST_CLICK
    conversion.attribution_weight = 1.0

    return AttributionResult(
        conversion=conversion,
        attributed=True,
        platform=first_click.platform,
        campaign_id=first_click.campaign_id,
        ad_id=first_click.ad_id,
        model=AttributionModel.FIRST_CLICK,
        weight=1.0,
        touchpoints=clicks,
    )


def _linear_attribution(
    conversion: Conversion,
    clicks: list[AdClick],
) -> AttributionResult:
    """
    Distribute credit equally across all touchpoints.

    For the Conversion object, we attribute to the last click
    but set the weight to 1/n where n is the number of touchpoints.
    """
    weight = 1.0 / len(clicks)
    last_click = clicks[-1]

    conversion.attributed_platform = last_click.platform
    conversion.attributed_campaign_id = last_click.campaign_id
    conversion.attributed_ad_id = last_click.ad_id
    conversion.attribution_model = AttributionModel.LINEAR
    conversion.attribution_weight = weight

    return AttributionResult(
        conversion=conversion,
        attributed=True,
        platform=last_click.platform,
        campaign_id=last_click.campaign_id,
        ad_id=last_click.ad_id,
        model=AttributionModel.LINEAR,
        weight=weight,
        touchpoints=clicks,
    )


def _time_decay_attribution(
    conversion: Conversion,
    clicks: list[AdClick],
) -> AttributionResult:
    """
    More credit to recent touchpoints.

    Uses exponential decay with 7-day half-life.
    """
    import math

    half_life_days = 7
    weights = []

    for click in clicks:
        if click.timestamp and conversion.timestamp:
            days_before = (conversion.timestamp - click.timestamp).days
            weight = math.pow(0.5, days_before / half_life_days)
        else:
            weight = 1.0
        weights.append(weight)

    # Normalize weights
    total = sum(weights)
    weights = [w / total for w in weights]

    # For single conversion, attribute to highest weight (most recent)
    max_idx = weights.index(max(weights))
    best_click = clicks[max_idx]

    conversion.attributed_platform = best_click.platform
    conversion.attributed_campaign_id = best_click.campaign_id
    conversion.attributed_ad_id = best_click.ad_id
    conversion.attribution_model = AttributionModel.TIME_DECAY
    conversion.attribution_weight = weights[max_idx]

    return AttributionResult(
        conversion=conversion,
        attributed=True,
        platform=best_click.platform,
        campaign_id=best_click.campaign_id,
        ad_id=best_click.ad_id,
        model=AttributionModel.TIME_DECAY,
        weight=weights[max_idx],
        touchpoints=clicks,
    )


def _position_based_attribution(
    conversion: Conversion,
    clicks: list[AdClick],
) -> AttributionResult:
    """
    40% to first, 40% to last, 20% distributed to middle.
    """
    if len(clicks) == 1:
        return _last_click_attribution(conversion, clicks)

    if len(clicks) == 2:
        # 50/50 split
        last_click = clicks[-1]
        conversion.attributed_platform = last_click.platform
        conversion.attributed_campaign_id = last_click.campaign_id
        conversion.attributed_ad_id = last_click.ad_id
        conversion.attribution_model = AttributionModel.POSITION_BASED
        conversion.attribution_weight = 0.5

        return AttributionResult(
            conversion=conversion,
            attributed=True,
            platform=last_click.platform,
            campaign_id=last_click.campaign_id,
            ad_id=last_click.ad_id,
            model=AttributionModel.POSITION_BASED,
            weight=0.5,
            touchpoints=clicks,
        )

    # 40/20/40 split
    # For single conversion record, attribute to last with 40% weight
    last_click = clicks[-1]

    conversion.attributed_platform = last_click.platform
    conversion.attributed_campaign_id = last_click.campaign_id
    conversion.attributed_ad_id = last_click.ad_id
    conversion.attribution_model = AttributionModel.POSITION_BASED
    conversion.attribution_weight = 0.4

    return AttributionResult(
        conversion=conversion,
        attributed=True,
        platform=last_click.platform,
        campaign_id=last_click.campaign_id,
        ad_id=last_click.ad_id,
        model=AttributionModel.POSITION_BASED,
        weight=0.4,
        touchpoints=clicks,
    )
