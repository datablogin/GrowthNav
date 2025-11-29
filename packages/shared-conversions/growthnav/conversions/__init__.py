"""
GrowthNav Conversions - Unified conversion tracking across platforms.

Provides:
- Platform-agnostic conversion schema
- Normalizers for POS, CRM, Loyalty Program data
- Cross-platform attribution support

The key insight: Conversions depend on the same transaction sources
(POS, CRM, Loyalty) regardless of which ad platform drove the traffic.
This allows unified attribution across Google Ads, Meta, Reddit, etc.

Usage:
    from growthnav.conversions import (
        Conversion,
        ConversionNormalizer,
        POSNormalizer,
        CRMNormalizer,
    )

    # Normalize POS transactions
    normalizer = POSNormalizer()
    conversions = normalizer.normalize(pos_data)

    # Attribute to ad platforms
    attributed = attribute_conversions(conversions, ad_clicks)
"""

from growthnav.conversions.attribution import (
    AttributionResult,
    attribute_conversions,
)
from growthnav.conversions.normalizer import (
    ConversionNormalizer,
    CRMNormalizer,
    LoyaltyNormalizer,
    POSNormalizer,
)
from growthnav.conversions.schema import (
    AttributionModel,
    Conversion,
    ConversionSource,
    ConversionType,
)

__all__ = [
    # Schema
    "Conversion",
    "ConversionSource",
    "ConversionType",
    "AttributionModel",
    # Normalizers
    "ConversionNormalizer",
    "POSNormalizer",
    "CRMNormalizer",
    "LoyaltyNormalizer",
    # Attribution
    "attribute_conversions",
    "AttributionResult",
]
