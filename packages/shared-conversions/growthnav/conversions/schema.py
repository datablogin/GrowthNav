"""
Unified conversion schema - platform-agnostic conversion data model.

This schema normalizes conversion data from multiple sources:
- Point of Sale (POS) systems
- Customer Relationship Management (CRM) systems
- Loyalty Program databases
- E-commerce platforms

The unified schema enables:
- Cross-platform attribution (Google Ads + Meta → same sale)
- Industry benchmarking across customers
- Consistent reporting regardless of source
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from growthnav.connectors.identity import IdentityFragment


class ConversionSource(str, Enum):
    """Source system for conversion data."""

    POS = "pos"  # Point of Sale
    CRM = "crm"  # Customer Relationship Management
    LOYALTY = "loyalty"  # Loyalty Program
    ECOMMERCE = "ecommerce"  # Online store
    MANUAL = "manual"  # Manual entry
    API = "api"  # Direct API integration


class ConversionType(str, Enum):
    """Type of conversion event."""

    PURCHASE = "purchase"
    LEAD = "lead"
    SIGNUP = "signup"
    SUBSCRIPTION = "subscription"
    RENEWAL = "renewal"
    UPSELL = "upsell"
    REFERRAL = "referral"
    BOOKING = "booking"  # Appointments, reservations
    DOWNLOAD = "download"
    CUSTOM = "custom"


class AttributionModel(str, Enum):
    """Attribution model for ad platform assignment."""

    LAST_CLICK = "last_click"
    FIRST_CLICK = "first_click"
    LINEAR = "linear"  # Equal credit to all touchpoints
    TIME_DECAY = "time_decay"  # More credit to recent touchpoints
    POSITION_BASED = "position_based"  # 40% first, 40% last, 20% middle
    DATA_DRIVEN = "data_driven"  # ML-based


@dataclass
class Conversion:
    """
    Unified conversion record.

    Represents a single conversion event that can be attributed
    to one or more ad platforms.

    All timestamps are stored as timezone-aware datetime objects in UTC.
    This ensures consistency across different data sources and accurate
    cross-platform attribution (Google Ads, Meta, etc.).

    Example:
        conversion = Conversion(
            customer_id="topgolf",
            transaction_id="TXN-12345",
            conversion_type=ConversionType.PURCHASE,
            source=ConversionSource.POS,
            value=150.00,
            currency="USD",
            timestamp=datetime.now(UTC),
        )
    """

    # Customer identification
    customer_id: str  # GrowthNav customer (e.g., "topgolf")
    user_id: str | None = None  # End user/buyer identifier

    # Identity resolution (populated after linking)
    identity_fragments: list[IdentityFragment] = field(default_factory=list)
    global_customer_id: str | None = None  # Resolved cross-system ID

    # Transaction identification
    transaction_id: str | None = None  # Source system transaction ID
    conversion_id: UUID = field(default_factory=uuid4)  # GrowthNav internal ID

    # Conversion details
    conversion_type: ConversionType = ConversionType.PURCHASE
    source: ConversionSource = ConversionSource.POS
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Value
    value: float = 0.0
    currency: str = "USD"
    quantity: int = 1

    # Product/service details
    product_id: str | None = None
    product_name: str | None = None
    product_category: str | None = None

    # Location (for multi-location businesses)
    location_id: str | None = None
    location_name: str | None = None

    # Attribution data (populated after attribution)
    attributed_platform: str | None = None  # "google_ads", "meta", etc.
    attributed_campaign_id: str | None = None
    attributed_ad_id: str | None = None
    attribution_model: AttributionModel | None = None
    attribution_weight: float = 1.0  # For multi-touch attribution

    # Tracking identifiers (for matching to ad clicks)
    gclid: str | None = None  # Google Click ID
    fbclid: str | None = None  # Facebook Click ID
    ttclid: str | None = None  # TikTok Click ID
    msclkid: str | None = None  # Microsoft Click ID
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None

    # Raw data preservation
    raw_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for BigQuery insertion."""
        return {
            "customer_id": self.customer_id,
            "user_id": self.user_id,
            "identity_fragments": [
                {
                    "fragment_type": frag.fragment_type.value,
                    "fragment_value": frag.fragment_value,
                    "source_system": frag.source_system,
                    "confidence": frag.confidence,
                }
                for frag in self.identity_fragments
            ],
            "global_customer_id": self.global_customer_id,
            "transaction_id": self.transaction_id,
            "conversion_id": str(self.conversion_id),
            "conversion_type": self.conversion_type.value,
            "source": self.source.value,
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "currency": self.currency,
            "quantity": self.quantity,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "product_category": self.product_category,
            "location_id": self.location_id,
            "location_name": self.location_name,
            "attributed_platform": self.attributed_platform,
            "attributed_campaign_id": self.attributed_campaign_id,
            "attributed_ad_id": self.attributed_ad_id,
            "attribution_model": self.attribution_model.value if self.attribution_model else None,
            "attribution_weight": self.attribution_weight,
            "gclid": self.gclid,
            "fbclid": self.fbclid,
            "ttclid": self.ttclid,
            "msclkid": self.msclkid,
            "utm_source": self.utm_source,
            "utm_medium": self.utm_medium,
            "utm_campaign": self.utm_campaign,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Conversion:
        """Create Conversion from dictionary.

        Args:
            data: Dictionary containing conversion data.

        Returns:
            Conversion instance.

        Raises:
            ValueError: If required field 'customer_id' is missing or if
                value/quantity cannot be converted to numeric types.
        """
        # Validate required field
        if "customer_id" not in data:
            raise ValueError("Missing required field: customer_id")

        # Validate numeric fields
        try:
            value = float(data.get("value", 0))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid value: {data.get('value')}") from e

        try:
            quantity = int(data.get("quantity", 1))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid quantity: {data.get('quantity')}") from e

        try:
            attribution_weight = float(data.get("attribution_weight", 1.0))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid attribution_weight: {data.get('attribution_weight')}") from e

        # Parse timestamp
        timestamp_value = data.get("timestamp")
        if isinstance(timestamp_value, str):
            try:
                timestamp = datetime.fromisoformat(timestamp_value)
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {timestamp_value}") from e
        elif isinstance(timestamp_value, datetime):
            timestamp = timestamp_value
        else:
            timestamp = datetime.now(UTC)

        return cls(
            customer_id=data["customer_id"],
            user_id=data.get("user_id"),
            identity_fragments=[],  # Will be populated by identity linker
            global_customer_id=data.get("global_customer_id"),
            transaction_id=data.get("transaction_id"),
            conversion_id=UUID(data["conversion_id"]) if data.get("conversion_id") else uuid4(),
            conversion_type=ConversionType(data.get("conversion_type", "purchase")),
            source=ConversionSource(data.get("source", "pos")),
            timestamp=timestamp,
            value=value,
            currency=data.get("currency", "USD"),
            quantity=quantity,
            product_id=data.get("product_id"),
            product_name=data.get("product_name"),
            product_category=data.get("product_category"),
            location_id=data.get("location_id"),
            location_name=data.get("location_name"),
            attributed_platform=data.get("attributed_platform"),
            attributed_campaign_id=data.get("attributed_campaign_id"),
            attributed_ad_id=data.get("attributed_ad_id"),
            attribution_model=AttributionModel(data["attribution_model"]) if data.get("attribution_model") else None,
            attribution_weight=attribution_weight,
            gclid=data.get("gclid"),
            fbclid=data.get("fbclid"),
            ttclid=data.get("ttclid"),
            msclkid=data.get("msclkid"),
            utm_source=data.get("utm_source"),
            utm_medium=data.get("utm_medium"),
            utm_campaign=data.get("utm_campaign"),
            raw_data=data.get("raw_data", {}),
        )
