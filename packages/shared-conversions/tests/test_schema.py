"""Tests for conversion schema (Conversion, enums)."""

from datetime import UTC, datetime
from uuid import UUID

from growthnav.conversions.schema import (
    AttributionModel,
    Conversion,
    ConversionSource,
    ConversionType,
)


class TestConversionSource:
    """Test ConversionSource enum."""

    def test_enum_values(self):
        """Test all enum values are defined correctly."""
        assert ConversionSource.POS == "pos"
        assert ConversionSource.CRM == "crm"
        assert ConversionSource.LOYALTY == "loyalty"
        assert ConversionSource.ECOMMERCE == "ecommerce"
        assert ConversionSource.MANUAL == "manual"
        assert ConversionSource.API == "api"

    def test_enum_members(self):
        """Test all expected members exist."""
        members = [e.value for e in ConversionSource]
        assert "pos" in members
        assert "crm" in members
        assert "loyalty" in members
        assert "ecommerce" in members
        assert "manual" in members
        assert "api" in members


class TestConversionType:
    """Test ConversionType enum."""

    def test_enum_values(self):
        """Test all enum values are defined correctly."""
        assert ConversionType.PURCHASE == "purchase"
        assert ConversionType.LEAD == "lead"
        assert ConversionType.SIGNUP == "signup"
        assert ConversionType.SUBSCRIPTION == "subscription"
        assert ConversionType.RENEWAL == "renewal"
        assert ConversionType.UPSELL == "upsell"
        assert ConversionType.REFERRAL == "referral"
        assert ConversionType.BOOKING == "booking"
        assert ConversionType.DOWNLOAD == "download"
        assert ConversionType.CUSTOM == "custom"

    def test_enum_members(self):
        """Test all expected members exist."""
        members = [e.value for e in ConversionType]
        assert "purchase" in members
        assert "lead" in members
        assert "signup" in members
        assert "subscription" in members
        assert "renewal" in members
        assert "upsell" in members
        assert "referral" in members
        assert "booking" in members
        assert "download" in members
        assert "custom" in members


class TestAttributionModel:
    """Test AttributionModel enum."""

    def test_enum_values(self):
        """Test all enum values are defined correctly."""
        assert AttributionModel.LAST_CLICK == "last_click"
        assert AttributionModel.FIRST_CLICK == "first_click"
        assert AttributionModel.LINEAR == "linear"
        assert AttributionModel.TIME_DECAY == "time_decay"
        assert AttributionModel.POSITION_BASED == "position_based"
        assert AttributionModel.DATA_DRIVEN == "data_driven"

    def test_enum_members(self):
        """Test all expected members exist."""
        members = [e.value for e in AttributionModel]
        assert "last_click" in members
        assert "first_click" in members
        assert "linear" in members
        assert "time_decay" in members
        assert "position_based" in members
        assert "data_driven" in members


class TestConversion:
    """Test Conversion dataclass."""

    def test_minimal_conversion(self):
        """Test creating conversion with minimal required fields."""
        conversion = Conversion(customer_id="test_customer")

        assert conversion.customer_id == "test_customer"
        assert conversion.user_id is None
        assert conversion.transaction_id is None
        assert isinstance(conversion.conversion_id, UUID)
        assert conversion.conversion_type == ConversionType.PURCHASE
        assert conversion.source == ConversionSource.POS
        assert isinstance(conversion.timestamp, datetime)
        assert conversion.value == 0.0
        assert conversion.currency == "USD"
        assert conversion.quantity == 1
        assert conversion.attribution_weight == 1.0
        assert conversion.raw_data == {}

    def test_full_conversion(self):
        """Test creating conversion with all fields populated."""
        timestamp = datetime(2025, 1, 15, 10, 30, 0)
        conversion_id = UUID("12345678-1234-5678-1234-567812345678")

        conversion = Conversion(
            customer_id="topgolf",
            user_id="user123",
            transaction_id="TXN-9999",
            conversion_id=conversion_id,
            conversion_type=ConversionType.PURCHASE,
            source=ConversionSource.POS,
            timestamp=timestamp,
            value=150.00,
            currency="USD",
            quantity=2,
            product_id="PROD-123",
            product_name="Bay Rental",
            product_category="entertainment",
            location_id="LOC-456",
            location_name="TopGolf Austin",
            attributed_platform="google_ads",
            attributed_campaign_id="CAMP-789",
            attributed_ad_id="AD-012",
            attribution_model=AttributionModel.LAST_CLICK,
            attribution_weight=1.0,
            gclid="abc123",
            fbclid="def456",
            ttclid="ghi789",
            msclkid="jkl012",
            utm_source="google",
            utm_medium="cpc",
            utm_campaign="summer_promo",
            raw_data={"extra": "data"},
        )

        assert conversion.customer_id == "topgolf"
        assert conversion.user_id == "user123"
        assert conversion.transaction_id == "TXN-9999"
        assert conversion.conversion_id == conversion_id
        assert conversion.conversion_type == ConversionType.PURCHASE
        assert conversion.source == ConversionSource.POS
        assert conversion.timestamp == timestamp
        assert conversion.value == 150.00
        assert conversion.currency == "USD"
        assert conversion.quantity == 2
        assert conversion.product_id == "PROD-123"
        assert conversion.product_name == "Bay Rental"
        assert conversion.product_category == "entertainment"
        assert conversion.location_id == "LOC-456"
        assert conversion.location_name == "TopGolf Austin"
        assert conversion.attributed_platform == "google_ads"
        assert conversion.attributed_campaign_id == "CAMP-789"
        assert conversion.attributed_ad_id == "AD-012"
        assert conversion.attribution_model == AttributionModel.LAST_CLICK
        assert conversion.attribution_weight == 1.0
        assert conversion.gclid == "abc123"
        assert conversion.fbclid == "def456"
        assert conversion.ttclid == "ghi789"
        assert conversion.msclkid == "jkl012"
        assert conversion.utm_source == "google"
        assert conversion.utm_medium == "cpc"
        assert conversion.utm_campaign == "summer_promo"
        assert conversion.raw_data == {"extra": "data"}

    def test_auto_generated_uuid(self):
        """Test that conversion_id is auto-generated as UUID."""
        conversion1 = Conversion(customer_id="test")
        conversion2 = Conversion(customer_id="test")

        assert isinstance(conversion1.conversion_id, UUID)
        assert isinstance(conversion2.conversion_id, UUID)
        assert conversion1.conversion_id != conversion2.conversion_id

    def test_auto_generated_timestamp(self):
        """Test that timestamp is auto-generated."""
        before = datetime.now(UTC)
        conversion = Conversion(customer_id="test")
        after = datetime.now(UTC)

        assert before <= conversion.timestamp <= after

    def test_to_dict_minimal(self):
        """Test serialization with minimal fields."""
        conversion = Conversion(customer_id="test")
        data = conversion.to_dict()

        assert data["customer_id"] == "test"
        assert data["user_id"] is None
        assert data["transaction_id"] is None
        assert isinstance(data["conversion_id"], str)
        assert data["conversion_type"] == "purchase"
        assert data["source"] == "pos"
        assert isinstance(data["timestamp"], str)
        assert data["value"] == 0.0
        assert data["currency"] == "USD"
        assert data["quantity"] == 1
        assert data["attribution_model"] is None

    def test_to_dict_full(self):
        """Test serialization with all fields."""
        timestamp = datetime(2025, 1, 15, 10, 30, 0)
        conversion_id = UUID("12345678-1234-5678-1234-567812345678")

        conversion = Conversion(
            customer_id="topgolf",
            user_id="user123",
            transaction_id="TXN-9999",
            conversion_id=conversion_id,
            conversion_type=ConversionType.LEAD,
            source=ConversionSource.CRM,
            timestamp=timestamp,
            value=500.00,
            currency="EUR",
            quantity=3,
            product_id="PROD-123",
            product_name="Consultation",
            product_category="service",
            location_id="LOC-456",
            location_name="Office North",
            attributed_platform="meta",
            attributed_campaign_id="CAMP-789",
            attributed_ad_id="AD-012",
            attribution_model=AttributionModel.LINEAR,
            attribution_weight=0.5,
            gclid="abc123",
            fbclid="def456",
            utm_source="facebook",
            utm_medium="social",
            utm_campaign="q4_leads",
        )

        data = conversion.to_dict()

        assert data["customer_id"] == "topgolf"
        assert data["user_id"] == "user123"
        assert data["transaction_id"] == "TXN-9999"
        assert data["conversion_id"] == "12345678-1234-5678-1234-567812345678"
        assert data["conversion_type"] == "lead"
        assert data["source"] == "crm"
        assert data["timestamp"] == "2025-01-15T10:30:00"
        assert data["value"] == 500.00
        assert data["currency"] == "EUR"
        assert data["quantity"] == 3
        assert data["product_id"] == "PROD-123"
        assert data["product_name"] == "Consultation"
        assert data["product_category"] == "service"
        assert data["location_id"] == "LOC-456"
        assert data["location_name"] == "Office North"
        assert data["attributed_platform"] == "meta"
        assert data["attributed_campaign_id"] == "CAMP-789"
        assert data["attributed_ad_id"] == "AD-012"
        assert data["attribution_model"] == "linear"
        assert data["attribution_weight"] == 0.5
        assert data["gclid"] == "abc123"
        assert data["fbclid"] == "def456"
        assert data["utm_source"] == "facebook"
        assert data["utm_medium"] == "social"
        assert data["utm_campaign"] == "q4_leads"

    def test_from_dict_minimal(self):
        """Test deserialization with minimal fields."""
        data = {
            "customer_id": "test",
            "conversion_id": "12345678-1234-5678-1234-567812345678",
            "timestamp": "2025-01-15T10:30:00",
        }

        conversion = Conversion.from_dict(data)

        assert conversion.customer_id == "test"
        assert str(conversion.conversion_id) == "12345678-1234-5678-1234-567812345678"
        assert conversion.timestamp == datetime(2025, 1, 15, 10, 30, 0)
        assert conversion.conversion_type == ConversionType.PURCHASE
        assert conversion.source == ConversionSource.POS
        assert conversion.value == 0.0
        assert conversion.currency == "USD"

    def test_from_dict_full(self):
        """Test deserialization with all fields."""
        data = {
            "customer_id": "topgolf",
            "user_id": "user123",
            "transaction_id": "TXN-9999",
            "conversion_id": "12345678-1234-5678-1234-567812345678",
            "conversion_type": "lead",
            "source": "crm",
            "timestamp": "2025-01-15T10:30:00",
            "value": 500.00,
            "currency": "EUR",
            "quantity": 3,
            "product_id": "PROD-123",
            "product_name": "Consultation",
            "product_category": "service",
            "location_id": "LOC-456",
            "location_name": "Office North",
            "attributed_platform": "meta",
            "attributed_campaign_id": "CAMP-789",
            "attributed_ad_id": "AD-012",
            "attribution_model": "linear",
            "attribution_weight": 0.5,
            "gclid": "abc123",
            "fbclid": "def456",
            "ttclid": "ghi789",
            "msclkid": "jkl012",
            "utm_source": "facebook",
            "utm_medium": "social",
            "utm_campaign": "q4_leads",
            "raw_data": {"extra": "data"},
        }

        conversion = Conversion.from_dict(data)

        assert conversion.customer_id == "topgolf"
        assert conversion.user_id == "user123"
        assert conversion.transaction_id == "TXN-9999"
        assert str(conversion.conversion_id) == "12345678-1234-5678-1234-567812345678"
        assert conversion.conversion_type == ConversionType.LEAD
        assert conversion.source == ConversionSource.CRM
        assert conversion.timestamp == datetime(2025, 1, 15, 10, 30, 0)
        assert conversion.value == 500.00
        assert conversion.currency == "EUR"
        assert conversion.quantity == 3
        assert conversion.product_id == "PROD-123"
        assert conversion.product_name == "Consultation"
        assert conversion.product_category == "service"
        assert conversion.location_id == "LOC-456"
        assert conversion.location_name == "Office North"
        assert conversion.attributed_platform == "meta"
        assert conversion.attributed_campaign_id == "CAMP-789"
        assert conversion.attributed_ad_id == "AD-012"
        assert conversion.attribution_model == AttributionModel.LINEAR
        assert conversion.attribution_weight == 0.5
        assert conversion.gclid == "abc123"
        assert conversion.fbclid == "def456"
        assert conversion.ttclid == "ghi789"
        assert conversion.msclkid == "jkl012"
        assert conversion.utm_source == "facebook"
        assert conversion.utm_medium == "social"
        assert conversion.utm_campaign == "q4_leads"
        assert conversion.raw_data == {"extra": "data"}

    def test_roundtrip_serialization(self):
        """Test to_dict -> from_dict roundtrip."""
        original = Conversion(
            customer_id="test",
            user_id="user456",
            transaction_id="TXN-123",
            conversion_type=ConversionType.SUBSCRIPTION,
            source=ConversionSource.ECOMMERCE,
            value=99.99,
            currency="GBP",
            gclid="test_gclid",
            utm_campaign="test_campaign",
        )

        # Serialize and deserialize
        data = original.to_dict()
        restored = Conversion.from_dict(data)

        # Compare key fields
        assert restored.customer_id == original.customer_id
        assert restored.user_id == original.user_id
        assert restored.transaction_id == original.transaction_id
        assert str(restored.conversion_id) == str(original.conversion_id)
        assert restored.conversion_type == original.conversion_type
        assert restored.source == original.source
        assert restored.value == original.value
        assert restored.currency == original.currency
        assert restored.gclid == original.gclid
        assert restored.utm_campaign == original.utm_campaign

    def test_from_dict_missing_conversion_id(self):
        """Test that missing conversion_id generates a new UUID."""
        data = {
            "customer_id": "test",
            "timestamp": "2025-01-15T10:30:00",
        }

        conversion = Conversion.from_dict(data)

        assert isinstance(conversion.conversion_id, UUID)

    def test_from_dict_missing_timestamp(self):
        """Test that missing timestamp uses current time."""
        data = {
            "customer_id": "test",
        }

        before = datetime.now(UTC)
        conversion = Conversion.from_dict(data)
        after = datetime.now(UTC)

        assert before <= conversion.timestamp <= after

    def test_from_dict_datetime_object(self):
        """Test from_dict with datetime object instead of string."""
        timestamp = datetime(2025, 1, 15, 10, 30, 0)
        data = {
            "customer_id": "test",
            "timestamp": timestamp,
        }

        conversion = Conversion.from_dict(data)

        assert conversion.timestamp == timestamp

    def test_nullable_attribution_model_in_to_dict(self):
        """Test that None attribution_model serializes correctly."""
        conversion = Conversion(customer_id="test")
        conversion.attribution_model = None

        data = conversion.to_dict()

        assert data["attribution_model"] is None

    def test_attribution_model_in_to_dict(self):
        """Test that attribution_model serializes to string value."""
        conversion = Conversion(customer_id="test")
        conversion.attribution_model = AttributionModel.FIRST_CLICK

        data = conversion.to_dict()

        assert data["attribution_model"] == "first_click"
