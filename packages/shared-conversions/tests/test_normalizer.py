"""Tests for conversion normalizers (POSNormalizer, CRMNormalizer, LoyaltyNormalizer)."""

from datetime import UTC, datetime

import pandas as pd
from growthnav.conversions.normalizer import (
    CRMNormalizer,
    LoyaltyNormalizer,
    POSNormalizer,
)
from growthnav.conversions.schema import (
    ConversionSource,
    ConversionType,
)


class TestPOSNormalizer:
    """Test POSNormalizer."""

    def test_normalize_basic_transaction_dict(self):
        """Test normalizing a basic POS transaction from dict."""
        normalizer = POSNormalizer(customer_id="topgolf")

        data = [
            {
                "order_id": "ORD-123",
                "total": 150.00,
                "created_at": "2025-01-15T10:30:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.customer_id == "topgolf"
        assert conv.transaction_id == "ORD-123"
        assert conv.value == 150.00
        assert conv.timestamp == datetime(2025, 1, 15, 10, 30, 0)
        assert conv.source == ConversionSource.POS
        assert conv.conversion_type == ConversionType.PURCHASE

    def test_normalize_from_dataframe(self):
        """Test normalizing from pandas DataFrame."""
        normalizer = POSNormalizer(customer_id="test")

        df = pd.DataFrame([
            {
                "transaction_id": "TXN-001",
                "amount": 99.99,
                "date": "2025-01-10T14:00:00",
            },
            {
                "transaction_id": "TXN-002",
                "amount": 250.50,
                "date": "2025-01-11T16:30:00",
            },
        ])

        conversions = normalizer.normalize(df)

        assert len(conversions) == 2
        assert conversions[0].transaction_id == "TXN-001"
        assert conversions[0].value == 99.99
        assert conversions[1].transaction_id == "TXN-002"
        assert conversions[1].value == 250.50

    def test_custom_field_mapping(self):
        """Test custom field mapping."""
        normalizer = POSNormalizer(
            customer_id="custom",
            field_map={
                "receipt_no": "transaction_id",
                "price": "value",
                "sale_date": "timestamp",
                "guest": "user_id",
            }
        )

        data = [
            {
                "receipt_no": "REC-999",
                "price": 75.00,
                "sale_date": "2025-01-20T12:00:00",
                "guest": "GUEST-456",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.transaction_id == "REC-999"
        assert conv.value == 75.00
        assert conv.user_id == "GUEST-456"
        assert conv.timestamp == datetime(2025, 1, 20, 12, 0, 0)

    def test_location_field_mapping(self):
        """Test location field mapping."""
        normalizer = POSNormalizer(customer_id="multi_location")

        data = [
            {
                "order_id": "ORD-456",
                "total": 200.00,
                "created_at": "2025-01-15T10:30:00",
                "store_id": "STORE-01",
                "store_name": "Downtown Location",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.location_id == "STORE-01"
        assert conv.location_name == "Downtown Location"

    def test_raw_data_preservation(self):
        """Test that raw data is preserved."""
        normalizer = POSNormalizer(customer_id="test")

        data = [
            {
                "order_id": "ORD-789",
                "total": 100.00,
                "created_at": "2025-01-15T10:30:00",
                "custom_field": "custom_value",
                "metadata": {"key": "value"},
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.raw_data["custom_field"] == "custom_value"
        assert conv.raw_data["metadata"] == {"key": "value"}

    def test_default_field_mappings(self):
        """Test that default field mappings work for common variants."""
        normalizer = POSNormalizer(customer_id="test")

        # Test each variant separately to avoid pandas NaN issues
        data1 = [{"order_id": "ORD-1", "total": 10.00, "created_at": "2025-01-01T10:00:00"}]
        data2 = [{"receipt_number": "REC-2", "amount": 20.00, "order_date": "2025-01-02T10:00:00"}]
        data3 = [{"check_number": "CHK-3", "subtotal": 30.00, "transaction_date": "2025-01-03T10:00:00"}]
        data4 = [{"transaction_id": "TXN-4", "total_amount": 40.00, "date": "2025-01-04T10:00:00"}]

        conv1 = normalizer.normalize(data1)
        conv2 = normalizer.normalize(data2)
        conv3 = normalizer.normalize(data3)
        conv4 = normalizer.normalize(data4)

        assert len(conv1) == 1
        assert conv1[0].transaction_id == "ORD-1"
        assert conv1[0].value == 10.00

        assert len(conv2) == 1
        assert conv2[0].transaction_id == "REC-2"
        assert conv2[0].value == 20.00

        assert len(conv3) == 1
        assert conv3[0].transaction_id == "CHK-3"
        assert conv3[0].value == 30.00

        assert len(conv4) == 1
        assert conv4[0].transaction_id == "TXN-4"
        assert conv4[0].value == 40.00

    def test_customer_id_variants(self):
        """Test various customer/user ID field names."""
        normalizer = POSNormalizer(customer_id="test")

        # Test each variant separately to avoid pandas NaN issues
        data1 = [{"order_id": "ORD-1", "total": 10.00, "created_at": "2025-01-01T10:00:00", "customer_id": "CUST-1"}]
        data2 = [{"order_id": "ORD-2", "total": 20.00, "created_at": "2025-01-02T10:00:00", "guest_id": "GUEST-2"}]
        data3 = [{"order_id": "ORD-3", "total": 30.00, "created_at": "2025-01-03T10:00:00", "member_id": "MEM-3"}]

        conv1 = normalizer.normalize(data1)
        conv2 = normalizer.normalize(data2)
        conv3 = normalizer.normalize(data3)

        assert len(conv1) == 1
        assert conv1[0].user_id == "CUST-1"

        assert len(conv2) == 1
        assert conv2[0].user_id == "GUEST-2"

        assert len(conv3) == 1
        assert conv3[0].user_id == "MEM-3"

    def test_empty_data_returns_empty_list(self):
        """Test that empty data returns empty list."""
        normalizer = POSNormalizer(customer_id="test")

        conversions = normalizer.normalize([])

        assert conversions == []

    def test_missing_optional_fields(self):
        """Test that missing optional fields are handled gracefully."""
        normalizer = POSNormalizer(customer_id="test")

        data = [
            {
                "order_id": "ORD-100",
                # Missing value field
                # Missing timestamp field
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.transaction_id == "ORD-100"
        assert conv.value == 0.0  # Default value
        assert isinstance(conv.timestamp, datetime)  # Auto-generated

    def test_timestamp_with_z_suffix(self):
        """Test timestamp parsing with Z suffix."""

        normalizer = POSNormalizer(customer_id="test")

        data = [
            {
                "order_id": "ORD-200",
                "total": 50.00,
                "created_at": "2025-01-15T10:30:00Z",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        # Z suffix creates a timezone-aware datetime
        assert conversions[0].timestamp == datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_timestamp_various_formats(self):
        """Test timestamp parsing with various formats."""
        normalizer = POSNormalizer(customer_id="test")

        data = [
            {"order_id": "ORD-1", "total": 10.00, "created_at": "2025-01-15T10:30:00"},
            {"order_id": "ORD-2", "total": 20.00, "created_at": "2025-01-15T10:30:00Z"},
            {"order_id": "ORD-3", "total": 30.00, "created_at": "2025-01-15T10:30:00+00:00"},
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 3
        # All should parse successfully
        for conv in conversions:
            assert isinstance(conv.timestamp, datetime)

    def test_non_string_timestamp(self):
        """Test handling of non-string timestamp values."""
        normalizer = POSNormalizer(customer_id="test")

        timestamp = datetime(2025, 1, 15, 10, 30, 0)
        data = [
            {
                "order_id": "ORD-300",
                "total": 60.00,
                "created_at": timestamp,
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].timestamp == timestamp


class TestCRMNormalizer:
    """Test CRMNormalizer."""

    def test_normalize_crm_lead(self):
        """Test normalizing a CRM lead."""
        normalizer = CRMNormalizer(customer_id="test_company")

        data = [
            {
                "lead_id": "LEAD-123",
                "amount": 5000.00,
                "created_date": "2025-01-15T10:30:00",
                "contact_id": "CONTACT-456",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.customer_id == "test_company"
        assert conv.transaction_id == "LEAD-123"
        assert conv.value == 5000.00
        assert conv.user_id == "CONTACT-456"
        assert conv.source == ConversionSource.CRM
        assert conv.conversion_type == ConversionType.LEAD

    def test_custom_conversion_type(self):
        """Test setting custom conversion type."""
        normalizer = CRMNormalizer(
            customer_id="test",
            conversion_type=ConversionType.SUBSCRIPTION,
        )

        data = [
            {
                "opportunity_id": "OPP-789",
                "amount": 1200.00,
                "close_date": "2025-01-20T15:00:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].conversion_type == ConversionType.SUBSCRIPTION

    def test_utm_tracking_preservation(self):
        """Test that UTM parameters are preserved."""
        normalizer = CRMNormalizer(customer_id="test")

        data = [
            {
                "lead_id": "LEAD-999",
                "amount": 3000.00,
                "created_date": "2025-01-15T10:30:00",
                "utm_source": "google",
                "utm_medium": "cpc",
                "utm_campaign": "q1_leads",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.utm_source == "google"
        assert conv.utm_medium == "cpc"
        assert conv.utm_campaign == "q1_leads"

    def test_click_id_preservation(self):
        """Test that click IDs (gclid, fbclid) are preserved."""
        normalizer = CRMNormalizer(customer_id="test")

        data = [
            {
                "lead_id": "LEAD-111",
                "amount": 2500.00,
                "created_date": "2025-01-15T10:30:00",
                "gclid": "google_click_123",
                "fbclid": "facebook_click_456",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.gclid == "google_click_123"
        assert conv.fbclid == "facebook_click_456"

    def test_default_field_mappings(self):
        """Test default field mappings for various CRM systems."""
        normalizer = CRMNormalizer(customer_id="test")

        # Test each variant separately to avoid pandas NaN issues
        data1 = [{"opportunity_id": "OPP-1", "opportunity_amount": 100.00, "close_date": "2025-01-01T10:00:00"}]
        data2 = [{"deal_id": "DEAL-2", "deal_value": 200.00, "conversion_date": "2025-01-02T10:00:00"}]
        data3 = [{"lead_id": "LEAD-3", "amount": 300.00, "created_date": "2025-01-03T10:00:00"}]

        conv1 = normalizer.normalize(data1)
        conv2 = normalizer.normalize(data2)
        conv3 = normalizer.normalize(data3)

        assert len(conv1) == 1
        assert conv1[0].transaction_id == "OPP-1"
        assert conv1[0].value == 100.00

        assert len(conv2) == 1
        assert conv2[0].transaction_id == "DEAL-2"
        assert conv2[0].value == 200.00

        assert len(conv3) == 1
        assert conv3[0].transaction_id == "LEAD-3"
        assert conv3[0].value == 300.00

    def test_custom_field_mapping(self):
        """Test custom field mapping for CRM."""
        normalizer = CRMNormalizer(
            customer_id="test",
            field_map={
                "pipeline_id": "transaction_id",
                "revenue": "value",
                "signup_date": "timestamp",
            }
        )

        data = [
            {
                "pipeline_id": "PIPE-123",
                "revenue": 7500.00,
                "signup_date": "2025-01-25T12:00:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.transaction_id == "PIPE-123"
        assert conv.value == 7500.00

    def test_empty_data_returns_empty_list(self):
        """Test that empty data returns empty list."""
        normalizer = CRMNormalizer(customer_id="test")

        conversions = normalizer.normalize([])

        assert conversions == []

    def test_raw_data_preservation(self):
        """Test that raw data is preserved."""
        normalizer = CRMNormalizer(customer_id="test")

        data = [
            {
                "lead_id": "LEAD-555",
                "amount": 1500.00,
                "created_date": "2025-01-15T10:30:00",
                "industry": "technology",
                "lead_source": "webinar",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.raw_data["industry"] == "technology"
        assert conv.raw_data["lead_source"] == "webinar"


class TestLoyaltyNormalizer:
    """Test LoyaltyNormalizer."""

    def test_normalize_redemption(self):
        """Test normalizing a loyalty redemption."""
        normalizer = LoyaltyNormalizer(customer_id="test_loyalty")

        data = [
            {
                "member_id": "MEM-123",
                "redemption_id": "RED-456",
                "reward_value": 25.00,
                "redemption_date": "2025-01-15T10:30:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.customer_id == "test_loyalty"
        assert conv.user_id == "MEM-123"
        assert conv.transaction_id == "RED-456"
        assert conv.value == 25.00
        assert conv.source == ConversionSource.LOYALTY
        assert conv.conversion_type == ConversionType.PURCHASE  # Redemption detected

    def test_signup_detection(self):
        """Test that signup events are detected from data content."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        data = [
            {
                "member_id": "MEM-789",
                "transaction_id": "SIGNUP-999",
                "created_at": "2025-01-15T10:30:00",
                "event_type": "member_signup",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].conversion_type == ConversionType.SIGNUP

    def test_member_id_mapping(self):
        """Test that member_id maps to user_id."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        data = [
            {
                "loyalty_id": "LOY-111",
                "transaction_id": "TXN-222",
                "points_value": 100.00,
                "created_at": "2025-01-15T10:30:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].user_id == "LOY-111"

    def test_custom_field_mapping(self):
        """Test custom field mapping for loyalty."""
        normalizer = LoyaltyNormalizer(
            customer_id="test",
            field_map={
                "user_id": "user_id",
                "activity_id": "transaction_id",
                "cash_value": "value",
                "activity_date": "timestamp",
            }
        )

        data = [
            {
                "user_id": "USER-333",
                "activity_id": "ACT-444",
                "cash_value": 50.00,
                "activity_date": "2025-01-20T14:00:00",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.user_id == "USER-333"
        assert conv.transaction_id == "ACT-444"
        assert conv.value == 50.00

    def test_conversion_type_detection_custom(self):
        """Test that events without signup or redemption are marked CUSTOM."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        data = [
            {
                "member_id": "MEM-555",
                "transaction_id": "TXN-666",
                "points_value": 10.00,
                "created_at": "2025-01-15T10:30:00",
                "event_type": "points_earned",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].conversion_type == ConversionType.CUSTOM

    def test_empty_data_returns_empty_list(self):
        """Test that empty data returns empty list."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        conversions = normalizer.normalize([])

        assert conversions == []

    def test_raw_data_preservation(self):
        """Test that raw data is preserved."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        data = [
            {
                "member_id": "MEM-777",
                "transaction_id": "TXN-888",
                "reward_value": 15.00,
                "redemption_date": "2025-01-15T10:30:00",
                "reward_name": "Free Coffee",
                "tier": "Gold",
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.raw_data["reward_name"] == "Free Coffee"
        assert conv.raw_data["tier"] == "Gold"

    def test_default_field_mappings(self):
        """Test default field mappings."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        # Test each variant separately to avoid pandas NaN issues
        data1 = [{"member_id": "MEM-1", "transaction_id": "TXN-1", "points_value": 10.00, "created_at": "2025-01-01T10:00:00"}]
        data2 = [{"loyalty_id": "LOY-2", "redemption_id": "RED-2", "reward_value": 20.00, "redemption_date": "2025-01-02T10:00:00"}]

        conv1 = normalizer.normalize(data1)
        conv2 = normalizer.normalize(data2)

        assert len(conv1) == 1
        assert conv1[0].user_id == "MEM-1"
        assert conv1[0].value == 10.00

        assert len(conv2) == 1
        assert conv2[0].user_id == "LOY-2"
        assert conv2[0].value == 20.00

    def test_missing_optional_fields(self):
        """Test that missing optional fields are handled gracefully."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        data = [
            {
                "member_id": "MEM-999",
                # Missing transaction_id, value, timestamp
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        assert conv.user_id == "MEM-999"
        assert conv.value == 0.0
        assert isinstance(conv.timestamp, datetime)

    def test_null_values_handled(self):
        """Test that null values are handled gracefully."""
        normalizer = LoyaltyNormalizer(customer_id="test")

        # Use empty dict for missing values instead of None to test realistic case
        # In practice, pandas DataFrame will have NaN for missing values
        data = [
            {
                "member_id": "MEM-000",
                "created_at": "2025-01-15T10:30:00",
                # transaction_id and points_value not present
            }
        ]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        conv = conversions[0]
        # Should handle missing values without crashing
        assert conv.value == 0.0
        assert conv.user_id == "MEM-000"
