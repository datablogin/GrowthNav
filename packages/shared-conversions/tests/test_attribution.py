"""Tests for attribution logic."""

from datetime import datetime

from growthnav.conversions.attribution import (
    AdClick,
    AttributionResult,
    attribute_conversions,
)
from growthnav.conversions.schema import (
    AttributionModel,
    Conversion,
)


class TestAdClick:
    """Test AdClick dataclass."""

    def test_create_ad_click(self):
        """Test creating an AdClick."""
        timestamp = datetime(2025, 1, 15, 10, 0, 0)
        click = AdClick(
            platform="google_ads",
            click_id="gclid_123",
            campaign_id="CAMP-456",
            ad_id="AD-789",
            timestamp=timestamp,
            user_id="USER-001",
        )

        assert click.platform == "google_ads"
        assert click.click_id == "gclid_123"
        assert click.campaign_id == "CAMP-456"
        assert click.ad_id == "AD-789"
        assert click.timestamp == timestamp
        assert click.user_id == "USER-001"


class TestAttributionResult:
    """Test AttributionResult dataclass."""

    def test_create_attribution_result(self):
        """Test creating an AttributionResult."""
        conversion = Conversion(customer_id="test")
        result = AttributionResult(
            conversion=conversion,
            attributed=True,
            platform="google_ads",
            campaign_id="CAMP-123",
            ad_id="AD-456",
            model=AttributionModel.LAST_CLICK,
            weight=1.0,
        )

        assert result.conversion == conversion
        assert result.attributed is True
        assert result.platform == "google_ads"
        assert result.campaign_id == "CAMP-123"
        assert result.ad_id == "AD-456"
        assert result.model == AttributionModel.LAST_CLICK
        assert result.weight == 1.0
        assert result.touchpoints == []

    def test_touchpoints_default_initialization(self):
        """Test that touchpoints defaults to empty list."""
        conversion = Conversion(customer_id="test")
        result = AttributionResult(
            conversion=conversion,
            attributed=False,
        )

        assert result.touchpoints == []


class TestAttributeConversions:
    """Test attribute_conversions function."""

    def test_no_matching_clicks(self):
        """Test conversion with no matching clicks."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="different_gclid",
                timestamp=datetime(2025, 1, 15, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is False
        assert results[0].platform is None

    def test_last_click_attribution(self):
        """Test last-click attribution model."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_123",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_123",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
            AdClick(
                platform="meta",
                click_id="gclid_123",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 15, 9, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.LAST_CLICK,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.platform == "meta"  # Last click
        assert result.campaign_id == "CAMP-B"
        assert result.model == AttributionModel.LAST_CLICK
        assert result.weight == 1.0
        assert len(result.touchpoints) == 2

    def test_first_click_attribution(self):
        """Test first-click attribution model."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_456",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_456",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
            AdClick(
                platform="meta",
                click_id="gclid_456",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 15, 9, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.FIRST_CLICK,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.platform == "google_ads"  # First click
        assert result.campaign_id == "CAMP-A"
        assert result.model == AttributionModel.FIRST_CLICK
        assert result.weight == 1.0

    def test_linear_attribution(self):
        """Test linear attribution model."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_789",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_789",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 13, 10, 0, 0),
            ),
            AdClick(
                platform="meta",
                click_id="gclid_789",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 14, 9, 0, 0),
            ),
            AdClick(
                platform="reddit",
                click_id="gclid_789",
                campaign_id="CAMP-C",
                timestamp=datetime(2025, 1, 15, 8, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.LINEAR,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.model == AttributionModel.LINEAR
        # Linear splits credit equally: 1/3 for each touchpoint
        assert abs(result.weight - 1.0 / 3.0) < 0.001
        assert len(result.touchpoints) == 3

    def test_time_decay_attribution(self):
        """Test time-decay attribution model."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_999",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_999",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 1, 10, 0, 0),  # 14 days before
            ),
            AdClick(
                platform="meta",
                click_id="gclid_999",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),  # 1 day before
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.TIME_DECAY,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.model == AttributionModel.TIME_DECAY
        # More recent click should get more credit
        assert result.platform == "meta"
        assert result.weight > 0.5  # More than half credit

    def test_position_based_attribution_single_click(self):
        """Test position-based attribution with single click."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            fbclid="fbclid_111",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="meta",
                click_id="fbclid_111",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.POSITION_BASED,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.weight == 1.0  # Single click gets full credit

    def test_position_based_attribution_two_clicks(self):
        """Test position-based attribution with two clicks."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            fbclid="fbclid_222",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="meta",
                click_id="fbclid_222",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 13, 10, 0, 0),
            ),
            AdClick(
                platform="google_ads",
                click_id="fbclid_222",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.POSITION_BASED,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        assert result.weight == 0.5  # 50/50 split for two clicks

    def test_position_based_attribution_many_clicks(self):
        """Test position-based attribution with 3+ clicks."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_333",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_333",
                campaign_id="CAMP-A",
                timestamp=datetime(2025, 1, 12, 10, 0, 0),
            ),
            AdClick(
                platform="meta",
                click_id="gclid_333",
                campaign_id="CAMP-B",
                timestamp=datetime(2025, 1, 13, 10, 0, 0),
            ),
            AdClick(
                platform="reddit",
                click_id="gclid_333",
                campaign_id="CAMP-C",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            model=AttributionModel.POSITION_BASED,
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        # Last click gets 40% in position-based
        assert result.weight == 0.4

    def test_lookback_window_filters_old_clicks(self):
        """Test that lookback window filters out old clicks."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_old",
            timestamp=datetime(2025, 2, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_old",
                campaign_id="CAMP-OLD",
                timestamp=datetime(2025, 1, 1, 10, 0, 0),  # 45 days before
            ),
            AdClick(
                platform="meta",
                click_id="gclid_old",
                campaign_id="CAMP-RECENT",
                timestamp=datetime(2025, 2, 10, 10, 0, 0),  # 5 days before
            ),
        ]

        results = attribute_conversions(
            [conversion],
            clicks,
            lookback_days=30,  # 30-day window
        )

        assert len(results) == 1
        result = results[0]
        assert result.attributed is True
        # Only recent click should be matched
        assert result.platform == "meta"
        assert len(result.touchpoints) == 1

    def test_clicks_after_conversion_ignored(self):
        """Test that clicks after conversion are ignored."""
        conversion = Conversion(
            customer_id="test",
            transaction_id="TXN-001",
            gclid="gclid_future",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_future",
                campaign_id="CAMP-BEFORE",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),  # Before conversion
            ),
            AdClick(
                platform="meta",
                click_id="gclid_future",
                campaign_id="CAMP-AFTER",
                timestamp=datetime(2025, 1, 16, 10, 0, 0),  # After conversion
            ),
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        result = results[0]
        # Only the click before conversion should be matched
        assert len(result.touchpoints) == 1
        assert result.platform == "google_ads"

    def test_match_by_gclid(self):
        """Test matching clicks by gclid."""
        conversion = Conversion(
            customer_id="test",
            gclid="test_gclid",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="test_gclid",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is True
        assert results[0].platform == "google_ads"

    def test_match_by_fbclid(self):
        """Test matching clicks by fbclid."""
        conversion = Conversion(
            customer_id="test",
            fbclid="test_fbclid",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="meta",
                click_id="test_fbclid",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is True
        assert results[0].platform == "meta"

    def test_match_by_ttclid(self):
        """Test matching clicks by ttclid (TikTok)."""
        conversion = Conversion(
            customer_id="test",
            ttclid="test_ttclid",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="tiktok",
                click_id="test_ttclid",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is True
        assert results[0].platform == "tiktok"

    def test_match_by_msclkid(self):
        """Test matching clicks by msclkid (Microsoft)."""
        conversion = Conversion(
            customer_id="test",
            msclkid="test_msclkid",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="microsoft_ads",
                click_id="test_msclkid",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is True
        assert results[0].platform == "microsoft_ads"

    def test_match_by_user_id_fallback(self):
        """Test matching clicks by user_id as fallback."""
        conversion = Conversion(
            customer_id="test",
            user_id="USER-123",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="some_click_id",
                user_id="USER-123",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].attributed is True
        assert results[0].platform == "google_ads"

    def test_multiple_conversions(self):
        """Test attributing multiple conversions."""
        conversions = [
            Conversion(
                customer_id="test",
                transaction_id="TXN-001",
                gclid="gclid_1",
                timestamp=datetime(2025, 1, 15, 12, 0, 0),
            ),
            Conversion(
                customer_id="test",
                transaction_id="TXN-002",
                fbclid="fbclid_2",
                timestamp=datetime(2025, 1, 16, 12, 0, 0),
            ),
        ]

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_1",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            ),
            AdClick(
                platform="meta",
                click_id="fbclid_2",
                timestamp=datetime(2025, 1, 15, 10, 0, 0),
            ),
        ]

        results = attribute_conversions(conversions, clicks)

        assert len(results) == 2
        assert results[0].attributed is True
        assert results[0].platform == "google_ads"
        assert results[1].attributed is True
        assert results[1].platform == "meta"

    def test_conversion_object_updated_with_attribution(self):
        """Test that the Conversion object is updated with attribution data."""
        conversion = Conversion(
            customer_id="test",
            gclid="gclid_test",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_test",
                campaign_id="CAMP-999",
                ad_id="AD-888",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        attribute_conversions([conversion], clicks)

        # Check that the original conversion object was updated
        assert conversion.attributed_platform == "google_ads"
        assert conversion.attributed_campaign_id == "CAMP-999"
        assert conversion.attributed_ad_id == "AD-888"
        assert conversion.attribution_model == AttributionModel.LAST_CLICK
        assert conversion.attribution_weight == 1.0

    def test_default_model_is_last_click(self):
        """Test that default attribution model is last-click."""
        conversion = Conversion(
            customer_id="test",
            gclid="gclid_default",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_default",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        # Don't specify model - should default to LAST_CLICK
        results = attribute_conversions([conversion], clicks)

        assert len(results) == 1
        assert results[0].model == AttributionModel.LAST_CLICK

    def test_empty_conversions_list(self):
        """Test with empty conversions list."""
        clicks = [
            AdClick(
                platform="google_ads",
                click_id="gclid_test",
                timestamp=datetime(2025, 1, 14, 10, 0, 0),
            )
        ]

        results = attribute_conversions([], clicks)

        assert results == []

    def test_empty_clicks_list(self):
        """Test with empty clicks list."""
        conversion = Conversion(
            customer_id="test",
            gclid="gclid_test",
            timestamp=datetime(2025, 1, 15, 12, 0, 0),
        )

        results = attribute_conversions([conversion], [])

        assert len(results) == 1
        assert results[0].attributed is False
