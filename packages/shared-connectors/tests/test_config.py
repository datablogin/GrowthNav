"""Tests for growthnav.connectors.config."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from growthnav.connectors.config import (
    ConnectorConfig,
    ConnectorType,
    SyncMode,
    SyncResult,
    SyncSchedule,
)


class TestConnectorType:
    """Tests for ConnectorType enum."""

    def test_data_lake_types(self) -> None:
        """Test data lake connector types."""
        assert ConnectorType.SNOWFLAKE.value == "snowflake"
        assert ConnectorType.BIGQUERY.value == "bigquery"

    def test_pos_types(self) -> None:
        """Test POS connector types."""
        assert ConnectorType.TOAST.value == "toast"
        assert ConnectorType.SQUARE.value == "square"
        assert ConnectorType.CLOVER.value == "clover"
        assert ConnectorType.LIGHTSPEED.value == "lightspeed"

    def test_crm_types(self) -> None:
        """Test CRM connector types."""
        assert ConnectorType.SALESFORCE.value == "salesforce"
        assert ConnectorType.HUBSPOT.value == "hubspot"
        assert ConnectorType.ZOHO.value == "zoho"

    def test_olo_types(self) -> None:
        """Test OLO connector types."""
        assert ConnectorType.OLO.value == "olo"
        assert ConnectorType.OTTER.value == "otter"
        assert ConnectorType.CHOWLY.value == "chowly"

    def test_loyalty_types(self) -> None:
        """Test loyalty connector types."""
        assert ConnectorType.FISHBOWL.value == "fishbowl"
        assert ConnectorType.PUNCHH.value == "punchh"


class TestSyncMode:
    """Tests for SyncMode enum."""

    def test_full_mode(self) -> None:
        """Test full sync mode."""
        assert SyncMode.FULL.value == "full"

    def test_incremental_mode(self) -> None:
        """Test incremental sync mode."""
        assert SyncMode.INCREMENTAL.value == "incremental"


class TestSyncSchedule:
    """Tests for SyncSchedule enum."""

    def test_schedule_values(self) -> None:
        """Test all schedule values."""
        assert SyncSchedule.HOURLY.value == "hourly"
        assert SyncSchedule.DAILY.value == "daily"
        assert SyncSchedule.WEEKLY.value == "weekly"
        assert SyncSchedule.MANUAL.value == "manual"


class TestConnectorConfig:
    """Tests for ConnectorConfig dataclass."""

    def test_minimal_config(self) -> None:
        """Test creating a config with minimal required fields."""
        config = ConnectorConfig(
            connector_type=ConnectorType.SNOWFLAKE,
            customer_id="test_customer",
            name="Test Connection",
        )

        assert config.connector_type == ConnectorType.SNOWFLAKE
        assert config.customer_id == "test_customer"
        assert config.name == "Test Connection"

    def test_default_values(self) -> None:
        """Test default values are set correctly."""
        config = ConnectorConfig(
            connector_type=ConnectorType.SALESFORCE,
            customer_id="acme",
            name="Salesforce CRM",
        )

        assert config.credentials_secret_path is None
        assert config.credentials == {}
        assert config.connection_params == {}
        assert config.field_overrides == {}
        assert config.sync_mode == SyncMode.INCREMENTAL
        assert config.sync_schedule == SyncSchedule.DAILY
        assert config.last_sync is None
        assert config.last_sync_cursor is None
        assert config.is_active is True
        assert config.error_message is None

    def test_full_config(self) -> None:
        """Test creating a fully specified config."""
        last_sync = datetime.now(UTC)

        config = ConnectorConfig(
            connector_type=ConnectorType.HUBSPOT,
            customer_id="medical_spa",
            name="HubSpot CRM",
            credentials_secret_path="projects/my-project/secrets/hubspot-key",
            credentials={"api_key": "test_key"},
            connection_params={"portal_id": "12345"},
            field_overrides={"email_address": "email"},
            sync_mode=SyncMode.FULL,
            sync_schedule=SyncSchedule.WEEKLY,
            last_sync=last_sync,
            last_sync_cursor="cursor_abc",
            is_active=False,
            error_message="Connection failed",
        )

        assert config.connector_type == ConnectorType.HUBSPOT
        assert config.customer_id == "medical_spa"
        assert config.credentials["api_key"] == "test_key"
        assert config.sync_mode == SyncMode.FULL
        assert config.sync_schedule == SyncSchedule.WEEKLY
        assert config.last_sync == last_sync
        assert config.is_active is False
        assert config.error_message == "Connection failed"


class TestSyncResult:
    """Tests for SyncResult dataclass."""

    def test_minimal_result(self) -> None:
        """Test creating a result with minimal fields."""
        started = datetime.now(UTC)

        result = SyncResult(
            connector_name="Test Connector",
            customer_id="test_customer",
            sync_mode=SyncMode.FULL,
            started_at=started,
        )

        assert result.connector_name == "Test Connector"
        assert result.customer_id == "test_customer"
        assert result.sync_mode == SyncMode.FULL
        assert result.started_at == started
        assert result.completed_at is None
        assert result.records_fetched == 0
        assert result.records_normalized == 0
        assert result.records_failed == 0
        assert result.success is False
        assert result.error is None
        assert result.cursor is None

    def test_successful_result(self) -> None:
        """Test a successful sync result."""
        started = datetime.now(UTC)
        completed = started + timedelta(seconds=30)

        result = SyncResult(
            connector_name="Snowflake",
            customer_id="topgolf",
            sync_mode=SyncMode.INCREMENTAL,
            started_at=started,
            completed_at=completed,
            records_fetched=1000,
            records_normalized=998,
            records_failed=2,
            success=True,
            cursor="2024-01-15T10:30:00Z",
        )

        assert result.success is True
        assert result.records_fetched == 1000
        assert result.records_normalized == 998
        assert result.records_failed == 2
        assert result.cursor == "2024-01-15T10:30:00Z"

    def test_failed_result(self) -> None:
        """Test a failed sync result."""
        started = datetime.now(UTC)
        completed = started + timedelta(seconds=5)

        result = SyncResult(
            connector_name="HubSpot",
            customer_id="acme",
            sync_mode=SyncMode.FULL,
            started_at=started,
            completed_at=completed,
            success=False,
            error="Authentication failed: Invalid API key",
        )

        assert result.success is False
        assert result.error == "Authentication failed: Invalid API key"
        assert result.records_fetched == 0

    def test_duration_seconds(self) -> None:
        """Test duration calculation."""
        started = datetime.now(UTC)
        completed = started + timedelta(seconds=45)

        result = SyncResult(
            connector_name="Test",
            customer_id="test",
            sync_mode=SyncMode.FULL,
            started_at=started,
            completed_at=completed,
        )

        assert result.duration_seconds == 45.0

    def test_duration_seconds_incomplete(self) -> None:
        """Test duration is None when sync not complete."""
        result = SyncResult(
            connector_name="Test",
            customer_id="test",
            sync_mode=SyncMode.FULL,
            started_at=datetime.now(UTC),
        )

        assert result.duration_seconds is None
