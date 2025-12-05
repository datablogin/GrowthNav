"""Tests for ConnectorStorage."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from growthnav.connectors import (
    ConnectorConfig,
    ConnectorType,
    SyncMode,
    SyncSchedule,
)


@pytest.fixture
def sample_connector_config() -> ConnectorConfig:
    """Create a sample connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.SNOWFLAKE,
        customer_id="test_customer",
        name="Test Snowflake Connector",
        connection_params={
            "account": "test.snowflakecomputing.com",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
        },
        credentials_secret_path="growthnav-test-connector-snowflake",
        field_overrides={"SALE_ID": "transaction_id"},
        sync_mode=SyncMode.INCREMENTAL,
        sync_schedule=SyncSchedule.DAILY,
    )


class TestConnectorStorage:
    """Tests for ConnectorStorage class."""

    def test_table_id_property(self):
        """Test table_id property returns correct table reference."""
        from growthnav.connectors.storage import ConnectorStorage

        storage = ConnectorStorage(project_id="my-project")
        assert storage.table_id == "my-project.growthnav_registry.connectors"

    def test_client_lazy_initialization(self):
        """Test BigQuery client is lazily initialized."""
        from growthnav.connectors.storage import ConnectorStorage

        with patch("google.cloud.bigquery.Client") as mock_client:
            storage = ConnectorStorage(project_id="my-project")

            # Client not created yet
            mock_client.assert_not_called()

            # Access client
            _ = storage.client

            # Now it should be created
            mock_client.assert_called_once_with(project="my-project")

    def test_client_uses_provided_instance(self):
        """Test that provided client is used instead of creating new one."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        assert storage.client is mock_client

    def test_save_generates_uuid_when_not_provided(self, sample_connector_config):
        """Test save generates connector_id when not provided."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        with patch("growthnav.connectors.storage.uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = "test-uuid-123"
            connector_id = storage.save(sample_connector_config)

        assert connector_id == "test-uuid-123"

    def test_save_uses_provided_connector_id(self, sample_connector_config):
        """Test save uses provided connector_id."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        connector_id = storage.save(sample_connector_config, connector_id="custom-id")

        assert connector_id == "custom-id"

    def test_save_calls_query_with_parameters(self, sample_connector_config):
        """Test save constructs proper parameterized query."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        storage.save(sample_connector_config, connector_id="test-id")

        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args

        # Verify query contains MERGE statement
        sql = call_args[0][0]
        assert "MERGE" in sql
        assert "growthnav_registry.connectors" in sql

    def test_get_returns_none_when_not_found(self):
        """Test get returns None when connector not found."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([])

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        result = storage.get("nonexistent-id")

        assert result is None

    def test_get_returns_config_when_found(self):
        """Test get returns ConnectorConfig when found."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_row = {
            "connector_id": "test-id",
            "customer_id": "test_customer",
            "connector_type": "snowflake",
            "name": "Test Connector",
            "connection_params": '{"account": "test.snowflakecomputing.com"}',
            "field_overrides": "{}",
            "sync_mode": "incremental",
            "sync_schedule": "daily",
            "last_sync": None,
            "last_sync_cursor": None,
            "credentials_secret_path": None,
            "is_active": True,
            "error_message": None,
        }

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([mock_row])

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        result = storage.get("test-id")

        assert result is not None
        assert result.connector_type == ConnectorType.SNOWFLAKE
        assert result.customer_id == "test_customer"
        assert result.name == "Test Connector"

    def test_list_for_customer_returns_configs(self):
        """Test list_for_customer returns list of configs."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_rows = [
            {
                "connector_id": "id-1",
                "customer_id": "test_customer",
                "connector_type": "snowflake",
                "name": "Connector 1",
                "connection_params": "{}",
                "field_overrides": "{}",
                "sync_mode": "incremental",
                "sync_schedule": "daily",
                "is_active": True,
            },
            {
                "connector_id": "id-2",
                "customer_id": "test_customer",
                "connector_type": "salesforce",
                "name": "Connector 2",
                "connection_params": "{}",
                "field_overrides": "{}",
                "sync_mode": "full",
                "sync_schedule": "weekly",
                "is_active": True,
            },
        ]

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter(mock_rows)

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        results = storage.list_for_customer("test_customer")

        assert len(results) == 2
        assert results[0].name == "Connector 1"
        assert results[1].name == "Connector 2"

    def test_list_for_customer_filters_active_by_default(self):
        """Test list_for_customer filters by is_active by default."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([])

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        storage.list_for_customer("test_customer")

        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        assert "is_active = TRUE" in sql

    def test_list_for_customer_can_include_inactive(self):
        """Test list_for_customer can include inactive connectors."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([])

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        storage.list_for_customer("test_customer", active_only=False)

        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        assert "is_active = TRUE" not in sql

    def test_delete_returns_true_when_deleted(self):
        """Test delete returns True when connector is deleted."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 1

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = mock_result

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        result = storage.delete("test-id")

        assert result is True

    def test_delete_returns_false_when_not_found(self):
        """Test delete returns False when connector not found."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 0

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = mock_result

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        result = storage.delete("nonexistent-id")

        assert result is False

    def test_deactivate_sets_inactive_and_error_message(self):
        """Test deactivate sets is_active=False and error_message."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 1

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = mock_result

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        result = storage.deactivate("test-id", error_message="Connection failed")

        assert result is True
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        assert "is_active = FALSE" in sql

    def test_update_sync_status(self):
        """Test update_sync_status updates sync fields."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_result = MagicMock()
        mock_result.num_dml_affected_rows = 1

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = mock_result

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        last_sync = datetime.now(UTC)
        result = storage.update_sync_status(
            connector_id="test-id",
            last_sync=last_sync,
            cursor="2024-01-15T00:00:00Z",
        )

        assert result is True
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        assert "last_sync" in sql
        assert "last_sync_cursor" in sql

    def test_ensure_table_exists_calls_create(self):
        """Test ensure_table_exists executes CREATE TABLE IF NOT EXISTS."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None

        storage = ConnectorStorage(project_id="my-project", client=mock_client)

        storage.ensure_table_exists()

        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "growthnav_registry.connectors" in sql


class TestRowToConfig:
    """Test _row_to_config conversion method."""

    def test_handles_json_string_connection_params(self):
        """Test conversion handles JSON string for connection_params."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_row = {
            "connector_id": "test-id",
            "customer_id": "test_customer",
            "connector_type": "snowflake",
            "name": "Test",
            "connection_params": '{"account": "test.com"}',
            "field_overrides": "{}",
            "sync_mode": "incremental",
            "sync_schedule": "daily",
            "is_active": True,
        }

        storage = ConnectorStorage(project_id="my-project", client=MagicMock())
        config = storage._row_to_config(mock_row)

        assert config.connection_params == {"account": "test.com"}

    def test_handles_none_connection_params(self):
        """Test conversion handles None for connection_params."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_row = {
            "connector_id": "test-id",
            "customer_id": "test_customer",
            "connector_type": "snowflake",
            "name": "Test",
            "connection_params": None,
            "field_overrides": None,
            "sync_mode": "incremental",
            "sync_schedule": "daily",
            "is_active": True,
        }

        storage = ConnectorStorage(project_id="my-project", client=MagicMock())
        config = storage._row_to_config(mock_row)

        assert config.connection_params == {}
        assert config.field_overrides == {}

    def test_handles_dict_connection_params(self):
        """Test conversion handles dict for connection_params (already parsed)."""
        from growthnav.connectors.storage import ConnectorStorage

        mock_row = {
            "connector_id": "test-id",
            "customer_id": "test_customer",
            "connector_type": "snowflake",
            "name": "Test",
            "connection_params": {"account": "test.com"},  # Already a dict
            "field_overrides": {},
            "sync_mode": "incremental",
            "sync_schedule": "daily",
            "is_active": True,
        }

        storage = ConnectorStorage(project_id="my-project", client=MagicMock())
        config = storage._row_to_config(mock_row)

        assert config.connection_params == {"account": "test.com"}
