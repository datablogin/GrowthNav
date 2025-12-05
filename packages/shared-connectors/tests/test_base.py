"""Tests for growthnav.connectors.base."""

from __future__ import annotations

from datetime import UTC, datetime

from growthnav.connectors.config import (
    ConnectorType,
    SyncMode,
)


class TestBaseConnector:
    """Tests for BaseConnector abstract class."""

    def test_init(self, mock_connector, connector_config) -> None:
        """Test connector initialization."""
        assert mock_connector.config == connector_config
        assert mock_connector._client is None
        assert mock_connector._authenticated is False

    def test_is_authenticated_false_initially(self, mock_connector) -> None:
        """Test is_authenticated is False before authentication."""
        assert mock_connector.is_authenticated is False

    def test_is_authenticated_true_after_auth(self, mock_connector) -> None:
        """Test is_authenticated is True after authentication."""
        mock_connector.authenticate()
        assert mock_connector.is_authenticated is True

    def test_authenticate(self, mock_connector) -> None:
        """Test authentication sets client and flag."""
        mock_connector.authenticate()

        assert mock_connector._authenticated is True
        assert mock_connector._client == "mock_client"

    def test_test_connection_success(self, mock_connector) -> None:
        """Test successful connection test."""
        result = mock_connector.test_connection()
        assert result is True
        assert mock_connector.is_authenticated is True

    def test_test_connection_failure(self, mock_connector) -> None:
        """Test failed connection test."""
        # Make authenticate raise an exception
        def failing_auth():
            raise ConnectionError("Cannot connect")

        mock_connector.authenticate = failing_auth

        result = mock_connector.test_connection()
        assert result is False

    def test_fetch_records_empty(self, mock_connector) -> None:
        """Test fetching with no records."""
        mock_connector.authenticate()
        records = list(mock_connector.fetch_records())
        assert records == []

    def test_fetch_records_with_data(self, mock_connector) -> None:
        """Test fetching with mock data."""
        mock_connector.authenticate()
        mock_connector.set_mock_records([
            {"id": 1, "amount": 100.0},
            {"id": 2, "amount": 200.0},
        ])

        records = list(mock_connector.fetch_records())

        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["amount"] == 200.0

    def test_fetch_records_with_limit(self, mock_connector) -> None:
        """Test fetching with limit."""
        mock_connector.authenticate()
        mock_connector.set_mock_records([
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ])

        records = list(mock_connector.fetch_records(limit=2))

        assert len(records) == 2

    def test_get_schema(self, mock_connector) -> None:
        """Test getting schema."""
        mock_connector.set_mock_schema({
            "id": "INTEGER",
            "amount": "DECIMAL",
            "created_at": "TIMESTAMP",
        })

        schema = mock_connector.get_schema()

        assert schema["id"] == "INTEGER"
        assert schema["amount"] == "DECIMAL"
        assert len(schema) == 3

    def test_normalize(self, mock_connector) -> None:
        """Test normalization returns records (mock implementation)."""
        records = [{"id": 1}, {"id": 2}]
        normalized = mock_connector.normalize(records)

        assert normalized == records

    def test_sync_success(self, mock_connector) -> None:
        """Test successful sync."""
        mock_connector.set_mock_records([
            {"id": 1, "amount": 100.0},
            {"id": 2, "amount": 200.0},
            {"id": 3, "amount": 300.0},
        ])

        result = mock_connector.sync()

        assert result.success is True
        assert result.records_fetched == 3
        assert result.records_normalized == 3
        assert result.records_failed == 0
        assert result.connector_name == "Test Connector"
        assert result.customer_id == "test_customer"
        assert result.completed_at is not None
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0

    def test_sync_incremental_uses_last_sync(self, connector_config, mock_connector) -> None:
        """Test incremental sync uses config.last_sync."""
        last_sync = datetime(2024, 1, 1, tzinfo=UTC)
        connector_config.last_sync = last_sync
        connector_config.sync_mode = SyncMode.INCREMENTAL

        # Create new connector with updated config using the fixture's class type
        MockConnector = type(mock_connector)
        connector = MockConnector(connector_config)
        connector.set_mock_records([{"id": 1}])

        result = connector.sync()

        assert result.success is True
        assert result.sync_mode == SyncMode.INCREMENTAL

    def test_sync_full_mode(self, mock_connector) -> None:
        """Test full sync mode."""
        mock_connector.set_mock_records([{"id": 1}])

        result = mock_connector.sync(mode=SyncMode.FULL)

        assert result.sync_mode == SyncMode.FULL
        assert result.success is True

    def test_sync_failure(self, mock_connector) -> None:
        """Test sync failure handling."""
        def failing_fetch(*args, **kwargs):
            raise RuntimeError("Database connection lost")

        mock_connector.authenticate()
        mock_connector.fetch_records = failing_fetch

        result = mock_connector.sync()

        assert result.success is False
        assert "Database connection lost" in result.error
        assert result.completed_at is not None

    def test_close(self, mock_connector) -> None:
        """Test closing connection."""
        mock_connector.authenticate()
        assert mock_connector._authenticated is True
        assert mock_connector._client is not None

        mock_connector.close()

        assert mock_connector._authenticated is False
        assert mock_connector._client is None

    def test_connector_type_class_attribute(self, mock_connector) -> None:
        """Test connector_type is accessible."""
        assert mock_connector.connector_type == ConnectorType.SNOWFLAKE

    def test_context_manager(self, mock_connector) -> None:
        """Test connector can be used as context manager."""
        mock_connector.authenticate()
        assert mock_connector._authenticated is True

        with mock_connector as connector:
            assert connector is mock_connector
            assert connector._authenticated is True

        # After exiting context, should be closed
        assert mock_connector._authenticated is False
        assert mock_connector._client is None

    def test_sync_with_batch_processing(self, mock_connector) -> None:
        """Test sync processes records in batches."""
        # Create more records than batch_size
        mock_connector.set_mock_records([{"id": i} for i in range(5)])

        result = mock_connector.sync(batch_size=2)

        assert result.success is True
        assert result.records_fetched == 5
        assert result.records_normalized == 5

    def test_sync_sets_cursor(self, mock_connector) -> None:
        """Test sync sets cursor for incremental sync."""
        mock_connector.set_mock_records([{"id": 1}])

        result = mock_connector.sync()

        assert result.cursor is not None
        # Cursor should be an ISO format datetime string
        datetime.fromisoformat(result.cursor)

    def test_sync_invalid_time_range(self, mock_connector) -> None:
        """Test sync validates time range."""
        since = datetime(2024, 2, 1, tzinfo=UTC)
        until = datetime(2024, 1, 1, tzinfo=UTC)  # Before since

        result = mock_connector.sync(since=since, until=until)

        assert result.success is False
        assert "must be before" in result.error

    def test_init_subclass_missing_connector_type(self) -> None:
        """Test that subclasses without connector_type raise TypeError."""
        import pytest
        from growthnav.connectors.base import BaseConnector

        with pytest.raises(TypeError, match="must define a 'connector_type'"):
            class BadConnector(BaseConnector):
                def authenticate(self) -> None:
                    pass

                def fetch_records(self, since=None, until=None, limit=None):
                    yield from []

                def get_schema(self):
                    return {}

                def normalize(self, raw_records):
                    return raw_records
