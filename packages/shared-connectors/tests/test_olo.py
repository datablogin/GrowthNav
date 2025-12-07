"""Tests for OLOConnector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest
from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode
from growthnav.connectors.exceptions import AuthenticationError


@pytest.fixture
def olo_config() -> ConnectorConfig:
    """Create an OLO connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.OLO,
        customer_id="test_customer",
        name="Test OLO Connector",
        credentials={
            "api_key": "test-api-key-12345",
        },
        connection_params={
            "brand_id": "brand-001",
        },
        sync_mode=SyncMode.FULL,
    )


class TestOLOConnector:
    """Tests for OLOConnector."""

    def test_connector_type(self, olo_config: ConnectorConfig) -> None:
        """Test connector has correct type."""
        from growthnav.connectors.adapters.olo import OLOConnector

        connector = OLOConnector(olo_config)
        assert connector.connector_type == ConnectorType.OLO

    def test_authenticate_success(self, olo_config: ConnectorConfig) -> None:
        """Test successful authentication."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            mock_client_class.assert_called_once_with(
                base_url="https://api.olo.com",
                headers={
                    "Authorization": "Bearer test-api-key-12345",
                    "Content-Type": "application/json",
                },
                timeout=httpx.Timeout(30.0, connect=10.0),
            )

    def test_authenticate_custom_base_url(self, olo_config: ConnectorConfig) -> None:
        """Test authentication with custom base URL."""
        olo_config.connection_params["base_url"] = "https://custom.olo.api.com"

        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            mock_client_class.assert_called_once_with(
                base_url="https://custom.olo.api.com",
                headers={
                    "Authorization": "Bearer test-api-key-12345",
                    "Content-Type": "application/json",
                },
                timeout=httpx.Timeout(30.0, connect=10.0),
            )

    def test_missing_api_key(self, olo_config: ConnectorConfig) -> None:
        """Test that missing api_key raises ValueError."""
        from growthnav.connectors.adapters.olo import OLOConnector

        olo_config.credentials = {}

        with pytest.raises(ValueError, match="OLO connector requires 'api_key'"):
            OLOConnector(olo_config)

    def test_authenticate_failure(self, olo_config: ConnectorConfig) -> None:
        """Test authentication failure raises AuthenticationError."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client_class.side_effect = Exception("Connection refused")

            connector = OLOConnector(olo_config)

            with pytest.raises(AuthenticationError, match="Failed to authenticate"):
                connector.authenticate()

    def test_fetch_records_basic(self, olo_config: ConnectorConfig) -> None:
        """Test fetching order records."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "orders": [
                {
                    "id": "order-001",
                    "order_number": "ORD-12345",
                    "customer_id": "cust-001",
                    "total": 25.99,
                    "created_at": "2024-06-15T12:00:00Z",
                },
                {
                    "id": "order-002",
                    "order_number": "ORD-12346",
                    "customer_id": "cust-002",
                    "total": 35.50,
                    "created_at": "2024-06-15T13:00:00Z",
                },
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert records[0]["id"] == "order-001"
            assert records[0]["total"] == 25.99
            assert records[1]["id"] == "order-002"

    def test_fetch_records_with_brand_filter(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records includes brand_id filter."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            list(connector.fetch_records())

            # Verify brand_id was included in query params
            call_args = mock_client.get.call_args
            assert call_args[1]["params"]["brand_id"] == "brand-001"

    def test_fetch_records_no_brand_filter(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records without brand_id filter."""
        del olo_config.connection_params["brand_id"]

        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            list(connector.fetch_records())

            # Verify brand_id was NOT included in query params
            call_args = mock_client.get.call_args
            assert "brand_id" not in call_args[1]["params"]

    def test_fetch_records_with_time_filter(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records with time filters."""
        from growthnav.connectors.adapters.olo import OLOConnector

        since = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        until = datetime(2024, 6, 30, 23, 59, 59, tzinfo=UTC)

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            list(connector.fetch_records(since=since, until=until))

            call_args = mock_client.get.call_args
            assert "created_after" in call_args[1]["params"]
            assert "created_before" in call_args[1]["params"]

    def test_fetch_records_with_limit(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records with limit."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "orders": [
                {"id": f"order-{i:03d}", "total": 10.00 * i}
                for i in range(10)
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            records = list(connector.fetch_records(limit=5))

            assert len(records) == 5

    def test_fetch_records_with_pagination(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records with pagination."""
        from growthnav.connectors.adapters.olo import OLOConnector

        # First page returns 100 records
        page1_orders = [{"id": f"order-{i:03d}"} for i in range(100)]
        mock_response1 = MagicMock()
        mock_response1.json.return_value = {"orders": page1_orders}
        mock_response1.raise_for_status = MagicMock()

        # Second page returns 50 records (less than page size = last page)
        page2_orders = [{"id": f"order-{100+i:03d}"} for i in range(50)]
        mock_response2 = MagicMock()
        mock_response2.json.return_value = {"orders": page2_orders}
        mock_response2.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.side_effect = [mock_response1, mock_response2]
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 150
            assert mock_client.get.call_count == 2

    def test_fetch_records_empty_response(self, olo_config: ConnectorConfig) -> None:
        """Test fetching records with empty response."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 0

    def test_fetch_records_auto_authenticate(self, olo_config: ConnectorConfig) -> None:
        """Test fetch_records authenticates if not already authenticated."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": [{"id": "order-001"}]}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)

            assert connector.is_authenticated is False

            list(connector.fetch_records())

            assert connector.is_authenticated is True

    def test_fetch_records_http_401_error(self, olo_config: ConnectorConfig) -> None:
        """Test that 401 errors raise AuthenticationError."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.status_code = 401
        http_error = httpx.HTTPStatusError(
            "Unauthorized", request=MagicMock(), response=mock_response
        )

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value.raise_for_status.side_effect = http_error
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            with pytest.raises(AuthenticationError, match="Invalid OLO API key"):
                list(connector.fetch_records())

    def test_fetch_records_http_403_error(self, olo_config: ConnectorConfig) -> None:
        """Test that 403 errors raise AuthenticationError."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.status_code = 403
        http_error = httpx.HTTPStatusError(
            "Forbidden", request=MagicMock(), response=mock_response
        )

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value.raise_for_status.side_effect = http_error
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            with pytest.raises(
                AuthenticationError, match="does not have permission"
            ):
                list(connector.fetch_records())

    def test_fetch_records_http_429_error(self, olo_config: ConnectorConfig) -> None:
        """Test that 429 errors are logged and re-raised."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.status_code = 429
        http_error = httpx.HTTPStatusError(
            "Too Many Requests", request=MagicMock(), response=mock_response
        )

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value.raise_for_status.side_effect = http_error
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            with pytest.raises(httpx.HTTPStatusError):
                list(connector.fetch_records())

    def test_get_schema(self, olo_config: ConnectorConfig) -> None:
        """Test schema retrieval."""
        from growthnav.connectors.adapters.olo import OLOConnector

        connector = OLOConnector(olo_config)
        schema = connector.get_schema()

        # Verify expected fields are present
        assert "id" in schema
        assert "order_number" in schema
        assert "customer_id" in schema
        assert "customer_email" in schema
        assert "customer_phone" in schema
        assert "subtotal" in schema
        assert "tax" in schema
        assert "total" in schema
        assert "tip" in schema
        assert "discount" in schema
        assert "created_at" in schema
        assert "completed_at" in schema
        assert "status" in schema
        assert "order_type" in schema
        assert "location_id" in schema
        assert "location_name" in schema
        assert "items" in schema
        assert "payments" in schema

        # Verify types
        assert schema["id"] == "string"
        assert schema["total"] == "number"
        assert schema["created_at"] == "datetime"
        assert schema["items"] == "array"

    def test_normalize_orders(self, olo_config: ConnectorConfig) -> None:
        """Test normalization of order records."""
        from growthnav.connectors.adapters.olo import OLOConnector

        connector = OLOConnector(olo_config)

        raw_records = [
            {
                "id": "order-001",
                "total": 25.99,
                "created_at": "2024-06-15T12:00:00Z",
                "customer_id": "cust-001",
                "location_id": "loc-001",
            },
            {
                "id": "order-002",
                "total": 35.50,
                "created_at": "2024-06-15T13:00:00Z",
                "customer_id": "cust-002",
                "location_id": "loc-002",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 2
        assert conversions[0].transaction_id == "order-001"
        assert conversions[0].customer_id == "test_customer"
        assert conversions[1].transaction_id == "order-002"

    def test_normalize_with_field_overrides(self, olo_config: ConnectorConfig) -> None:
        """Test normalization with custom field mappings."""
        olo_config.field_overrides = {
            "custom_total": "value",
            "custom_time": "timestamp",
        }

        from growthnav.connectors.adapters.olo import OLOConnector

        connector = OLOConnector(olo_config)

        raw_records = [
            {
                "id": "order-001",
                "custom_total": 50.00,
                "custom_time": "2024-08-01T00:00:00Z",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 1
        assert conversions[0].transaction_id == "order-001"

    def test_auto_registration(self, olo_config: ConnectorConfig) -> None:
        """Test connector is auto-registered with registry."""
        from growthnav.connectors.adapters.olo import OLOConnector
        from growthnav.connectors.registry import get_registry

        registry = get_registry()

        assert registry.is_registered(ConnectorType.OLO)

        connector = registry.create(olo_config)
        assert isinstance(connector, OLOConnector)

    def test_context_manager(self, olo_config: ConnectorConfig) -> None:
        """Test connector works as context manager."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            with connector as ctx:
                assert ctx.is_authenticated is True

            assert connector._authenticated is False

    def test_cleanup_client(self, olo_config: ConnectorConfig) -> None:
        """Test cleanup closes the HTTP client."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            assert connector._client is not None

            connector._cleanup_client()

            mock_client.close.assert_called_once()

    def test_close_method(self, olo_config: ConnectorConfig) -> None:
        """Test close method via base class."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.authenticate()

            connector.close()

            assert connector._authenticated is False


class TestOLOConnectorSync:
    """Tests for OLOConnector sync functionality."""

    def test_sync_success(self, olo_config: ConnectorConfig) -> None:
        """Test successful sync operation."""
        from growthnav.connectors.adapters.olo import OLOConnector

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "orders": [
                {
                    "id": "order-001",
                    "total": 25.99,
                    "created_at": "2024-01-15T12:00:00Z",
                },
                {
                    "id": "order-002",
                    "total": 35.50,
                    "created_at": "2024-01-16T13:00:00Z",
                },
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)

            result = connector.sync()

            assert result.success is True
            assert result.records_fetched == 2
            assert result.records_normalized == 2
            assert result.connector_name == "Test OLO Connector"

    def test_sync_with_incremental_mode(self, olo_config: ConnectorConfig) -> None:
        """Test sync with incremental mode uses since parameter."""
        from growthnav.connectors.adapters.olo import OLOConnector
        from growthnav.connectors.config import SyncMode

        olo_config.sync_mode = SyncMode.INCREMENTAL
        olo_config.last_sync = datetime(2024, 1, 1, tzinfo=UTC)

        mock_response = MagicMock()
        mock_response.json.return_value = {"orders": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)
            connector.sync()

            # Verify since was passed to the API
            call_args = mock_client.get.call_args
            assert "created_after" in call_args[1]["params"]

    def test_sync_failure(self, olo_config: ConnectorConfig) -> None:
        """Test sync handles errors gracefully."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get.side_effect = Exception("API error")
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)

            result = connector.sync()

            assert result.success is False
            assert "API error" in result.error

    def test_test_connection_success(self, olo_config: ConnectorConfig) -> None:
        """Test connection test success."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            connector = OLOConnector(olo_config)

            assert connector.test_connection() is True

    def test_test_connection_failure(self, olo_config: ConnectorConfig) -> None:
        """Test connection test failure."""
        from growthnav.connectors.adapters.olo import OLOConnector

        with patch("httpx.Client") as mock_client_class:
            mock_client_class.side_effect = Exception("Connection failed")

            connector = OLOConnector(olo_config)

            assert connector.test_connection() is False
