"""Tests for ZohoConnector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode


@pytest.fixture
def zoho_config() -> ConnectorConfig:
    """Create a Zoho connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.ZOHO,
        customer_id="test_customer",
        name="Test Zoho Connector",
        credentials={
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
        },
        connection_params={
            "module": "Deals",
            "domain": "zohoapis.com",
        },
        sync_mode=SyncMode.FULL,
    )


class TestZohoConnector:
    """Tests for ZohoConnector."""

    def test_connector_type(self, zoho_config: ConnectorConfig) -> None:
        """Test connector has correct type."""
        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)
        assert connector.connector_type == ConnectorType.ZOHO

    def test_authenticate_success(self, zoho_config: ConnectorConfig) -> None:
        """Test successful authentication."""
        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "test_access_token"}
        mock_token_response.raise_for_status = MagicMock()

        mock_http_client = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            # First call is for token refresh (context manager)
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            # Second call is for the API client
            mock_client_class.side_effect = [mock_token_client, mock_http_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            assert connector._access_token == "test_access_token"
            assert connector._client == mock_http_client

    def test_authenticate_token_request(self, zoho_config: ConnectorConfig) -> None:
        """Test token refresh request is sent correctly."""
        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "test_access_token"}
        mock_token_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_api_client = MagicMock()
            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Verify token request
            mock_token_client.post.assert_called_once_with(
                "https://accounts.zohoapis.com/oauth/v2/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": "test_client_id",
                    "client_secret": "test_client_secret",
                    "refresh_token": "test_refresh_token",
                },
            )

    def test_fetch_records_basic(self, zoho_config: ConnectorConfig) -> None:
        """Test basic record fetching."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "123456",
                    "Deal_Name": "Test Deal",
                    "Amount": 10000.0,
                    "Closing_Date": "2024-06-15",
                },
                {
                    "id": "789012",
                    "Deal_Name": "Another Deal",
                    "Amount": 25000.0,
                    "Closing_Date": "2024-07-01",
                },
            ],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert records[0]["id"] == "123456"
            assert records[0]["Amount"] == 10000.0

    def test_fetch_records_with_pagination(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test record fetching with pagination."""
        mock_response1 = MagicMock()
        mock_response1.json.return_value = {
            "data": [{"id": "001"}],
            "info": {"more_records": True},
        }
        mock_response1.raise_for_status = MagicMock()

        mock_response2 = MagicMock()
        mock_response2.json.return_value = {
            "data": [{"id": "002"}],
            "info": {"more_records": False},
        }
        mock_response2.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.side_effect = [mock_response1, mock_response2]

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert mock_api_client.get.call_count == 2

    def test_fetch_records_with_time_filter(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test record fetching with time filter."""
        since = datetime(2024, 6, 1, tzinfo=UTC)
        until = datetime(2024, 7, 1, tzinfo=UTC)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": "001", "Modified_Time": "2024-06-15T00:00:00Z"},  # In range
                {"id": "002", "Modified_Time": "2024-05-01T00:00:00Z"},  # Before range
                {"id": "003", "Modified_Time": "2024-08-01T00:00:00Z"},  # After range
            ],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records(since=since, until=until))

            assert len(records) == 1
            assert records[0]["id"] == "001"

    def test_fetch_records_with_limit(self, zoho_config: ConnectorConfig) -> None:
        """Test record fetching with limit."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": f"{i:03d}"} for i in range(10)],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records(limit=5))

            assert len(records) == 5

    def test_fetch_records_empty_data(self, zoho_config: ConnectorConfig) -> None:
        """Test fetching when no records exist."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 0

    def test_fetch_records_leads_module(self, zoho_config: ConnectorConfig) -> None:
        """Test fetching from Leads module."""
        zoho_config.connection_params["module"] = "Leads"

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": "lead-001", "Email": "test@example.com"}],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 1
            # Verify the correct module endpoint was called
            call_args = mock_api_client.get.call_args
            assert "/Leads" in call_args[0][0]

    def test_get_schema(self, zoho_config: ConnectorConfig) -> None:
        """Test schema retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "fields": [
                {"api_name": "Deal_Name", "data_type": "text"},
                {"api_name": "Amount", "data_type": "currency"},
                {"api_name": "Closing_Date", "data_type": "date"},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            schema = connector.get_schema()

            assert schema["Deal_Name"] == "text"
            assert schema["Amount"] == "currency"
            assert schema["Closing_Date"] == "date"

    def test_normalize_deals(self, zoho_config: ConnectorConfig) -> None:
        """Test normalization of deal records."""
        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        raw_records = [
            {
                "id": "deal-001",
                "Amount": 15000.0,
                "Closing_Date": "2024-06-15T00:00:00Z",
            },
            {
                "id": "deal-002",
                "Amount": 25000.0,
                "Closing_Date": "2024-07-01T00:00:00Z",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 2
        assert conversions[0].transaction_id == "deal-001"
        assert conversions[0].customer_id == "test_customer"

    def test_normalize_leads(self, zoho_config: ConnectorConfig) -> None:
        """Test normalization of lead records."""
        zoho_config.connection_params["module"] = "Leads"

        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        raw_records = [
            {
                "id": "lead-001",
                "Email": "test@example.com",
                "Created_Time": "2024-06-15T10:00:00Z",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 1
        assert conversions[0].transaction_id == "lead-001"

    def test_normalize_accounts_custom_type(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test normalization of account records (custom type)."""
        zoho_config.connection_params["module"] = "Accounts"

        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        raw_records = [
            {
                "id": "account-001",
                "Account_Name": "Acme Corp",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 1
        assert conversions[0].transaction_id == "account-001"

    def test_normalize_with_field_overrides(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test normalization with custom field mappings."""
        zoho_config.field_overrides = {
            "Custom_Amount": "value",
            "Custom_Date": "timestamp",
        }

        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        raw_records = [
            {
                "id": "deal-001",
                "Custom_Amount": 50000.0,
                "Custom_Date": "2024-08-01T00:00:00Z",
            },
        ]

        conversions = connector.normalize(raw_records)

        assert len(conversions) == 1

    def test_auto_registration(self, zoho_config: ConnectorConfig) -> None:
        """Test connector is auto-registered with registry."""
        from growthnav.connectors.adapters.zoho import ZohoConnector
        from growthnav.connectors.registry import get_registry

        registry = get_registry()

        assert registry.is_registered(ConnectorType.ZOHO)

        connector = registry.create(zoho_config)
        assert isinstance(connector, ZohoConnector)

    def test_context_manager(self, zoho_config: ConnectorConfig) -> None:
        """Test connector works as context manager."""
        mock_api_client = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            with connector as ctx:
                assert ctx.is_authenticated is True

            mock_api_client.close.assert_called_once()
            assert connector._authenticated is False

    def test_cleanup_client(self, zoho_config: ConnectorConfig) -> None:
        """Test client cleanup closes HTTP client."""
        mock_api_client = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            connector.close()

            mock_api_client.close.assert_called_once()
            assert connector._authenticated is False

    def test_cleanup_client_with_error(self, zoho_config: ConnectorConfig) -> None:
        """Test client cleanup handles errors gracefully."""
        mock_api_client = MagicMock()
        mock_api_client.close.side_effect = Exception("Connection error")

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Should not raise even though close() fails
            connector.close()

            assert connector._authenticated is False

    def test_fetch_records_auto_authenticate(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test fetch_records authenticates if not already authenticated."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": "001"}],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)

            assert connector.is_authenticated is False

            list(connector.fetch_records())

            assert connector.is_authenticated is True

    def test_get_schema_auto_authenticate(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test get_schema authenticates if not already authenticated."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"fields": []}
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)

            assert connector.is_authenticated is False

            connector.get_schema()

            assert connector.is_authenticated is True

    def test_default_module(self, zoho_config: ConnectorConfig) -> None:
        """Test default module is Deals."""
        del zoho_config.connection_params["module"]

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": "001"}],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            list(connector.fetch_records())

            call_args = mock_api_client.get.call_args
            assert "/Deals" in call_args[0][0]

    def test_default_domain(self, zoho_config: ConnectorConfig) -> None:
        """Test default domain is zohoapis.com."""
        del zoho_config.connection_params["domain"]

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_api_client = MagicMock()
            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Verify token URL uses default domain
            token_call = mock_token_client.post.call_args
            assert "zohoapis.com" in token_call[0][0]


class TestZohoConnectorSync:
    """Tests for ZohoConnector sync functionality."""

    def test_sync_success(self, zoho_config: ConnectorConfig) -> None:
        """Test successful sync operation."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": "001", "Amount": 1000.0, "Closing_Date": "2024-01-15T00:00:00Z"},
                {"id": "002", "Amount": 2000.0, "Closing_Date": "2024-01-16T00:00:00Z"},
            ],
            "info": {"more_records": False},
        }
        mock_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_response = MagicMock()
            mock_token_response.json.return_value = {"access_token": "token"}
            mock_token_response.raise_for_status = MagicMock()

            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)

            result = connector.sync()

            assert result.success is True
            assert result.records_fetched == 2
            assert result.records_normalized == 2
            assert result.connector_name == "Test Zoho Connector"
