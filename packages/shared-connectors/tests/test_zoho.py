"""Tests for ZohoConnector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
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
        # Use type name comparison to avoid module import caching issues
        assert type(connector).__name__ == ZohoConnector.__name__
        assert type(connector).__module__ == ZohoConnector.__module__

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


    def test_invalid_module_raises_error(self, zoho_config: ConnectorConfig) -> None:
        """Test invalid module name raises ValueError."""
        zoho_config.connection_params["module"] = "InvalidModule"

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

            with pytest.raises(ValueError, match="Unsupported Zoho module"):
                list(connector.fetch_records())

    def test_invalid_domain_raises_error(self, zoho_config: ConnectorConfig) -> None:
        """Test invalid domain raises ValueError during initialization."""
        zoho_config.connection_params["domain"] = "evil-domain.com"

        from growthnav.connectors.adapters.zoho import ZohoConnector

        # Domain is now validated in __init__, so this raises immediately
        with pytest.raises(ValueError, match="Invalid Zoho domain"):
            ZohoConnector(zoho_config)

    def test_authenticate_failure(self, zoho_config: ConnectorConfig) -> None:
        """Test authentication failure raises AuthenticationError."""
        from growthnav.connectors.exceptions import AuthenticationError

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.side_effect = Exception("Connection refused")
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.return_value = mock_token_client

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)

            with pytest.raises(AuthenticationError, match="Failed to authenticate"):
                connector.authenticate()

    def test_fetch_records_with_invalid_date_format(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test records with invalid date format are still included with warning."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": "001", "Modified_Time": "invalid-date-format"},
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

            since = datetime(2024, 1, 1, tzinfo=UTC)
            records = list(connector.fetch_records(since=since))

            # Record should be included even with invalid date
            assert len(records) == 1
            assert records[0]["id"] == "001"

    def test_get_schema_failure(self, zoho_config: ConnectorConfig) -> None:
        """Test schema retrieval failure raises SchemaError."""
        from growthnav.connectors.exceptions import SchemaError

        mock_api_client = MagicMock()
        mock_api_client.get.side_effect = Exception("API error")

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

            with pytest.raises(SchemaError, match="Failed to get schema"):
                connector.get_schema()


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


class TestZohoConnectorTokenRefresh:
    """Tests for ZohoConnector token refresh functionality."""

    def test_token_refresh_on_401_fetch_records(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test automatic token refresh when fetch_records gets 401."""
        # First API call returns 401, then token refresh, then retry succeeds
        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_success_response = MagicMock()
        mock_success_response.json.return_value = {
            "data": [{"id": "001", "Deal_Name": "Test Deal"}],
            "info": {"more_records": False},
        }
        mock_success_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        # First call returns 401, second call succeeds (after token refresh)
        mock_api_client.get.side_effect = [mock_401_response, mock_success_response]
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        # Token refresh responses
        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        mock_token_response_refresh = MagicMock()
        mock_token_response_refresh.json.return_value = {"access_token": "new_token"}
        mock_token_response_refresh.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            # Initial auth token client
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(
                return_value=mock_token_client_initial
            )
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            # Refresh token client
            mock_token_client_refresh = MagicMock()
            mock_token_client_refresh.post.return_value = mock_token_response_refresh
            mock_token_client_refresh.__enter__ = MagicMock(
                return_value=mock_token_client_refresh
            )
            mock_token_client_refresh.__exit__ = MagicMock(return_value=False)

            # Order: initial token client, API client, refresh token client
            mock_client_class.side_effect = [
                mock_token_client_initial,
                mock_api_client,
                mock_token_client_refresh,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            # Should have gotten records after token refresh
            assert len(records) == 1
            assert records[0]["id"] == "001"

            # Verify token was refreshed (client.get called twice)
            assert mock_api_client.get.call_count == 2

            # Verify authorization header was updated
            assert mock_api_client.headers["Authorization"] == "Zoho-oauthtoken new_token"

    def test_token_refresh_on_401_get_schema(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test automatic token refresh when get_schema gets 401."""
        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_success_response = MagicMock()
        mock_success_response.json.return_value = {
            "fields": [
                {"api_name": "Deal_Name", "data_type": "text"},
                {"api_name": "Amount", "data_type": "currency"},
            ]
        }
        mock_success_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.get.side_effect = [mock_401_response, mock_success_response]
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        mock_token_response_refresh = MagicMock()
        mock_token_response_refresh.json.return_value = {"access_token": "new_token"}
        mock_token_response_refresh.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(
                return_value=mock_token_client_initial
            )
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            mock_token_client_refresh = MagicMock()
            mock_token_client_refresh.post.return_value = mock_token_response_refresh
            mock_token_client_refresh.__enter__ = MagicMock(
                return_value=mock_token_client_refresh
            )
            mock_token_client_refresh.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [
                mock_token_client_initial,
                mock_api_client,
                mock_token_client_refresh,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            schema = connector.get_schema()

            assert schema["Deal_Name"] == "text"
            assert schema["Amount"] == "currency"
            assert mock_api_client.get.call_count == 2
            assert mock_api_client.headers["Authorization"] == "Zoho-oauthtoken new_token"

    def test_token_refresh_fails_raises_authentication_error(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that failed token refresh raises AuthenticationError."""
        from growthnav.connectors.exceptions import AuthenticationError

        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_401_response
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(
                return_value=mock_token_client_initial
            )
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            # Token refresh will fail
            mock_token_client_refresh = MagicMock()
            mock_token_client_refresh.post.side_effect = Exception("Token refresh failed")
            mock_token_client_refresh.__enter__ = MagicMock(
                return_value=mock_token_client_refresh
            )
            mock_token_client_refresh.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [
                mock_token_client_initial,
                mock_api_client,
                mock_token_client_refresh,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            with pytest.raises(AuthenticationError, match="Failed to refresh"):
                list(connector.fetch_records())

    def test_max_retry_limit_exceeded(self, zoho_config: ConnectorConfig) -> None:
        """Test that 401 after max retries raises HTTPStatusError."""
        # Both calls return 401 - should fail after one retry attempt
        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_401_response
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "token"}
        mock_token_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            # Initial token, API client, refresh token (success but API still 401)
            mock_client_class.side_effect = [
                mock_token_client,
                mock_api_client,
                mock_token_client,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            with pytest.raises(httpx.HTTPStatusError):
                list(connector.fetch_records())

            # Should have tried twice: initial call + 1 retry
            assert mock_api_client.get.call_count == 2

    def test_non_401_error_not_retried(self, zoho_config: ConnectorConfig) -> None:
        """Test that non-401 HTTP errors are not retried."""
        mock_500_response = MagicMock()
        mock_500_response.status_code = 500
        mock_500_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Internal Server Error",
            request=MagicMock(),
            response=mock_500_response,
        )

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_500_response
        mock_api_client.headers = {}

        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "token"}
        mock_token_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            with pytest.raises(httpx.HTTPStatusError):
                list(connector.fetch_records())

            # Should only have tried once - 500 is not retried
            assert mock_api_client.get.call_count == 1

    def test_domain_stored_for_token_refresh(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that domain is stored during authentication for token refresh."""
        zoho_config.connection_params["domain"] = "zohoapis.eu"

        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "token"}
        mock_token_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Domain should be stored
            assert connector._domain == "zohoapis.eu"

            # Token URL should use the EU domain
            call_args = mock_token_client.post.call_args
            assert "zohoapis.eu" in call_args[0][0]

    def test_update_client_authorization(self, zoho_config: ConnectorConfig) -> None:
        """Test _update_client_authorization updates header correctly."""
        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "initial_token"}
        mock_token_response.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken initial_token"}

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.return_value = mock_token_response
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [mock_token_client, mock_api_client]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Manually update access token and call update method
            connector._access_token = "new_token"
            connector._update_client_authorization()

            assert (
                mock_api_client.headers["Authorization"]
                == "Zoho-oauthtoken new_token"
            )

    def test_domain_initialized_in_init(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that domain is initialized during __init__."""
        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        # Domain should be set during init (default: zohoapis.com)
        assert connector._domain == "zohoapis.com"

    def test_missing_credentials_raises_error(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that missing credentials raise AuthenticationError."""
        from growthnav.connectors.exceptions import AuthenticationError

        # Remove a required credential
        del zoho_config.credentials["client_secret"]

        from growthnav.connectors.adapters.zoho import ZohoConnector

        connector = ZohoConnector(zoho_config)

        with pytest.raises(AuthenticationError, match="Missing required Zoho credentials"):
            connector.authenticate()

    def test_credential_validation_reraise_during_token_refresh(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test credential validation error is re-raised during token refresh retry."""
        from growthnav.connectors.exceptions import AuthenticationError

        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_401_response
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(
                return_value=mock_token_client_initial
            )
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [
                mock_token_client_initial,
                mock_api_client,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Now remove a credential to trigger validation error during refresh
            del connector.config.credentials["client_secret"]

            # Should raise AuthenticationError for missing credentials (re-raised path)
            with pytest.raises(AuthenticationError, match="Missing required Zoho credentials"):
                list(connector.fetch_records())

    def test_get_schema_reraises_authentication_error(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test get_schema re-raises AuthenticationError when token refresh fails."""
        from growthnav.connectors.exceptions import AuthenticationError

        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_401_response,
        )

        mock_api_client = MagicMock()
        mock_api_client.get.return_value = mock_401_response
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_class:
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(
                return_value=mock_token_client_initial
            )
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            # Token refresh will fail
            mock_token_client_refresh = MagicMock()
            mock_token_client_refresh.post.side_effect = Exception("Token refresh failed")
            mock_token_client_refresh.__enter__ = MagicMock(
                return_value=mock_token_client_refresh
            )
            mock_token_client_refresh.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [
                mock_token_client_initial,
                mock_api_client,
                mock_token_client_refresh,
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Should raise AuthenticationError, not SchemaError
            with pytest.raises(AuthenticationError, match="Failed to refresh"):
                connector.get_schema()

    def test_concurrent_token_refresh_thread_safety(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that concurrent 401 responses don't cause race conditions."""
        import threading
        import time

        # Track how many times token refresh was called
        refresh_call_count = 0
        refresh_lock = threading.Lock()

        mock_token_response = MagicMock()
        mock_token_response.json.return_value = {"access_token": "refreshed_token"}
        mock_token_response.raise_for_status = MagicMock()

        mock_success_response = MagicMock()
        mock_success_response.json.return_value = {
            "data": [{"id": "001", "Deal_Name": "Test"}],
            "info": {"more_records": False},
        }
        mock_success_response.raise_for_status = MagicMock()

        # First call returns 401, subsequent calls succeed
        call_count = 0
        call_count_lock = threading.Lock()

        def mock_get(*args, **kwargs):
            nonlocal call_count
            with call_count_lock:
                call_count += 1
                current_call = call_count

            # First 2 calls (from 2 threads) return 401
            if current_call <= 2:
                mock_401 = MagicMock()
                mock_401.status_code = 401
                mock_401.raise_for_status.side_effect = httpx.HTTPStatusError(
                    "Unauthorized", request=MagicMock(), response=mock_401
                )
                return mock_401
            return mock_success_response

        mock_api_client = MagicMock()
        mock_api_client.get.side_effect = mock_get
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken old_token"}

        def mock_post(*args, **kwargs):
            nonlocal refresh_call_count
            with refresh_lock:
                refresh_call_count += 1
            # Small delay to simulate network latency and increase chance of race
            time.sleep(0.01)
            return mock_token_response

        with patch("httpx.Client") as mock_client_class:
            mock_token_client = MagicMock()
            mock_token_client.post.side_effect = mock_post
            mock_token_client.__enter__ = MagicMock(return_value=mock_token_client)
            mock_token_client.__exit__ = MagicMock(return_value=False)

            # Return token client for initial auth, then API client, then token clients for refreshes
            mock_client_class.side_effect = [
                mock_token_client,  # Initial auth
                mock_api_client,  # API client
                mock_token_client,  # First refresh
                mock_token_client,  # Second refresh (if needed)
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Reset refresh count after initial auth
            refresh_call_count = 0

            # Run two concurrent fetch operations
            results = []
            errors = []

            def fetch_in_thread():
                try:
                    records = list(connector.fetch_records(limit=1))
                    results.append(records)
                except Exception as e:
                    errors.append(e)

            threads = [
                threading.Thread(target=fetch_in_thread),
                threading.Thread(target=fetch_in_thread),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)

            # Verify no unhandled errors
            assert len(errors) == 0, f"Unexpected errors: {errors}"

            # The lock ensures token refresh is serialized
            # (exact count depends on timing, but should be at least 1)
            assert refresh_call_count >= 1

    def test_token_refresh_updates_header_atomically(
        self, zoho_config: ConnectorConfig
    ) -> None:
        """Test that token and header are updated together."""
        mock_token_response_initial = MagicMock()
        mock_token_response_initial.json.return_value = {"access_token": "initial_token"}
        mock_token_response_initial.raise_for_status = MagicMock()

        mock_token_response_refresh = MagicMock()
        mock_token_response_refresh.json.return_value = {"access_token": "new_token"}
        mock_token_response_refresh.raise_for_status = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.headers = {"Authorization": "Zoho-oauthtoken initial_token"}

        with patch("httpx.Client") as mock_client_class:
            mock_token_client_initial = MagicMock()
            mock_token_client_initial.post.return_value = mock_token_response_initial
            mock_token_client_initial.__enter__ = MagicMock(return_value=mock_token_client_initial)
            mock_token_client_initial.__exit__ = MagicMock(return_value=False)

            mock_token_client_refresh = MagicMock()
            mock_token_client_refresh.post.return_value = mock_token_response_refresh
            mock_token_client_refresh.__enter__ = MagicMock(return_value=mock_token_client_refresh)
            mock_token_client_refresh.__exit__ = MagicMock(return_value=False)

            mock_client_class.side_effect = [
                mock_token_client_initial,  # Initial auth
                mock_api_client,  # API client
                mock_token_client_refresh,  # Manual refresh
            ]

            from growthnav.connectors.adapters.zoho import ZohoConnector

            connector = ZohoConnector(zoho_config)
            connector.authenticate()

            # Manually trigger token refresh
            connector._refresh_access_token()
            connector._update_client_authorization()

            # Both token and header should be updated
            assert connector._access_token == "new_token"
            assert mock_api_client.headers["Authorization"] == "Zoho-oauthtoken new_token"
