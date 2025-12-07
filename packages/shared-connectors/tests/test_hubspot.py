"""Tests for HubSpotConnector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode


@pytest.fixture
def hubspot_config() -> ConnectorConfig:
    """Create a HubSpot connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.HUBSPOT,
        customer_id="test_customer",
        name="Test HubSpot Connector",
        credentials={
            "access_token": "pat-na1-test-token",
        },
        connection_params={
            "object_type": "deals",
        },
        sync_mode=SyncMode.FULL,
    )


class TestHubSpotConnector:
    """Tests for HubSpotConnector."""

    def test_connector_type(self, hubspot_config: ConnectorConfig) -> None:
        """Test connector has correct type."""
        mock_hubspot = MagicMock()
        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            assert connector.connector_type == ConnectorType.HUBSPOT

    def test_authenticate_success(self, hubspot_config: ConnectorConfig) -> None:
        """Test successful authentication."""
        mock_hs_client = MagicMock()
        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            assert connector._client == mock_hs_client
            mock_hubspot.HubSpot.assert_called_once_with(
                access_token="pat-na1-test-token"
            )

    def test_authenticate_missing_dependency(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test missing hubspot-api-client package raises ImportError."""
        mock_hubspot = MagicMock()
        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

        connector = HubSpotConnector(hubspot_config)

        import builtins

        real_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):
            if "hubspot" in name:
                raise ImportError("No module named 'hubspot'")
            return real_import(name, *args, **kwargs)

        with (
            patch.object(builtins, "__import__", side_effect=mock_import),
            pytest.raises(ImportError, match="hubspot-api-client is required"),
        ):
            connector.authenticate()

    def test_fetch_records_deals(self, hubspot_config: ConnectorConfig) -> None:
        """Test fetching deal records."""
        mock_result1 = MagicMock()
        mock_result1.id = "deal-001"
        mock_result1.properties = {
            "dealname": "Test Deal",
            "amount": "10000",
            "closedate": "2024-06-15T00:00:00Z",
        }

        mock_result2 = MagicMock()
        mock_result2.id = "deal-002"
        mock_result2.properties = {
            "dealname": "Another Deal",
            "amount": "25000",
            "closedate": "2024-07-01T00:00:00Z",
        }

        mock_response = MagicMock()
        mock_response.results = [mock_result1, mock_result2]
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert records[0]["id"] == "deal-001"
            assert records[0]["dealname"] == "Test Deal"
            assert records[1]["id"] == "deal-002"

    def test_fetch_records_contacts(self, hubspot_config: ConnectorConfig) -> None:
        """Test fetching contact records."""
        hubspot_config.connection_params["object_type"] = "contacts"

        mock_result = MagicMock()
        mock_result.id = "contact-001"
        mock_result.properties = {
            "email": "test@example.com",
            "firstname": "John",
            "lastname": "Doe",
        }

        mock_response = MagicMock()
        mock_response.results = [mock_result]
        mock_response.paging = None

        mock_contacts_api = MagicMock()
        mock_contacts_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.contacts.basic_api = mock_contacts_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 1
            assert records[0]["email"] == "test@example.com"

    def test_fetch_records_companies(self, hubspot_config: ConnectorConfig) -> None:
        """Test fetching company records."""
        hubspot_config.connection_params["object_type"] = "companies"

        mock_result = MagicMock()
        mock_result.id = "company-001"
        mock_result.properties = {
            "name": "Acme Corp",
            "domain": "acme.com",
            "industry": "Technology",
        }

        mock_response = MagicMock()
        mock_response.results = [mock_result]
        mock_response.paging = None

        mock_companies_api = MagicMock()
        mock_companies_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.companies.basic_api = mock_companies_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 1
            assert records[0]["name"] == "Acme Corp"

    def test_fetch_records_unsupported_object_type(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test unsupported object type raises error."""
        hubspot_config.connection_params["object_type"] = "invalid"

        mock_hs_client = MagicMock()
        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            with pytest.raises(ValueError, match="Unsupported object type"):
                list(connector.fetch_records())

    def test_fetch_records_with_pagination(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test record fetching with pagination."""
        mock_result1 = MagicMock()
        mock_result1.id = "deal-001"
        mock_result1.properties = {}

        mock_result2 = MagicMock()
        mock_result2.id = "deal-002"
        mock_result2.properties = {}

        mock_paging_next = MagicMock()
        mock_paging_next.after = "cursor123"

        mock_paging = MagicMock()
        mock_paging.next = mock_paging_next

        mock_response1 = MagicMock()
        mock_response1.results = [mock_result1]
        mock_response1.paging = mock_paging

        mock_response2 = MagicMock()
        mock_response2.results = [mock_result2]
        mock_response2.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.side_effect = [mock_response1, mock_response2]

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert mock_deals_api.get_page.call_count == 2

    def test_fetch_records_with_time_filter(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test record fetching with time filter."""
        since = datetime(2024, 6, 1, tzinfo=UTC)
        until = datetime(2024, 7, 1, tzinfo=UTC)

        # Record within range
        mock_result1 = MagicMock()
        mock_result1.id = "deal-001"
        mock_result1.properties = {
            "hs_lastmodifieddate": "2024-06-15T00:00:00Z",
        }

        # Record before range (should be filtered out)
        mock_result2 = MagicMock()
        mock_result2.id = "deal-002"
        mock_result2.properties = {
            "hs_lastmodifieddate": "2024-05-01T00:00:00Z",
        }

        # Record after range (should be filtered out)
        mock_result3 = MagicMock()
        mock_result3.id = "deal-003"
        mock_result3.properties = {
            "hs_lastmodifieddate": "2024-08-01T00:00:00Z",
        }

        mock_response = MagicMock()
        mock_response.results = [mock_result1, mock_result2, mock_result3]
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records(since=since, until=until))

            # Only the record within range should be returned
            assert len(records) == 1
            assert records[0]["id"] == "deal-001"

    def test_fetch_records_with_limit(self, hubspot_config: ConnectorConfig) -> None:
        """Test record fetching with limit."""
        mock_results = []
        for i in range(10):
            mock_result = MagicMock()
            mock_result.id = f"deal-{i:03d}"
            mock_result.properties = {}
            mock_results.append(mock_result)

        mock_response = MagicMock()
        mock_response.results = mock_results
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records(limit=5))

            assert len(records) == 5

    def test_get_schema(self, hubspot_config: ConnectorConfig) -> None:
        """Test schema retrieval."""
        mock_prop1 = MagicMock()
        mock_prop1.name = "dealname"
        mock_prop1.type = "string"

        mock_prop2 = MagicMock()
        mock_prop2.name = "amount"
        mock_prop2.type = "number"

        mock_response = MagicMock()
        mock_response.results = [mock_prop1, mock_prop2]

        mock_properties_api = MagicMock()
        mock_properties_api.get_all.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.properties.core_api = mock_properties_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            schema = connector.get_schema()

            assert schema["dealname"] == "string"
            assert schema["amount"] == "number"

    def test_normalize_deals(self, hubspot_config: ConnectorConfig) -> None:
        """Test normalization of deal records."""
        mock_hubspot = MagicMock()

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            raw_records = [
                {
                    "id": "deal-001",
                    "amount": "15000",
                    "closedate": "2024-06-15T00:00:00Z",
                },
                {
                    "id": "deal-002",
                    "amount": "25000",
                    "closedate": "2024-07-01T00:00:00Z",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 2
            assert conversions[0].transaction_id == "deal-001"
            assert conversions[0].customer_id == "test_customer"

    def test_normalize_contacts(self, hubspot_config: ConnectorConfig) -> None:
        """Test normalization of contact records."""
        hubspot_config.connection_params["object_type"] = "contacts"

        mock_hubspot = MagicMock()

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            raw_records = [
                {
                    "id": "contact-001",
                    "email": "test@example.com",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1
            assert conversions[0].transaction_id == "contact-001"

    def test_normalize_companies(self, hubspot_config: ConnectorConfig) -> None:
        """Test normalization of company records (custom type)."""
        hubspot_config.connection_params["object_type"] = "companies"

        mock_hubspot = MagicMock()

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            raw_records = [
                {
                    "id": "company-001",
                    "name": "Acme Corp",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1
            assert conversions[0].transaction_id == "company-001"

    def test_normalize_with_field_overrides(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test normalization with custom field mappings."""
        hubspot_config.field_overrides = {
            "custom_amount": "value",
            "custom_date": "timestamp",
        }

        mock_hubspot = MagicMock()

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            raw_records = [
                {
                    "id": "deal-001",
                    "custom_amount": "50000",
                    "custom_date": "2024-08-01T00:00:00Z",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1

    def test_auto_registration(self, hubspot_config: ConnectorConfig) -> None:
        """Test connector is auto-registered with registry."""
        mock_hubspot = MagicMock()

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector
            from growthnav.connectors.registry import get_registry

            registry = get_registry()

            assert registry.is_registered(ConnectorType.HUBSPOT)

            connector = registry.create(hubspot_config)
            assert isinstance(connector, HubSpotConnector)

    def test_context_manager(self, hubspot_config: ConnectorConfig) -> None:
        """Test connector works as context manager."""
        mock_hs_client = MagicMock()
        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            with connector as ctx:
                assert ctx.is_authenticated is True

            assert connector._authenticated is False

    def test_fetch_records_auto_authenticate(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test fetch_records authenticates if not already authenticated."""
        mock_result = MagicMock()
        mock_result.id = "deal-001"
        mock_result.properties = {}

        mock_response = MagicMock()
        mock_response.results = [mock_result]
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            assert connector.is_authenticated is False

            list(connector.fetch_records())

            assert connector.is_authenticated is True

    def test_get_schema_auto_authenticate(
        self, hubspot_config: ConnectorConfig
    ) -> None:
        """Test get_schema authenticates if not already authenticated."""
        mock_response = MagicMock()
        mock_response.results = []

        mock_properties_api = MagicMock()
        mock_properties_api.get_all.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.properties.core_api = mock_properties_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            assert connector.is_authenticated is False

            connector.get_schema()

            assert connector.is_authenticated is True

    def test_default_object_type(self, hubspot_config: ConnectorConfig) -> None:
        """Test default object type is deals."""
        del hubspot_config.connection_params["object_type"]

        mock_result = MagicMock()
        mock_result.id = "deal-001"
        mock_result.properties = {}

        mock_response = MagicMock()
        mock_response.results = [mock_result]
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            # Should use deals API by default
            assert len(records) == 1
            mock_deals_api.get_page.assert_called_once()


class TestHubSpotConnectorSync:
    """Tests for HubSpotConnector sync functionality."""

    def test_sync_success(self, hubspot_config: ConnectorConfig) -> None:
        """Test successful sync operation."""
        mock_result1 = MagicMock()
        mock_result1.id = "deal-001"
        mock_result1.properties = {"amount": "1000", "closedate": "2024-01-15T00:00:00Z"}

        mock_result2 = MagicMock()
        mock_result2.id = "deal-002"
        mock_result2.properties = {"amount": "2000", "closedate": "2024-01-16T00:00:00Z"}

        mock_response = MagicMock()
        mock_response.results = [mock_result1, mock_result2]
        mock_response.paging = None

        mock_deals_api = MagicMock()
        mock_deals_api.get_page.return_value = mock_response

        mock_hs_client = MagicMock()
        mock_hs_client.crm.deals.basic_api = mock_deals_api

        mock_hubspot = MagicMock()
        mock_hubspot.HubSpot.return_value = mock_hs_client

        with patch.dict("sys.modules", {"hubspot": mock_hubspot}):
            from growthnav.connectors.adapters.hubspot import HubSpotConnector

            connector = HubSpotConnector(hubspot_config)

            result = connector.sync()

            assert result.success is True
            assert result.records_fetched == 2
            assert result.records_normalized == 2
            assert result.connector_name == "Test HubSpot Connector"
