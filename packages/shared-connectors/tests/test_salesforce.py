"""Tests for SalesforceConnector."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode


@pytest.fixture
def salesforce_config() -> ConnectorConfig:
    """Create a Salesforce connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.SALESFORCE,
        customer_id="test_customer",
        name="Test Salesforce Connector",
        credentials={
            "username": "test_user@company.com",
            "password": "test_pass",
            "security_token": "test_token",
        },
        connection_params={
            "domain": "login",
            "object_type": "Opportunity",
        },
        sync_mode=SyncMode.FULL,
    )


class TestSalesforceConnector:
    """Tests for SalesforceConnector."""

    def test_connector_type(self, salesforce_config: ConnectorConfig) -> None:
        """Test connector has correct type."""
        mock_sf = MagicMock()
        with patch.dict(
            "sys.modules",
            {"simple_salesforce": mock_sf},
        ):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            assert connector.connector_type == ConnectorType.SALESFORCE

    def test_authenticate_success(self, salesforce_config: ConnectorConfig) -> None:
        """Test successful authentication."""
        mock_sf_client = MagicMock()
        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict(
            "sys.modules",
            {"simple_salesforce": mock_sf_module},
        ):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            assert connector._client == mock_sf_client
            mock_sf_module.Salesforce.assert_called_once_with(
                username="test_user@company.com",
                password="test_pass",
                security_token="test_token",
                domain="login",
            )

    def test_authenticate_missing_dependency(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test missing simple-salesforce package raises ImportError."""
        # We need to test that the ImportError is raised when simple_salesforce is not installed
        # First import the connector class before mocking
        mock_sf = MagicMock()
        with patch.dict("sys.modules", {"simple_salesforce": mock_sf}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

        connector = SalesforceConnector(salesforce_config)

        # Now patch builtins to make import fail
        import builtins

        real_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):
            if "simple_salesforce" in name:
                raise ImportError("No module named 'simple_salesforce'")
            return real_import(name, *args, **kwargs)

        with (
            patch.object(builtins, "__import__", side_effect=mock_import),
            pytest.raises(ImportError, match="simple-salesforce is required"),
        ):
            connector.authenticate()

    def test_fetch_records_basic(self, salesforce_config: ConnectorConfig) -> None:
        """Test basic record fetching."""
        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {
            "records": [
                {
                    "attributes": {"type": "Opportunity"},
                    "Id": "001ABC",
                    "Name": "Test Opportunity",
                    "Amount": 10000.0,
                    "CloseDate": "2024-06-15",
                },
                {
                    "attributes": {"type": "Opportunity"},
                    "Id": "002DEF",
                    "Name": "Another Opportunity",
                    "Amount": 25000.0,
                    "CloseDate": "2024-07-01",
                },
            ],
            "done": True,
        }

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert records[0]["Id"] == "001ABC"
            assert records[0]["Amount"] == 10000.0
            # attributes should be removed
            assert "attributes" not in records[0]

    def test_fetch_records_with_pagination(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test record fetching with pagination."""
        mock_sf_client = MagicMock()
        # First page
        mock_sf_client.query.return_value = {
            "records": [{"Id": "001"}],
            "done": False,
            "nextRecordsUrl": "/services/data/v58.0/query/more",
        }
        # Second page
        mock_sf_client.query_more.return_value = {
            "records": [{"Id": "002"}],
            "done": True,
        }

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            mock_sf_client.query_more.assert_called_once()

    def test_fetch_records_with_time_range(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test record fetching with time range."""
        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {"records": [], "done": True}

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            since = datetime(2024, 1, 1, tzinfo=UTC)
            until = datetime(2024, 12, 31, tzinfo=UTC)

            list(connector.fetch_records(since=since, until=until))

            # Check that query was called with time filters
            call_args = mock_sf_client.query.call_args
            query = call_args[0][0]
            assert "LastModifiedDate >=" in query
            assert "LastModifiedDate <=" in query

    def test_fetch_records_with_limit(self, salesforce_config: ConnectorConfig) -> None:
        """Test record fetching with limit."""
        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {"records": [], "done": True}

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            list(connector.fetch_records(limit=50))

            call_args = mock_sf_client.query.call_args
            query = call_args[0][0]
            assert "LIMIT 50" in query

    def test_fetch_records_with_custom_query(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test record fetching with custom SOQL query."""
        salesforce_config.connection_params["query"] = (
            "SELECT Id, Name FROM Account WHERE Type = 'Customer'"
        )

        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {"records": [], "done": True}

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            list(connector.fetch_records())

            call_args = mock_sf_client.query.call_args
            query = call_args[0][0]
            assert query == "SELECT Id, Name FROM Account WHERE Type = 'Customer'"

    def test_get_fields_for_opportunity(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test fields selection for Opportunity object."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            fields = connector._get_fields_for_object("Opportunity")

            assert "Id" in fields
            assert "Amount" in fields
            assert "CloseDate" in fields
            assert "StageName" in fields

    def test_get_fields_for_lead(self, salesforce_config: ConnectorConfig) -> None:
        """Test fields selection for Lead object."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            fields = connector._get_fields_for_object("Lead")

            assert "Id" in fields
            assert "Email" in fields
            assert "Phone" in fields
            assert "Status" in fields

    def test_get_fields_for_account(self, salesforce_config: ConnectorConfig) -> None:
        """Test fields selection for Account object."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            fields = connector._get_fields_for_object("Account")

            assert "Id" in fields
            assert "Industry" in fields
            assert "AnnualRevenue" in fields

    def test_get_fields_for_unknown_object(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test fields selection for unknown object returns common fields."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            fields = connector._get_fields_for_object("CustomObject__c")

            assert fields == ["Id", "Name", "CreatedDate", "LastModifiedDate"]

    def test_get_schema(self, salesforce_config: ConnectorConfig) -> None:
        """Test schema retrieval."""
        mock_sf_client = MagicMock()
        mock_describe = {
            "fields": [
                {"name": "Id", "type": "id"},
                {"name": "Amount", "type": "currency"},
                {"name": "CloseDate", "type": "date"},
            ]
        }
        mock_sf_client.Opportunity.describe.return_value = mock_describe

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            schema = connector.get_schema()

            assert schema["Id"] == "id"
            assert schema["Amount"] == "currency"
            assert schema["CloseDate"] == "date"

    def test_normalize_opportunities(self, salesforce_config: ConnectorConfig) -> None:
        """Test normalization of Opportunity records."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            raw_records = [
                {
                    "Id": "001ABC",
                    "Amount": 15000.0,
                    "CloseDate": "2024-06-15T00:00:00Z",
                    "AccountId": "ACC001",
                },
                {
                    "Id": "002DEF",
                    "Amount": 25000.0,
                    "CloseDate": "2024-07-01T00:00:00Z",
                    "AccountId": "ACC002",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 2
            assert conversions[0].transaction_id == "001ABC"
            assert conversions[0].value == 15000.0
            assert conversions[0].customer_id == "test_customer"

    def test_normalize_leads(self, salesforce_config: ConnectorConfig) -> None:
        """Test normalization of Lead records."""
        salesforce_config.connection_params["object_type"] = "Lead"

        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            raw_records = [
                {
                    "Id": "LEAD001",
                    "Email": "test@example.com",
                    "CreatedDate": "2024-06-15T10:00:00Z",
                    "LeadSource": "Web",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1
            assert conversions[0].transaction_id == "LEAD001"

    def test_normalize_with_field_overrides(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test normalization with custom field mappings."""
        salesforce_config.field_overrides = {
            "CustomAmount__c": "value",
            "CustomDate__c": "timestamp",
        }

        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            raw_records = [
                {
                    "Id": "001ABC",
                    "CustomAmount__c": 50000.0,
                    "CustomDate__c": "2024-08-01T00:00:00Z",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1
            assert conversions[0].value == 50000.0

    def test_auto_registration(self, salesforce_config: ConnectorConfig) -> None:
        """Test connector is auto-registered with registry."""
        mock_sf_module = MagicMock()

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector
            from growthnav.connectors.registry import get_registry

            registry = get_registry()

            # The connector should be registered
            assert registry.is_registered(ConnectorType.SALESFORCE)

            # Should be able to create from registry
            connector = registry.create(salesforce_config)
            assert isinstance(connector, SalesforceConnector)

    def test_context_manager(self, salesforce_config: ConnectorConfig) -> None:
        """Test connector works as context manager."""
        mock_sf_client = MagicMock()
        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            with connector as ctx:
                assert ctx.is_authenticated is True

            assert connector._authenticated is False

    def test_fetch_records_auto_authenticate(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test fetch_records authenticates if not already authenticated."""
        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {"records": [], "done": True}

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            # Don't authenticate first
            assert connector.is_authenticated is False

            # Fetch should auto-authenticate
            list(connector.fetch_records())

            assert connector.is_authenticated is True
            mock_sf_module.Salesforce.assert_called_once()

    def test_get_schema_auto_authenticate(
        self, salesforce_config: ConnectorConfig
    ) -> None:
        """Test get_schema authenticates if not already authenticated."""
        mock_sf_client = MagicMock()
        mock_sf_client.Opportunity.describe.return_value = {"fields": []}

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            assert connector.is_authenticated is False

            connector.get_schema()

            assert connector.is_authenticated is True

    def test_default_security_token(self, salesforce_config: ConnectorConfig) -> None:
        """Test empty security token is used when not provided."""
        del salesforce_config.credentials["security_token"]

        mock_sf_client = MagicMock()
        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            call_kwargs = mock_sf_module.Salesforce.call_args[1]
            assert call_kwargs["security_token"] == ""

    def test_default_domain(self, salesforce_config: ConnectorConfig) -> None:
        """Test default domain is login."""
        del salesforce_config.connection_params["domain"]

        mock_sf_client = MagicMock()
        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            call_kwargs = mock_sf_module.Salesforce.call_args[1]
            assert call_kwargs["domain"] == "login"

    def test_sandbox_domain(self, salesforce_config: ConnectorConfig) -> None:
        """Test sandbox domain configuration."""
        salesforce_config.connection_params["domain"] = "test"

        mock_sf_client = MagicMock()
        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)
            connector.authenticate()

            call_kwargs = mock_sf_module.Salesforce.call_args[1]
            assert call_kwargs["domain"] == "test"


class TestSalesforceConnectorSync:
    """Tests for SalesforceConnector sync functionality."""

    def test_sync_success(self, salesforce_config: ConnectorConfig) -> None:
        """Test successful sync operation."""
        mock_sf_client = MagicMock()
        mock_sf_client.query.return_value = {
            "records": [
                {"Id": "001", "Amount": 1000.0, "CloseDate": "2024-01-15T00:00:00Z"},
                {"Id": "002", "Amount": 2000.0, "CloseDate": "2024-01-16T00:00:00Z"},
            ],
            "done": True,
        }

        mock_sf_module = MagicMock()
        mock_sf_module.Salesforce.return_value = mock_sf_client

        with patch.dict("sys.modules", {"simple_salesforce": mock_sf_module}):
            from growthnav.connectors.adapters.salesforce import SalesforceConnector

            connector = SalesforceConnector(salesforce_config)

            result = connector.sync()

            assert result.success is True
            assert result.records_fetched == 2
            assert result.records_normalized == 2
            assert result.connector_name == "Test Salesforce Connector"
