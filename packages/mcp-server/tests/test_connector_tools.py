"""Tests for MCP connector tools."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

# =============================================================================
# _sanitize_error Tests
# =============================================================================


def test_sanitize_error_password():
    """Test _sanitize_error removes password from error message."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("Connection failed: password=secret123 at host")
    assert "secret123" not in result
    assert "password=***" in result


def test_sanitize_error_token():
    """Test _sanitize_error removes token from error message."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("Auth failed: token: abc123xyz456")
    assert "abc123xyz456" not in result
    assert "token=***" in result


def test_sanitize_error_api_key():
    """Test _sanitize_error removes api_key from error message."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("API error: api_key=sk-1234567890abcdef invalid")
    assert "sk-1234567890abcdef" not in result
    assert "api_key=***" in result


def test_sanitize_error_api_hyphen_key():
    """Test _sanitize_error removes api-key from error message."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error('Error: {"api-key": "supersecret"}')
    assert "supersecret" not in result
    assert "api_key=***" in result


def test_sanitize_error_secret():
    """Test _sanitize_error removes secret from error message."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("Failed with secret=mysecretvalue")
    assert "mysecretvalue" not in result
    assert "secret=***" in result


def test_sanitize_error_preserves_other_text():
    """Test _sanitize_error preserves non-sensitive error information."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("Connection timeout after 30 seconds to host.example.com")
    assert result == "Connection timeout after 30 seconds to host.example.com"


def test_sanitize_error_case_insensitive():
    """Test _sanitize_error works case-insensitively."""
    from growthnav_mcp.server import _sanitize_error

    result = _sanitize_error("Failed: PASSWORD=Secret123 and TOKEN=abc123")
    assert "Secret123" not in result
    assert "abc123" not in result


# =============================================================================
# list_connectors Tests
# =============================================================================


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorType")
def test_list_connectors(mock_connector_type, mock_get_registry):
    """Test listing all connector types."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    list_connectors = mcp._tool_manager._tools["list_connectors"].fn

    # Setup mock ConnectorType enum
    mock_snowflake = Mock()
    mock_snowflake.value = "snowflake"

    mock_salesforce = Mock()
    mock_salesforce.value = "salesforce"

    mock_olo = Mock()
    mock_olo.value = "olo"

    mock_connector_type.__iter__ = Mock(
        return_value=iter([mock_snowflake, mock_salesforce, mock_olo])
    )

    # Setup mock registry
    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.is_registered.side_effect = [True, True, False]

    # Execute
    result = list_connectors()

    # Verify
    assert len(result) == 3
    assert result[0]["type"] == "snowflake"
    assert result[0]["registered"] is True
    assert result[1]["type"] == "salesforce"
    assert result[1]["registered"] is True
    assert result[2]["type"] == "olo"
    assert result[2]["registered"] is False


def test_get_connector_category():
    """Test connector category mapping."""
    from growthnav.connectors import ConnectorType
    from growthnav_mcp.server import _get_connector_category

    # Test data lake connectors
    assert _get_connector_category(ConnectorType.SNOWFLAKE) == "data_lake"
    assert _get_connector_category(ConnectorType.BIGQUERY) == "data_lake"

    # Test POS connectors
    assert _get_connector_category(ConnectorType.TOAST) == "pos"
    assert _get_connector_category(ConnectorType.SQUARE) == "pos"
    assert _get_connector_category(ConnectorType.CLOVER) == "pos"
    assert _get_connector_category(ConnectorType.LIGHTSPEED) == "pos"

    # Test CRM connectors
    assert _get_connector_category(ConnectorType.SALESFORCE) == "crm"
    assert _get_connector_category(ConnectorType.HUBSPOT) == "crm"
    assert _get_connector_category(ConnectorType.ZOHO) == "crm"

    # Test OLO connectors
    assert _get_connector_category(ConnectorType.OLO) == "olo"
    assert _get_connector_category(ConnectorType.OTTER) == "olo"
    assert _get_connector_category(ConnectorType.CHOWLY) == "olo"

    # Test loyalty connectors
    assert _get_connector_category(ConnectorType.FISHBOWL) == "loyalty"
    assert _get_connector_category(ConnectorType.PUNCHH) == "loyalty"


# =============================================================================
# configure_data_source Tests
# =============================================================================


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
def test_configure_data_source_success(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test successful data source configuration."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type.value = "snowflake"
    mock_connector_type_class.return_value = mock_connector_type

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.is_registered.return_value = True

    mock_connector = Mock()
    mock_connector.test_connection.return_value = True
    mock_registry.create.return_value = mock_connector

    mock_config = Mock()
    mock_config.customer_id = "acme"
    mock_config.connector_type = mock_connector_type
    mock_config.name = "Snowflake POS"
    mock_config_class.return_value = mock_config

    # Execute
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Snowflake POS",
        connection_params={"account": "acme.snowflakecomputing.com"},
        credentials={"username": "user", "password": "pass"},
    )

    # Verify
    assert result["success"] is True
    assert result["config"]["customer_id"] == "acme"
    assert result["config"]["connector_type"] == "snowflake"
    assert result["message"] == "Connection test successful"


@patch("growthnav.connectors.ConnectorType")
def test_configure_data_source_invalid_type(mock_connector_type_class):
    """Test configuration with invalid connector type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Make ConnectorType raise ValueError for invalid type
    mock_connector_type_class.side_effect = ValueError("Invalid type")

    # Execute
    result = configure_data_source(
        customer_id="acme",
        connector_type="invalid_connector",
        name="Test",
        connection_params={},
    )

    # Verify
    assert result["success"] is False
    assert "Unknown connector type" in result["error"]


def test_configure_data_source_both_credentials():
    """Test configuration fails when both credentials and secret_path provided."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Execute with both credentials options
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params={},
        credentials={"user": "test"},
        credentials_secret_path="projects/my-project/secrets/my-secret",
    )

    # Verify
    assert result["success"] is False
    assert "either credentials or credentials_secret_path" in result["error"]


def test_configure_data_source_invalid_connection_params_type():
    """Test configuration fails when connection_params is not a dict."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Execute with invalid connection_params type
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params="not a dict",  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "connection_params must be a dictionary" in result["error"]


def test_configure_data_source_invalid_credentials_type():
    """Test configuration fails when credentials is not a dict."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Execute with invalid credentials type
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params={},
        credentials="not a dict",  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "credentials must be a dictionary" in result["error"]


def test_configure_data_source_invalid_field_overrides_type():
    """Test configuration fails when field_overrides is not a dict."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Execute with invalid field_overrides type
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params={},
        field_overrides=["not", "a", "dict"],  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "field_overrides must be a dictionary" in result["error"]


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
def test_configure_data_source_connection_exception(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test configuration when test_connection raises an exception."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.is_registered.return_value = True

    mock_connector = Mock()
    mock_connector.test_connection.side_effect = RuntimeError("Connection refused")
    mock_registry.create.return_value = mock_connector

    mock_config_class.return_value = Mock()

    # Execute
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params={},
    )

    # Verify
    assert result["success"] is False
    assert "Connection test error" in result["error"]
    assert "Connection refused" in result["error"]
    mock_connector.close.assert_called_once()


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorType")
def test_configure_data_source_unregistered(mock_connector_type_class, mock_get_registry):
    """Test configuration with unregistered connector type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.is_registered.return_value = False

    # Execute
    result = configure_data_source(
        customer_id="acme",
        connector_type="punchh",
        name="Punchh Loyalty",
        connection_params={},
    )

    # Verify
    assert result["success"] is False
    assert "Connector not available" in result["error"]


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
def test_configure_data_source_connection_failed(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test configuration when connection test fails."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    configure_data_source = mcp._tool_manager._tools["configure_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.is_registered.return_value = True

    mock_connector = Mock()
    mock_connector.test_connection.return_value = False
    mock_registry.create.return_value = mock_connector

    mock_config_class.return_value = Mock()

    # Execute
    result = configure_data_source(
        customer_id="acme",
        connector_type="snowflake",
        name="Test",
        connection_params={},
    )

    # Verify
    assert result["success"] is False
    assert "Connection test failed" in result["error"]


# =============================================================================
# discover_schema Tests
# =============================================================================


@pytest.mark.asyncio
@patch("growthnav.connectors.discovery.SchemaDiscovery")
@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
async def test_discover_schema_success(
    mock_connector_type_class, mock_config_class, mock_get_registry, mock_discovery_class
):
    """Test successful schema discovery."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_config_class.return_value = Mock()

    mock_connector = Mock()
    mock_connector.get_schema.return_value = {"amount": "FLOAT", "date": "DATE"}
    mock_connector.fetch_records.return_value = iter(
        [{"amount": 100.0, "date": "2024-01-01"}]
    )

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Setup discovery mock
    mock_suggestion = Mock()
    mock_suggestion.source_field = "amount"
    mock_suggestion.target_field = "transaction_value"
    mock_suggestion.confidence = 0.95
    mock_suggestion.reason = "Matches currency pattern"

    mock_discovery = Mock()
    mock_discovery.analyze = AsyncMock(
        return_value={
            "suggestions": [mock_suggestion],
            "field_map": {"amount": "transaction_value"},
            "confidence_summary": {"high": 1, "medium": 0, "low": 0},
        }
    )
    mock_discovery_class.return_value = mock_discovery

    # Execute
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={"account": "test"},
        credentials={"user": "test"},
        sample_size=10,
    )

    # Verify
    assert result["success"] is True
    assert result["source_schema"] == {"amount": "FLOAT", "date": "DATE"}
    assert len(result["suggested_mappings"]) == 1
    assert result["suggested_mappings"][0]["source"] == "amount"
    assert result["suggested_mappings"][0]["target"] == "transaction_value"
    assert result["suggested_mappings"][0]["confidence"] == 0.95
    mock_connector.authenticate.assert_called_once()
    mock_connector.close.assert_called_once()


@pytest.mark.asyncio
@patch("growthnav.connectors.ConnectorType")
async def test_discover_schema_invalid_type(mock_connector_type_class):
    """Test schema discovery with invalid connector type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Make ConnectorType raise ValueError
    mock_connector_type_class.side_effect = ValueError("Invalid type")

    # Execute
    result = await discover_schema(
        customer_id="acme",
        connector_type="invalid",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "Unknown connector type" in result["error"]


@pytest.mark.asyncio
async def test_discover_schema_invalid_connection_params_type():
    """Test schema discovery with invalid connection_params type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Execute with invalid connection_params type
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params="not a dict",  # type: ignore
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "connection_params must be a dictionary" in result["error"]


@pytest.mark.asyncio
async def test_discover_schema_invalid_credentials_type():
    """Test schema discovery with invalid credentials type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Execute with invalid credentials type
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials="not a dict",  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "credentials must be a dictionary" in result["error"]


@pytest.mark.asyncio
async def test_discover_schema_sample_size_too_small():
    """Test schema discovery with sample_size below minimum."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Execute with sample_size = 0
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
        sample_size=0,
    )

    # Verify
    assert result["success"] is False
    assert "sample_size must be between" in result["error"]


@pytest.mark.asyncio
async def test_discover_schema_sample_size_too_large():
    """Test schema discovery with sample_size above maximum."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Execute with sample_size = 20000
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
        sample_size=20000,
    )

    # Verify
    assert result["success"] is False
    assert "sample_size must be between" in result["error"]


@pytest.mark.asyncio
@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
async def test_discover_schema_auth_error(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test schema discovery when authentication fails."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_config_class.return_value = Mock()

    mock_connector = Mock()
    mock_connector.authenticate.side_effect = ConnectionError("Auth failed")

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "Auth failed" in result["error"]


@pytest.mark.asyncio
@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
async def test_discover_schema_empty_sample_data(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test schema discovery when sample data is empty."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_config_class.return_value = Mock()

    mock_connector = Mock()
    mock_connector.fetch_records.return_value = iter([])  # Empty iterator

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "No data available to sample" in result["error"]
    mock_connector.authenticate.assert_called_once()
    # Note: close() is not called because we return early before the finally block
    # The connector was created but the try block returns early


@pytest.mark.asyncio
@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.ConnectorType")
async def test_discover_schema_sanitizes_credentials_in_error(
    mock_connector_type_class, mock_config_class, mock_get_registry
):
    """Test that discover_schema sanitizes credentials in error messages."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    discover_schema = mcp._tool_manager._tools["discover_schema"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_config_class.return_value = Mock()

    mock_connector = Mock()
    # Simulate an error that might leak credentials
    mock_connector.authenticate.side_effect = RuntimeError(
        "Connection failed: password=supersecret123 token=abc123"
    )

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = await discover_schema(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
    )

    # Verify credentials are sanitized
    assert result["success"] is False
    assert "supersecret123" not in result["error"]
    assert "abc123" not in result["error"]
    assert "Connection failed" in result["error"]
    mock_connector.close.assert_called_once()


# =============================================================================
# sync_data_source Tests
# =============================================================================


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.SyncMode")
@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_full_sync(
    mock_connector_type_class, mock_sync_mode_class, mock_config_class, mock_get_registry
):
    """Test full data sync."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_full = Mock()
    mock_sync_mode_class.FULL = mock_full

    mock_config_class.return_value = Mock()

    mock_sync_result = Mock()
    mock_sync_result.success = True
    mock_sync_result.records_fetched = 100
    mock_sync_result.records_normalized = 98
    mock_sync_result.records_failed = 2
    mock_sync_result.duration_seconds = 5.5
    mock_sync_result.error = None

    mock_connector = Mock()
    mock_connector.sync.return_value = mock_sync_result

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={"account": "test"},
        credentials={"user": "test"},
    )

    # Verify
    assert result["success"] is True
    assert result["records_fetched"] == 100
    assert result["records_normalized"] == 98
    assert result["records_failed"] == 2
    assert result["duration_seconds"] == 5.5
    mock_connector.sync.assert_called_once_with(since=None)
    mock_connector.close.assert_called_once()


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.SyncMode")
@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_incremental(
    mock_connector_type_class, mock_sync_mode_class, mock_config_class, mock_get_registry
):
    """Test incremental data sync."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mocks
    mock_connector_type = Mock()
    mock_connector_type_class.return_value = mock_connector_type

    mock_incremental = Mock()
    mock_sync_mode_class.INCREMENTAL = mock_incremental

    mock_config_class.return_value = Mock()

    mock_sync_result = Mock()
    mock_sync_result.success = True
    mock_sync_result.records_fetched = 10
    mock_sync_result.records_normalized = 10
    mock_sync_result.records_failed = 0
    mock_sync_result.duration_seconds = 1.2
    mock_sync_result.error = None

    mock_connector = Mock()
    mock_connector.sync.return_value = mock_sync_result

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute with since parameter
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={"account": "test"},
        credentials={"user": "test"},
        since="2024-01-01T00:00:00",
    )

    # Verify incremental sync was called with datetime
    assert result["success"] is True
    assert result["records_fetched"] == 10

    # Verify since was parsed and passed
    call_args = mock_connector.sync.call_args
    since_arg = call_args.kwargs.get("since") or call_args[1].get("since")
    assert since_arg is not None
    assert isinstance(since_arg, datetime)


def test_sync_data_source_invalid_connection_params_type():
    """Test sync with invalid connection_params type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Execute with invalid connection_params type
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params="not a dict",  # type: ignore
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "connection_params must be a dictionary" in result["error"]


def test_sync_data_source_invalid_credentials_type():
    """Test sync with invalid credentials type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Execute with invalid credentials type
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials="not a dict",  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "credentials must be a dictionary" in result["error"]


def test_sync_data_source_invalid_field_overrides_type():
    """Test sync with invalid field_overrides type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Execute with invalid field_overrides type
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
        field_overrides=["not", "a", "dict"],  # type: ignore
    )

    # Verify
    assert result["success"] is False
    assert "field_overrides must be a dictionary" in result["error"]


@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_invalid_type(mock_connector_type_class):
    """Test sync with invalid connector type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Make ConnectorType raise ValueError
    mock_connector_type_class.side_effect = ValueError("Invalid type")

    # Execute
    result = sync_data_source(
        customer_id="acme",
        connector_type="invalid",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "Unknown connector type" in result["error"]


@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_invalid_datetime(mock_connector_type_class):
    """Test sync with invalid datetime format."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mock
    mock_connector_type_class.return_value = Mock()

    # Execute with invalid datetime
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
        since="not-a-valid-datetime",
    )

    # Verify
    assert result["success"] is False
    assert "Invalid ISO datetime format" in result["error"]
    assert "not-a-valid-datetime" in result["error"]


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.SyncMode")
@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_failure(
    mock_connector_type_class, mock_sync_mode_class, mock_config_class, mock_get_registry
):
    """Test sync when data sync fails."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_sync_mode_class.FULL = Mock()
    mock_config_class.return_value = Mock()

    mock_sync_result = Mock()
    mock_sync_result.success = False
    mock_sync_result.records_fetched = 50
    mock_sync_result.records_normalized = 0
    mock_sync_result.records_failed = 50
    mock_sync_result.duration_seconds = 10.0
    mock_sync_result.error = "Database connection lost"

    mock_connector = Mock()
    mock_connector.sync.return_value = mock_sync_result

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert result["records_fetched"] == 50
    assert result["records_failed"] == 50
    assert result["error"] == "Database connection lost"
    mock_connector.close.assert_called_once()


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.SyncMode")
@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_exception(
    mock_connector_type_class, mock_sync_mode_class, mock_config_class, mock_get_registry
):
    """Test sync when an exception is raised."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_sync_mode_class.FULL = Mock()
    mock_config_class.return_value = Mock()

    mock_connector = Mock()
    mock_connector.sync.side_effect = RuntimeError("Unexpected error")

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
    )

    # Verify
    assert result["success"] is False
    assert "Unexpected error" in result["error"]
    mock_connector.close.assert_called_once()


@patch("growthnav.connectors.get_registry")
@patch("growthnav.connectors.ConnectorConfig")
@patch("growthnav.connectors.SyncMode")
@patch("growthnav.connectors.ConnectorType")
def test_sync_data_source_with_field_overrides(
    mock_connector_type_class, mock_sync_mode_class, mock_config_class, mock_get_registry
):
    """Test sync with custom field overrides."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    sync_data_source = mcp._tool_manager._tools["sync_data_source"].fn

    # Setup mocks
    mock_connector_type_class.return_value = Mock()
    mock_sync_mode_class.FULL = Mock()

    mock_config_class.return_value = Mock()

    mock_sync_result = Mock()
    mock_sync_result.success = True
    mock_sync_result.records_fetched = 50
    mock_sync_result.records_normalized = 50
    mock_sync_result.records_failed = 0
    mock_sync_result.duration_seconds = 2.0
    mock_sync_result.error = None

    mock_connector = Mock()
    mock_connector.sync.return_value = mock_sync_result

    mock_registry = Mock()
    mock_get_registry.return_value = mock_registry
    mock_registry.create.return_value = mock_connector

    # Execute with field overrides
    result = sync_data_source(
        customer_id="acme",
        connector_type="snowflake",
        connection_params={},
        credentials={},
        field_overrides={"source_amount": "transaction_value"},
    )

    # Verify
    assert result["success"] is True

    # Verify field_overrides was passed to config
    config_call = mock_config_class.call_args
    assert config_call.kwargs["field_overrides"] == {"source_amount": "transaction_value"}


# =============================================================================
# Tool Registration Tests
# =============================================================================


def test_connector_tools_registered():
    """Test that all connector tools are registered in MCP server."""
    from growthnav_mcp.server import mcp

    tools = mcp._tool_manager._tools

    assert "list_connectors" in tools
    assert "configure_data_source" in tools
    assert "discover_schema" in tools
    assert "sync_data_source" in tools


def test_list_connectors_tool_metadata():
    """Test list_connectors tool has correct metadata."""
    from growthnav_mcp.server import mcp

    tool = mcp._tool_manager._tools["list_connectors"]
    assert "List all available connector types" in (tool.description or "")


def test_configure_data_source_tool_metadata():
    """Test configure_data_source tool has correct metadata."""
    from growthnav_mcp.server import mcp

    tool = mcp._tool_manager._tools["configure_data_source"]
    assert "Configure a new data source" in (tool.description or "")


def test_discover_schema_tool_metadata():
    """Test discover_schema tool has correct metadata."""
    from growthnav_mcp.server import mcp

    tool = mcp._tool_manager._tools["discover_schema"]
    assert "Discover and analyze schema" in (tool.description or "")


def test_sync_data_source_tool_metadata():
    """Test sync_data_source tool has correct metadata."""
    from growthnav_mcp.server import mcp

    tool = mcp._tool_manager._tools["sync_data_source"]
    assert "Sync data from a configured connector" in (tool.description or "")
