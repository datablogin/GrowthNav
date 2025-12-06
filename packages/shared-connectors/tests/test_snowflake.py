"""Tests for SnowflakeConnector."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode
from growthnav.connectors.exceptions import AuthenticationError, SchemaError


class TestAdaptersInit:
    """Tests for adapters __init__.py module."""

    def test_adapters_import_error_handling(self) -> None:
        """Test adapters module handles ImportError gracefully when snowflake not installed."""
        # Store original modules
        original_modules = {}
        modules_to_remove = [k for k in sys.modules if 'snowflake' in k or 'growthnav.connectors.adapters' in k]
        for key in modules_to_remove:
            original_modules[key] = sys.modules.pop(key, None)

        try:
            # Mock the import to fail for snowflake
            import builtins
            real_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if 'snowflake' in name:
                    raise ImportError("No module named 'snowflake'")
                return real_import(name, *args, **kwargs)

            with patch.object(builtins, '__import__', side_effect=mock_import):
                # Force reimport of adapters module
                if 'growthnav.connectors.adapters' in sys.modules:
                    del sys.modules['growthnav.connectors.adapters']

                # This should not raise - it should gracefully handle the ImportError
                import importlib
                adapters_module = importlib.import_module('growthnav.connectors.adapters')

                # __all__ should be empty when snowflake import fails
                assert adapters_module.__all__ == []
        finally:
            # Restore original modules
            for key, value in original_modules.items():
                if value is not None:
                    sys.modules[key] = value
            # Re-import to restore state
            import growthnav.connectors.adapters  # noqa: F401


@pytest.fixture
def snowflake_config() -> ConnectorConfig:
    """Create a Snowflake connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.SNOWFLAKE,
        customer_id="test_customer",
        name="Test Snowflake Connector",
        credentials={
            "user": "test_user",
            "password": "test_pass",
        },
        connection_params={
            "account": "test.snowflakecomputing.com",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "table": "TRANSACTIONS",
            "timestamp_column": "UPDATED_AT",
        },
        sync_mode=SyncMode.FULL,
    )


class TestSnowflakeConnector:
    """Tests for SnowflakeConnector."""

    def test_connector_type(self, snowflake_config) -> None:
        """Test connector has correct type."""
        with patch.dict("sys.modules", {"snowflake": MagicMock(), "snowflake.connector": MagicMock()}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            assert connector.connector_type == ConnectorType.SNOWFLAKE

    def test_authenticate_success(self, snowflake_config) -> None:
        """Test successful authentication."""
        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            assert connector.is_authenticated is True
            assert connector._client == mock_connection
            mock_snowflake.connector.connect.assert_called_once()

    def test_authenticate_failure(self, snowflake_config) -> None:
        """Test authentication failure raises AuthenticationError."""
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.side_effect = Exception("Connection failed")

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            with pytest.raises(AuthenticationError, match="Failed to authenticate"):
                connector.authenticate()

    def test_authenticate_missing_dependency(self, snowflake_config) -> None:
        """Test missing snowflake package raises ImportError."""
        # Mock sys.modules to simulate missing snowflake package
        original_modules = {}
        import sys
        for key in list(sys.modules.keys()):
            if key.startswith('snowflake'):
                original_modules[key] = sys.modules.pop(key)

        try:
            # Import after clearing snowflake from sys.modules
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector

            # Create a new instance and patch the import to fail
            connector = SnowflakeConnector(snowflake_config)

            # Patch builtins to make import fail
            import builtins
            real_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if 'snowflake' in name:
                    raise ImportError("No module named 'snowflake'")
                return real_import(name, *args, **kwargs)

            with (
                patch.object(builtins, '__import__', side_effect=mock_import),
                pytest.raises(ImportError, match="snowflake-connector-python is required"),
            ):
                connector.authenticate()
        finally:
            # Restore original modules
            sys.modules.update(original_modules)

    def test_fetch_records_basic(self, snowflake_config) -> None:
        """Test basic record fetching."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("AMOUNT",), ("UPDATED_AT",)]
        mock_cursor.__iter__ = lambda self: iter([
            (1, 100.0, datetime(2024, 1, 1, tzinfo=UTC)),
            (2, 200.0, datetime(2024, 1, 2, tzinfo=UTC)),
        ])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            records = list(connector.fetch_records())

            assert len(records) == 2
            assert records[0]["ID"] == 1
            assert records[0]["AMOUNT"] == 100.0
            assert records[1]["ID"] == 2

    def test_fetch_records_with_time_range(self, snowflake_config) -> None:
        """Test record fetching with time range."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",), ("UPDATED_AT",)]
        mock_cursor.__iter__ = lambda self: iter([])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            since = datetime(2024, 1, 1, tzinfo=UTC)
            until = datetime(2024, 1, 31, tzinfo=UTC)

            list(connector.fetch_records(since=since, until=until))

            # Verify the query was called with parameters
            mock_cursor.execute.assert_called()

    def test_fetch_records_with_limit(self, snowflake_config) -> None:
        """Test record fetching with limit."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",)]
        mock_cursor.__iter__ = lambda self: iter([(1,), (2,), (3,)])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            list(connector.fetch_records(limit=10))

            # Check that LIMIT was in the query
            call_args = mock_cursor.execute.call_args
            query = call_args[0][0]
            assert "LIMIT 10" in query

    def test_get_schema(self, snowflake_config) -> None:
        """Test schema retrieval."""
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = lambda self: iter([
            ("ID", "INTEGER"),
            ("AMOUNT", "DECIMAL(10,2)"),
            ("CREATED_AT", "TIMESTAMP"),
        ])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            schema = connector.get_schema()

            assert schema["ID"] == "INTEGER"
            assert schema["AMOUNT"] == "DECIMAL(10,2)"
            assert schema["CREATED_AT"] == "TIMESTAMP"
            mock_cursor.execute.assert_called_with("DESCRIBE TABLE TRANSACTIONS")

    def test_get_schema_failure(self, snowflake_config) -> None:
        """Test schema retrieval failure raises SchemaError."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Table not found")

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            with pytest.raises(SchemaError, match="Failed to get schema"):
                connector.get_schema()

    def test_normalize(self, snowflake_config) -> None:
        """Test record normalization."""
        mock_snowflake = MagicMock()

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            raw_records = [
                {
                    "order_id": "ORD-001",
                    "total": 150.0,
                    "created_at": "2024-01-15T10:30:00Z",
                    "customer_id": "CUST-123",
                },
                {
                    "order_id": "ORD-002",
                    "total": 250.0,
                    "created_at": "2024-01-16T11:00:00Z",
                    "customer_id": "CUST-456",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 2
            assert conversions[0].transaction_id == "ORD-001"
            assert conversions[0].value == 150.0
            assert conversions[0].customer_id == "test_customer"

    def test_normalize_with_field_overrides(self, snowflake_config) -> None:
        """Test normalization with custom field mappings."""
        snowflake_config.field_overrides = {
            "sale_id": "transaction_id",
            "sale_amount": "value",
            "sale_date": "timestamp",
        }

        mock_snowflake = MagicMock()

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            raw_records = [
                {
                    "sale_id": "SALE-001",
                    "sale_amount": 99.99,
                    "sale_date": "2024-01-15T10:30:00Z",
                },
            ]

            conversions = connector.normalize(raw_records)

            assert len(conversions) == 1
            assert conversions[0].transaction_id == "SALE-001"
            assert conversions[0].value == 99.99

    def test_cleanup_client(self, snowflake_config) -> None:
        """Test client cleanup closes connection."""
        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            connector.close()

            mock_connection.close.assert_called_once()
            assert connector._authenticated is False

    def test_context_manager(self, snowflake_config) -> None:
        """Test connector works as context manager."""
        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            with connector as ctx:
                assert ctx.is_authenticated is True

            mock_connection.close.assert_called_once()

    def test_auto_registration(self, snowflake_config) -> None:
        """Test connector is auto-registered with registry."""
        mock_snowflake = MagicMock()

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            from growthnav.connectors.registry import get_registry

            registry = get_registry()

            # The connector should be registered
            assert registry.is_registered(ConnectorType.SNOWFLAKE)

            # Should be able to create from registry
            connector = registry.create(snowflake_config)
            assert isinstance(connector, SnowflakeConnector)

    def test_default_table_name(self, snowflake_config) -> None:
        """Test default table name is TRANSACTIONS."""
        del snowflake_config.connection_params["table"]

        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",)]
        mock_cursor.__iter__ = lambda self: iter([])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            list(connector.fetch_records())

            call_args = mock_cursor.execute.call_args
            query = call_args[0][0]
            assert "FROM TRANSACTIONS" in query

    def test_default_schema_name(self, snowflake_config) -> None:
        """Test default schema is PUBLIC."""
        del snowflake_config.connection_params["schema"]

        mock_snowflake = MagicMock()

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            call_kwargs = mock_snowflake.connector.connect.call_args[1]
            assert call_kwargs["schema"] == "PUBLIC"

    def test_fetch_records_auto_authenticate(self, snowflake_config) -> None:
        """Test fetch_records authenticates if not already authenticated."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID",)]
        mock_cursor.__iter__ = lambda self: iter([(1,)])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            # Don't authenticate first
            assert connector.is_authenticated is False

            # Fetch should auto-authenticate
            list(connector.fetch_records())

            assert connector.is_authenticated is True
            mock_snowflake.connector.connect.assert_called_once()

    def test_get_schema_auto_authenticate(self, snowflake_config) -> None:
        """Test get_schema authenticates if not already authenticated."""
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = lambda self: iter([("ID", "INTEGER")])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            # Don't authenticate first
            assert connector.is_authenticated is False

            # get_schema should auto-authenticate
            connector.get_schema()

            assert connector.is_authenticated is True
            mock_snowflake.connector.connect.assert_called_once()

    def test_cleanup_client_with_error(self, snowflake_config) -> None:
        """Test client cleanup handles errors gracefully."""
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Connection already closed")

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            # Should not raise even though close() fails
            connector.close()

            # Should still mark as not authenticated
            assert connector._authenticated is False


class TestSQLIdentifierValidation:
    """Tests for SQL identifier validation."""

    def test_validate_identifier_valid_names(self) -> None:
        """Test valid SQL identifiers are accepted."""
        mock_snowflake = MagicMock()
        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import _validate_identifier

            # Valid identifiers
            assert _validate_identifier("TRANSACTIONS") == "TRANSACTIONS"
            assert _validate_identifier("my_table") == "my_table"
            assert _validate_identifier("Table123") == "Table123"
            assert _validate_identifier("_private") == "_private"
            assert _validate_identifier("A") == "A"

    def test_validate_identifier_invalid_names(self) -> None:
        """Test invalid SQL identifiers are rejected."""
        mock_snowflake = MagicMock()
        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import _validate_identifier

            # SQL injection attempts
            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("TRANSACTIONS; DROP TABLE users;--")

            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("table' OR '1'='1")

            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("123invalid")  # Can't start with number

            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("table-name")  # Hyphen not allowed

            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("table.name")  # Dot not allowed

            with pytest.raises(ValueError, match="Invalid SQL"):
                _validate_identifier("")  # Empty not allowed

    def test_fetch_records_with_invalid_table(self, snowflake_config) -> None:
        """Test fetch_records rejects invalid table names."""
        snowflake_config.connection_params["table"] = "TRANSACTIONS; DROP TABLE users;--"

        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            with pytest.raises(ValueError, match="Invalid SQL table"):
                list(connector.fetch_records())

    def test_fetch_records_with_invalid_timestamp_column(self, snowflake_config) -> None:
        """Test fetch_records rejects invalid timestamp column names."""
        snowflake_config.connection_params["timestamp_column"] = "col' OR '1'='1"

        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            with pytest.raises(ValueError, match="Invalid SQL column"):
                list(connector.fetch_records())

    def test_get_schema_with_invalid_table(self, snowflake_config) -> None:
        """Test get_schema rejects invalid table names."""
        snowflake_config.connection_params["table"] = "table; DROP TABLE x;--"

        mock_connection = MagicMock()
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)
            connector.authenticate()

            with pytest.raises(ValueError, match="Invalid SQL table"):
                connector.get_schema()


class TestSnowflakeConnectorSync:
    """Tests for SnowflakeConnector sync functionality."""

    def test_sync_success(self, snowflake_config) -> None:
        """Test successful sync operation."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("order_id",), ("total",), ("created_at",)]
        mock_cursor.__iter__ = lambda self: iter([
            ("ORD-001", 100.0, "2024-01-15T10:30:00Z"),
            ("ORD-002", 200.0, "2024-01-16T11:00:00Z"),
        ])

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_connection

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_snowflake.connector}):
            from growthnav.connectors.adapters.snowflake import SnowflakeConnector
            connector = SnowflakeConnector(snowflake_config)

            result = connector.sync()

            assert result.success is True
            assert result.records_fetched == 2
            assert result.records_normalized == 2
            assert result.connector_name == "Test Snowflake Connector"
