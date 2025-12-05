"""Tests for growthnav.connectors.registry."""

from __future__ import annotations

import pytest
from growthnav.connectors.config import ConnectorType
from growthnav.connectors.registry import ConnectorRegistry, get_registry


class TestConnectorRegistry:
    """Tests for ConnectorRegistry singleton."""

    def test_singleton_pattern(self) -> None:
        """Test registry is a singleton."""
        # Reset singleton for clean test
        ConnectorRegistry._instance = None

        registry1 = ConnectorRegistry()
        registry2 = ConnectorRegistry()

        assert registry1 is registry2

        # Clean up
        ConnectorRegistry._instance = None

    def test_register_connector(self, fresh_registry, mock_connector) -> None:
        """Test registering a connector type."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.SNOWFLAKE, MockConnector)

        assert fresh_registry.is_registered(ConnectorType.SNOWFLAKE)

    def test_unregister_connector(self, fresh_registry) -> None:
        """Test unregistering a connector type."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.SNOWFLAKE, MockConnector)
        assert fresh_registry.is_registered(ConnectorType.SNOWFLAKE)

        fresh_registry.unregister(ConnectorType.SNOWFLAKE)

        assert not fresh_registry.is_registered(ConnectorType.SNOWFLAKE)

    def test_unregister_nonexistent(self, fresh_registry) -> None:
        """Test unregistering a non-existent type doesn't raise."""
        fresh_registry.unregister(ConnectorType.SALESFORCE)
        # Should not raise

    def test_get_connector_class(self, fresh_registry) -> None:
        """Test getting a registered connector class."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.HUBSPOT, MockConnector)

        connector_class = fresh_registry.get(ConnectorType.HUBSPOT)

        assert connector_class is MockConnector

    def test_get_unregistered_returns_none(self, fresh_registry) -> None:
        """Test getting an unregistered type returns None."""
        result = fresh_registry.get(ConnectorType.ZOHO)
        assert result is None

    def test_create_connector(self, fresh_registry, connector_config) -> None:
        """Test creating a connector instance."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.SNOWFLAKE, MockConnector)

        connector = fresh_registry.create(connector_config)

        assert isinstance(connector, MockConnector)
        assert connector.config == connector_config

    def test_create_unregistered_raises(self, fresh_registry, connector_config) -> None:
        """Test creating an unregistered connector raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            fresh_registry.create(connector_config)

        assert "No connector registered for type" in str(exc_info.value)
        assert "snowflake" in str(exc_info.value)

    def test_list_available_empty(self, fresh_registry) -> None:
        """Test listing when no connectors registered."""
        available = fresh_registry.list_available()
        assert available == []

    def test_list_available_with_connectors(self, fresh_registry) -> None:
        """Test listing registered connector types."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.SNOWFLAKE, MockConnector)
        fresh_registry.register(ConnectorType.SALESFORCE, MockConnector)

        available = fresh_registry.list_available()

        assert len(available) == 2
        assert ConnectorType.SNOWFLAKE in available
        assert ConnectorType.SALESFORCE in available

    def test_is_registered_true(self, fresh_registry) -> None:
        """Test is_registered returns True for registered types."""
        from tests.conftest import MockConnector

        fresh_registry.register(ConnectorType.TOAST, MockConnector)

        assert fresh_registry.is_registered(ConnectorType.TOAST) is True

    def test_is_registered_false(self, fresh_registry) -> None:
        """Test is_registered returns False for unregistered types."""
        assert fresh_registry.is_registered(ConnectorType.OLO) is False


class TestGetRegistry:
    """Tests for get_registry function."""

    def test_get_registry_returns_singleton(self) -> None:
        """Test get_registry returns the global singleton."""
        # Reset singleton
        ConnectorRegistry._instance = None

        registry1 = get_registry()
        registry2 = get_registry()

        assert registry1 is registry2
        assert isinstance(registry1, ConnectorRegistry)

        # Clean up
        ConnectorRegistry._instance = None

    def test_get_registry_returns_consistent_instance(self) -> None:
        """Test get_registry always returns the same instance.

        Note: This test verifies that get_registry() consistently returns
        the module-level singleton instance across multiple calls.
        """
        # Multiple calls to get_registry should return the same instance
        registry1 = get_registry()
        registry2 = get_registry()

        assert registry1 is registry2

        # The instance should be a ConnectorRegistry
        assert isinstance(registry1, ConnectorRegistry)
