"""Connector registry for managing available connector types."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from growthnav.connectors.config import ConnectorConfig, ConnectorType

if TYPE_CHECKING:
    from growthnav.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class ConnectorRegistry:
    """Registry of available connector implementations.

    Singleton pattern for global connector type registration.

    Example:
        # Register a connector type
        registry = ConnectorRegistry()
        registry.register(ConnectorType.SNOWFLAKE, SnowflakeConnector)

        # Create a connector instance
        config = ConnectorConfig(
            connector_type=ConnectorType.SNOWFLAKE,
            customer_id="topgolf",
            name="Toast via Snowflake",
        )
        connector = registry.create(config)
    """

    _instance: ConnectorRegistry | None = None
    _connectors: dict[ConnectorType, type[BaseConnector]]

    def __new__(cls) -> ConnectorRegistry:
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connectors = {}
        return cls._instance

    def register(
        self,
        connector_type: ConnectorType,
        connector_class: type[BaseConnector],
    ) -> None:
        """Register a connector implementation.

        Args:
            connector_type: The type of connector.
            connector_class: The connector class to use.
        """
        self._connectors[connector_type] = connector_class
        logger.debug(f"Registered connector: {connector_type.value}")

    def unregister(self, connector_type: ConnectorType) -> None:
        """Unregister a connector type.

        Args:
            connector_type: The type to unregister.
        """
        if connector_type in self._connectors:
            del self._connectors[connector_type]

    def get(self, connector_type: ConnectorType) -> type[BaseConnector] | None:
        """Get a connector class by type.

        Args:
            connector_type: The connector type to look up.

        Returns:
            The connector class or None if not registered.
        """
        return self._connectors.get(connector_type)

    def create(self, config: ConnectorConfig) -> BaseConnector:
        """Create a connector instance from configuration.

        Args:
            config: Connector configuration.

        Returns:
            Initialized connector instance.

        Raises:
            ValueError: If connector type is not registered.
        """
        connector_class = self.get(config.connector_type)
        if connector_class is None:
            raise ValueError(
                f"No connector registered for type: {config.connector_type.value}"
            )
        return connector_class(config)

    def list_available(self) -> list[ConnectorType]:
        """List all registered connector types.

        Returns:
            List of available connector types.
        """
        return list(self._connectors.keys())

    def is_registered(self, connector_type: ConnectorType) -> bool:
        """Check if a connector type is registered.

        Args:
            connector_type: The type to check.

        Returns:
            True if registered.
        """
        return connector_type in self._connectors


# Global registry instance
_registry = ConnectorRegistry()


def get_registry() -> ConnectorRegistry:
    """Get the global connector registry."""
    return _registry
