"""Custom exceptions for connectors."""

from __future__ import annotations


class ConnectorError(Exception):
    """Base exception for connector errors."""

    pass


class AuthenticationError(ConnectorError):
    """Raised when authentication fails."""

    pass


class SyncError(ConnectorError):
    """Raised when sync operation fails."""

    pass


class SchemaError(ConnectorError):
    """Raised when schema validation or discovery fails."""

    pass


class ConnectionError(ConnectorError):
    """Raised when connection to external system fails."""

    pass
