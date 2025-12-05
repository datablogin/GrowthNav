"""Pytest fixtures for shared-connectors tests."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime
from typing import Any

import pytest
from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import (
    ConnectorConfig,
    ConnectorType,
    SyncMode,
)
from growthnav.connectors.registry import ConnectorRegistry


class MockConnector(BaseConnector):
    """Mock connector for testing."""

    connector_type = ConnectorType.SNOWFLAKE

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._mock_records: list[dict[str, Any]] = []
        self._mock_schema: dict[str, str] = {}

    def authenticate(self) -> None:
        """Mock authentication."""
        self._authenticated = True
        self._client = "mock_client"

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield mock records."""
        records = self._mock_records
        if limit:
            records = records[:limit]
        yield from records

    def get_schema(self) -> dict[str, str]:
        """Return mock schema."""
        return self._mock_schema

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Any]:
        """Return records as-is for testing."""
        return raw_records  # type: ignore[return-value]

    def set_mock_records(self, records: list[dict[str, Any]]) -> None:
        """Set mock records for testing."""
        self._mock_records = records

    def set_mock_schema(self, schema: dict[str, str]) -> None:
        """Set mock schema for testing."""
        self._mock_schema = schema


@pytest.fixture
def connector_config() -> ConnectorConfig:
    """Create a test connector configuration."""
    return ConnectorConfig(
        connector_type=ConnectorType.SNOWFLAKE,
        customer_id="test_customer",
        name="Test Connector",
        credentials={"user": "test_user", "password": "test_pass"},
        connection_params={
            "account": "test.snowflakecomputing.com",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
        },
        sync_mode=SyncMode.FULL,
    )


@pytest.fixture
def mock_connector(connector_config: ConnectorConfig) -> MockConnector:
    """Create a mock connector instance."""
    return MockConnector(connector_config)


@pytest.fixture
def fresh_registry() -> Generator[ConnectorRegistry, None, None]:
    """Create a fresh registry instance for testing.

    Resets the singleton after the test.
    """
    # Reset the singleton
    ConnectorRegistry._instance = None
    registry = ConnectorRegistry()
    yield registry
    # Reset again after test
    ConnectorRegistry._instance = None
