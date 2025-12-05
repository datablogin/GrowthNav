"""GrowthNav Data Source Connectors.

This package provides connector adapters for external data sources:
- Data Lakes (Snowflake, BigQuery)
- POS systems (Toast, Square via data lakes)
- CRM systems (Salesforce, HubSpot, Zoho)
- OLO platforms (OLO, Otter, Chowly)
- Loyalty programs (Fishbowl, Punchh)

Example:
    from growthnav.connectors import ConnectorConfig, ConnectorType, get_registry

    config = ConnectorConfig(
        connector_type=ConnectorType.SNOWFLAKE,
        customer_id="topgolf",
        name="Toast POS via Snowflake",
        connection_params={
            "account": "xxx.snowflakecomputing.com",
            "warehouse": "ANALYTICS_WH",
            "database": "TOAST_DATA",
        }
    )

    registry = get_registry()
    connector = registry.create(config)
    connector.authenticate()

    result = connector.sync()
    print(f"Synced {result.records_normalized} records")
"""

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import (
    ConnectorConfig,
    ConnectorType,
    SyncMode,
    SyncResult,
    SyncSchedule,
)
from growthnav.connectors.exceptions import (
    AuthenticationError,
    ConnectorConnectionError,
    ConnectorError,
    SchemaError,
    SyncError,
)
from growthnav.connectors.registry import ConnectorRegistry, get_registry

__all__ = [
    # Base
    "BaseConnector",
    # Config
    "ConnectorConfig",
    "ConnectorType",
    "SyncMode",
    "SyncResult",
    "SyncSchedule",
    # Exceptions
    "AuthenticationError",
    "ConnectorConnectionError",
    "ConnectorError",
    "SchemaError",
    "SyncError",
    # Registry
    "ConnectorRegistry",
    "get_registry",
]
