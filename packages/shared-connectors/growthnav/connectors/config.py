"""Configuration models for data source connectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ConnectorType(str, Enum):
    """Supported connector types."""

    # Data Lakes
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"

    # POS (via data lakes typically)
    TOAST = "toast"
    SQUARE = "square"
    CLOVER = "clover"
    LIGHTSPEED = "lightspeed"

    # CRM
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    ZOHO = "zoho"

    # OLO
    OLO = "olo"
    OTTER = "otter"
    CHOWLY = "chowly"

    # Loyalty
    FISHBOWL = "fishbowl"
    PUNCHH = "punchh"


class SyncMode(str, Enum):
    """Data sync mode."""

    FULL = "full"  # Full refresh
    INCREMENTAL = "incremental"  # Only new/updated records


class SyncSchedule(str, Enum):
    """Sync frequency."""

    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MANUAL = "manual"


@dataclass
class ConnectorConfig:
    """Configuration for a data source connector."""

    connector_type: ConnectorType
    customer_id: str
    name: str  # Human-readable name for this connection

    # Authentication (repr=False to prevent credential exposure in logs)
    credentials_secret_path: str | None = None  # Secret Manager path
    credentials: dict[str, Any] = field(default_factory=dict, repr=False)

    # Connection settings
    connection_params: dict[str, Any] = field(default_factory=dict)

    # Field mapping overrides
    field_overrides: dict[str, str] = field(default_factory=dict)

    # Sync settings
    sync_mode: SyncMode = SyncMode.INCREMENTAL
    sync_schedule: SyncSchedule = SyncSchedule.DAILY
    last_sync: datetime | None = None
    last_sync_cursor: str | None = None  # For incremental syncs

    # Status
    is_active: bool = True
    error_message: str | None = None


@dataclass
class SyncResult:
    """Result of a data sync operation."""

    connector_name: str
    customer_id: str
    sync_mode: SyncMode
    started_at: datetime
    completed_at: datetime | None = None

    records_fetched: int = 0
    records_normalized: int = 0
    records_failed: int = 0

    success: bool = False
    error: str | None = None
    cursor: str | None = None  # For next incremental sync

    @property
    def duration_seconds(self) -> float | None:
        """Return sync duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
