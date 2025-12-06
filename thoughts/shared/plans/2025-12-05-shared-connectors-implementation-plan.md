# Shared Connectors Implementation Plan

## Overview

This plan implements the `shared-connectors` package to bridge external data sources (POS, CRM, OLO, Data Lakes) with GrowthNav's existing normalization pipeline. The architecture provides connector adapters, LLM-assisted schema discovery, and probabilistic identity resolution using Splink.

## Current State Analysis

### What Exists

| Component | Location | Capabilities |
|-----------|----------|--------------|
| **OnboardingOrchestrator** | [orchestrator.py](../../packages/shared-onboarding/growthnav/onboarding/orchestrator.py) | Multi-step workflow with rollback, validation, status tracking |
| **DatasetProvisioner** | [provisioning.py](../../packages/shared-onboarding/growthnav/onboarding/provisioning.py) | BigQuery dataset creation |
| **POSNormalizer** | [normalizer.py:64-171](../../packages/shared-conversions/growthnav/conversions/normalizer.py) | Field mapping for POS data (dictionary-based) |
| **CRMNormalizer** | [normalizer.py:174-263](../../packages/shared-conversions/growthnav/conversions/normalizer.py) | Field mapping for CRM data |
| **Conversion Schema** | [schema.py](../../packages/shared-conversions/growthnav/conversions/schema.py) | Unified conversion model with attribution fields |
| **MCP Server** | [server.py](../../packages/mcp-server/growthnav_mcp/server.py) | FastMCP tools for BigQuery, reporting, normalization |

### Key Gaps

1. **No API Connectors**: Current system expects pre-transformed data; no direct integration with external systems
2. **No Automated Schema Discovery**: Normalizers require explicit field mappings; no LLM-assisted detection
3. **No Identity Resolution**: Single `user_id` field; no support for probabilistic linking across identity fragments
4. **No Data Source Configuration**: After onboarding, no workflow to connect customer data sources

## Desired End State

After this plan is complete:

1. **Connector Framework**: Abstract base class with registry for pluggable data source adapters
2. **Data Lake Connector**: Connect to Snowflake (where POS data like Toast/Fishbowl resides)
3. **CRM Connectors**: Direct connections to Salesforce, Zoho, HubSpot
4. **OLO Connector**: Online ordering platform integration
5. **LLM Schema Discovery**: Claude-powered semantic field detection and mapping suggestions
6. **Identity Resolution**: Splink-based probabilistic record linkage across identity fragments
7. **MCP Tools**: Full connector lifecycle management via MCP

### Verification

- All unit tests pass: `uv run pytest packages/shared-connectors/tests/ -v`
- Coverage >80%: `uv run pytest packages/shared-connectors/tests/ --cov=growthnav.connectors`
- Type checking passes: `uv run mypy packages/shared-connectors/`
- Linting passes: `uv run ruff check packages/shared-connectors/`
- Integration tests pass with credentials configured

## What We're NOT Doing

- **Real-time sync**: Pull-based scheduled syncs only (no webhooks)
- **Data transformation pipelines**: Connectors fetch and normalize; complex ETL is out of scope
- **UI for configuration**: MCP tools and programmatic API only
- **Media Mix Modeling**: Robyn/Meridian integration is future work
- **Customer Match export**: Google/Meta audience upload is future work

## Implementation Approach

Build incrementally with each phase delivering working functionality:

1. Foundation package structure and base patterns
2. Snowflake connector (covers Toast, Fishbowl via data lake)
3. LLM-assisted schema discovery
4. Identity resolution with Splink
5. CRM connectors (Salesforce, Zoho, HubSpot)
6. OLO connector
7. MCP tools and onboarding integration

---

## Phase 1: Foundation - shared-connectors Package

### Overview

Create the package structure, abstract base class, and connector registry pattern.

### Changes Required

#### 1. Package Structure

**Directory**: `packages/shared-connectors/`

```
packages/shared-connectors/
├── pyproject.toml
├── growthnav/
│   └── connectors/
│       ├── __init__.py
│       ├── base.py              # BaseConnector abstract class
│       ├── config.py            # ConnectorConfig, SyncConfig
│       ├── registry.py          # ConnectorRegistry singleton
│       ├── exceptions.py        # Custom exceptions
│       ├── adapters/
│       │   ├── __init__.py
│       │   └── # (connectors added in later phases)
│       ├── discovery/
│       │   ├── __init__.py
│       │   └── # (schema discovery added in Phase 3)
│       └── identity/
│           ├── __init__.py
│           └── # (identity resolution added in Phase 4)
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_base.py
    ├── test_config.py
    └── test_registry.py
```

#### 2. pyproject.toml

**File**: `packages/shared-connectors/pyproject.toml`

```toml
[project]
name = "growthnav-connectors"
version = "0.1.0"
description = "Data source connectors for GrowthNav"
requires-python = ">=3.11"
dependencies = [
    "growthnav-conversions",
    "growthnav-bigquery",
    "httpx>=0.27.0",
    "tenacity>=8.0.0",
    "python-dateutil>=2.8.0",
    "pydantic>=2.0.0",
]

[project.optional-dependencies]
snowflake = ["snowflake-connector-python>=3.0.0"]
salesforce = ["simple-salesforce>=1.12.0"]
hubspot = ["hubspot-api-client>=9.0.0"]
identity = ["splink>=4.0.0"]
llm = ["anthropic>=0.40.0"]
all = [
    "growthnav-connectors[snowflake,salesforce,hubspot,identity,llm]",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["growthnav"]
```

#### 3. ConnectorConfig

**File**: `packages/shared-connectors/growthnav/connectors/config.py`

```python
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

    # Authentication
    credentials_secret_path: str | None = None  # Secret Manager path
    credentials: dict[str, Any] = field(default_factory=dict)  # Direct credentials

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
```

#### 4. BaseConnector Abstract Class

**File**: `packages/shared-connectors/growthnav/connectors/base.py`

```python
"""Base connector abstract class."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generator

from growthnav.connectors.config import (
    ConnectorConfig,
    ConnectorType,
    SyncMode,
    SyncResult,
)

if TYPE_CHECKING:
    from growthnav.conversions import Conversion

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base class for data source connectors.

    Subclasses must implement:
    - authenticate(): Connect to the external system
    - fetch_records(): Yield raw records from the source
    - get_schema(): Return source system schema
    - normalize(): Convert raw records to Conversion objects

    Example:
        class SnowflakeConnector(BaseConnector):
            connector_type = ConnectorType.SNOWFLAKE

            def authenticate(self) -> None:
                self._client = snowflake.connector.connect(...)

            def fetch_records(self, since=None, until=None):
                cursor = self._client.cursor()
                cursor.execute("SELECT * FROM transactions WHERE ...")
                for row in cursor:
                    yield dict(row)
    """

    connector_type: ConnectorType

    def __init__(self, config: ConnectorConfig):
        """Initialize connector with configuration.

        Args:
            config: Connector configuration including credentials and settings.
        """
        self.config = config
        self._client: Any = None
        self._authenticated = False

    @property
    def is_authenticated(self) -> bool:
        """Return True if connector is authenticated."""
        return self._authenticated

    @abstractmethod
    def authenticate(self) -> None:
        """Authenticate with the external system.

        Raises:
            ConnectionError: If authentication fails.
        """
        pass

    @abstractmethod
    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield raw records from the source system.

        Args:
            since: Fetch records updated after this time (for incremental sync).
            until: Fetch records updated before this time.
            limit: Maximum records to fetch.

        Yields:
            Raw record dictionaries from the source system.
        """
        pass

    @abstractmethod
    def get_schema(self) -> dict[str, str]:
        """Return the source system's schema.

        Returns:
            Dictionary mapping column names to data types.
        """
        pass

    @abstractmethod
    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize raw records to Conversion objects.

        Args:
            raw_records: List of raw records from fetch_records().

        Returns:
            List of normalized Conversion objects.
        """
        pass

    def test_connection(self) -> bool:
        """Test if the connection is valid.

        Returns:
            True if connection is successful.
        """
        try:
            self.authenticate()
            return True
        except Exception as e:
            logger.warning(f"Connection test failed for {self.config.name}: {e}")
            return False

    def sync(
        self,
        mode: SyncMode | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> SyncResult:
        """Sync data from the source system.

        Args:
            mode: Sync mode (full or incremental). Defaults to config setting.
            since: Override start time for incremental sync.
            until: Override end time for sync.

        Returns:
            SyncResult with sync statistics.
        """
        mode = mode or self.config.sync_mode
        result = SyncResult(
            connector_name=self.config.name,
            customer_id=self.config.customer_id,
            sync_mode=mode,
            started_at=datetime.now(UTC),
        )

        try:
            # Authenticate if needed
            if not self.is_authenticated:
                self.authenticate()

            # Determine time range for incremental sync
            if mode == SyncMode.INCREMENTAL:
                since = since or self.config.last_sync

            # Fetch and normalize records
            raw_records = []
            for record in self.fetch_records(since=since, until=until):
                raw_records.append(record)
                result.records_fetched += 1

            conversions = self.normalize(raw_records)
            result.records_normalized = len(conversions)
            result.records_failed = result.records_fetched - result.records_normalized

            result.success = True
            result.completed_at = datetime.now(UTC)

            logger.info(
                f"Sync completed for {self.config.name}: "
                f"{result.records_fetched} fetched, {result.records_normalized} normalized"
            )

        except Exception as e:
            result.success = False
            result.error = str(e)
            result.completed_at = datetime.now(UTC)
            logger.exception(f"Sync failed for {self.config.name}")

        return result

    def close(self) -> None:
        """Close the connection and cleanup resources."""
        self._client = None
        self._authenticated = False
```

#### 5. ConnectorRegistry

**File**: `packages/shared-connectors/growthnav/connectors/registry.py`

```python
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
```

#### 6. Package __init__.py

**File**: `packages/shared-connectors/growthnav/connectors/__init__.py`

```python
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
from growthnav.connectors.registry import ConnectorRegistry, get_registry

__all__ = [
    "BaseConnector",
    "ConnectorConfig",
    "ConnectorRegistry",
    "ConnectorType",
    "SyncMode",
    "SyncResult",
    "SyncSchedule",
    "get_registry",
]
```

### Success Criteria

#### Automated Verification:
- [x] `uv sync` includes the new package
- [x] `uv run python -c "from growthnav.connectors import BaseConnector, ConnectorConfig"` works
- [x] `uv run pytest packages/shared-connectors/tests/ -v` passes (47 tests)
- [x] `uv run mypy packages/shared-connectors/growthnav/connectors/ --namespace-packages --explicit-package-bases` passes
- [x] `uv run ruff check packages/shared-connectors/` passes

#### Manual Verification:
- [ ] Package structure follows namespace pattern
- [ ] ConnectorRegistry singleton works correctly

**Implementation Note**: After completing this phase and all automated verification passes, pause here for confirmation before proceeding to Phase 2.

---

## Phase 2: Snowflake Connector ✅ COMPLETED

### Overview

Implement the Snowflake connector, which serves as the data lake layer for POS systems like Toast and loyalty platforms like Fishbowl.

### Implementation Summary

**Files Created/Modified:**
- `packages/shared-connectors/growthnav/connectors/adapters/snowflake.py` - Snowflake connector
- `packages/shared-connectors/growthnav/connectors/adapters/__init__.py` - Auto-registration
- `packages/shared-connectors/tests/test_snowflake.py` - 20 comprehensive tests

**Key Features:**
- Authentication with Snowflake using `snowflake-connector-python`
- Parameterized queries for incremental sync with `since`/`until` parameters
- Schema discovery via `DESCRIBE TABLE`
- Normalization using `POSNormalizer` from shared-conversions
- Proper cleanup with `_cleanup_client()` override
- Auto-registration with ConnectorRegistry on import
- Uses `AuthenticationError` and `SchemaError` custom exceptions

### Test Results (2025-12-05)

**Target:** Nothing Bundt Cakes Snowflake instance
- Account: `NOTHINGBUNDTCAKES-NOTHINGBUNDTCAKES`
- Database: `MARKETING_DB.MART_SALE`
- Tables: `DIM_BAKERIES`, `THANX_PURCHASES`, `THANX_MEMBERSHIPS`, etc.

**Results:**
- ✅ Connection successful
- ✅ Schema discovery works (18 columns in THANX_PURCHASES)
- ✅ Incremental sync with date range works (fetched records for Jan 2024)
- ✅ Full sync processed ~4 million records in 133 seconds
- ✅ Normalization with custom field mappings works

**Example Configuration:**
```python
config = ConnectorConfig(
    connector_type=ConnectorType.SNOWFLAKE,
    customer_id='nothingbundtcakes',
    name='Nothing Bundt Cakes - Thanx Purchases',
    credentials={
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
    },
    connection_params={
        'account': 'NOTHINGBUNDTCAKES-NOTHINGBUNDTCAKES',
        'warehouse': 'MARKETING_WH',
        'database': 'MARKETING_DB',
        'schema': 'MART_SALE',
        'role': 'MARKETING_ROLE',
        'table': 'THANX_PURCHASES',
        'timestamp_column': 'PURCHASED_AT',
    },
    field_overrides={
        'PURCHASE_ID': 'transaction_id',
        'USER_ID': 'user_id',
        'SETTLEMENT_AMOUNT': 'value',
        'PURCHASED_AT': 'timestamp',
        'LOCATION_ID': 'location_id',
    },
    sync_mode=SyncMode.INCREMENTAL,
)
```

### Connector Storage Architecture (Recommendation) ✅ IMPLEMENTED

During onboarding, connector configurations should be stored as follows:

#### 1. BigQuery Table for Connector Metadata ✅

**Table:** `growthnav_registry.connectors`

```sql
CREATE TABLE growthnav_registry.connectors (
    connector_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    connector_type STRING NOT NULL,  -- 'snowflake', 'salesforce', etc.
    name STRING NOT NULL,
    connection_params JSON,  -- account, warehouse, database, schema, table
    field_overrides JSON,    -- custom field mappings
    sync_mode STRING DEFAULT 'incremental',
    sync_schedule STRING DEFAULT 'daily',
    last_sync TIMESTAMP,
    last_sync_cursor STRING,
    credentials_secret_path STRING,  -- Reference to Secret Manager
    is_active BOOL DEFAULT TRUE,
    error_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

#### 2. Credentials in Secret Manager ✅

- **Path format:** `growthnav-{customer_id}-connector-{connector_id}`
- **Example:** `growthnav-nothingbundtcakes-connector-snowflake-thanx`
- **Contents:** JSON with `user`, `password`, and any other auth fields

#### 3. Extend OnboardingRequest ✅

```python
@dataclass
class DataSourceConfig:
    """Configuration for a data source during onboarding."""
    connector_type: ConnectorType
    name: str
    credentials: dict[str, str]
    connection_params: dict[str, Any]
    field_overrides: dict[str, str] | None = None
    sync_schedule: SyncSchedule = SyncSchedule.DAILY

@dataclass
class OnboardingRequest:
    # ... existing fields ...
    data_sources: list[DataSourceConfig] = field(default_factory=list)
```

#### 4. New Onboarding Step ✅

Add `CONFIGURING_DATA_SOURCES` step to `OnboardingOrchestrator`:

1. For each data source in request ✅
2. Generate connector_id ✅
3. Store credentials in Secret Manager (via `credentials_secret_path` reference) ✅
4. Store connector config in BigQuery (via `ConnectorStorage.save()`) ✅
5. Test connection (optional) - Deferred to Phase 3
6. Schedule initial sync - Deferred to Phase 3

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest packages/shared-connectors/tests/test_snowflake.py -v` passes (20 tests)
- [x] `uv run mypy packages/shared-connectors/` passes
- [x] `uv run ruff check packages/shared-connectors/` passes
- [x] Connector auto-registers on import

#### Connector Storage Verification (New)

- [x] `uv run pytest packages/shared-connectors/tests/test_storage.py -v` passes (19 tests)
- [x] `uv run pytest packages/shared-onboarding/tests/test_orchestrator.py::TestOnboardingOrchestratorDataSources -v` passes (8 tests)
- [x] `DataSourceConfig` dataclass exported from `growthnav.onboarding`
- [x] `ConnectorStorage` class exported from `growthnav.connectors`
- [x] `CONFIGURING_DATA_SOURCES` status in `OnboardingStatus` enum
- [x] `OnboardingOrchestrator` accepts `connector_storage` parameter

#### Manual Verification

- [x] Connection to test Snowflake instance succeeds (Nothing Bundt Cakes)
- [x] Records are fetched and normalized correctly (THANX_PURCHASES table)
- [x] Incremental sync respects `since` parameter (tested with Jan 2024 range)

#### Manual Verification for Connector Storage (New)

- [x] Test onboarding flow with data sources using mock storage
- [x] Verify connector configs can be saved and retrieved (tested with mock storage.save)
- [x] Test rollback behavior when data source configuration fails (covered by unit tests)

**Implementation Note**: Phase 2 is complete. Proceed to Phase 3 (LLM-Assisted Schema Discovery).

---

## Phase 3: LLM-Assisted Schema Discovery

### Overview

Implement schema discovery service using Claude for semantic field detection and mapping suggestions.

### Changes Required

#### 1. Column Profiler

**File**: `packages/shared-connectors/growthnav/connectors/discovery/profiler.py`

```python
"""Column profiling for schema discovery."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ColumnProfile:
    """Statistical profile of a data column."""

    name: str
    inferred_type: str  # "string", "number", "datetime", "boolean", "unknown"

    # Statistics
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0

    # Numeric stats (if applicable)
    min_value: float | None = None
    max_value: float | None = None
    mean_value: float | None = None

    # String stats (if applicable)
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None

    # Pattern detection
    detected_patterns: list[str] = field(default_factory=list)
    sample_values: list[Any] = field(default_factory=list)

    @property
    def null_percentage(self) -> float:
        """Return percentage of null values."""
        if self.total_count == 0:
            return 0.0
        return (self.null_count / self.total_count) * 100

    @property
    def unique_percentage(self) -> float:
        """Return percentage of unique values."""
        if self.total_count == 0:
            return 0.0
        return (self.unique_count / self.total_count) * 100


class ColumnProfiler:
    """Profiles columns to detect data types and patterns.

    Analyzes sample data to produce ColumnProfile objects with:
    - Data type inference
    - Statistical summaries
    - Pattern detection (email, phone, currency, date)
    - Sample values for LLM analysis

    Example:
        profiler = ColumnProfiler()
        profiles = profiler.profile(sample_data)

        for name, profile in profiles.items():
            print(f"{name}: {profile.inferred_type}")
            print(f"  Patterns: {profile.detected_patterns}")
    """

    # Common patterns for semantic detection
    PATTERNS = {
        "email": re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),
        "phone": re.compile(r"^[\d\s\-\(\)\+]{7,20}$"),
        "currency": re.compile(r"^[\$\€\£]?\s*[\d,]+\.?\d*$"),
        "date_iso": re.compile(r"^\d{4}-\d{2}-\d{2}"),
        "uuid": re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$", re.I),
        "gclid": re.compile(r"^[A-Za-z0-9_-]{30,100}$"),
        "url": re.compile(r"^https?://"),
    }

    def profile(
        self,
        data: list[dict[str, Any]],
        sample_size: int = 10,
    ) -> dict[str, ColumnProfile]:
        """Profile all columns in the data.

        Args:
            data: List of record dictionaries.
            sample_size: Number of sample values to keep per column.

        Returns:
            Dictionary mapping column names to ColumnProfile objects.
        """
        if not data:
            return {}

        # Collect all column names
        columns = set()
        for row in data:
            columns.update(row.keys())

        profiles = {}
        for col in columns:
            values = [row.get(col) for row in data if col in row]
            profiles[col] = self._profile_column(col, values, sample_size)

        return profiles

    def _profile_column(
        self,
        name: str,
        values: list[Any],
        sample_size: int,
    ) -> ColumnProfile:
        """Profile a single column."""
        profile = ColumnProfile(name=name)
        profile.total_count = len(values)

        # Filter non-null values
        non_null = [v for v in values if v is not None and v != ""]
        profile.null_count = profile.total_count - len(non_null)

        if not non_null:
            profile.inferred_type = "unknown"
            return profile

        # Unique count
        try:
            profile.unique_count = len(set(str(v) for v in non_null))
        except TypeError:
            profile.unique_count = len(non_null)

        # Sample values (diverse)
        unique_samples = list(dict.fromkeys(str(v) for v in non_null))[:sample_size]
        profile.sample_values = unique_samples

        # Infer type
        profile.inferred_type = self._infer_type(non_null)

        # Type-specific stats
        if profile.inferred_type == "number":
            numeric = [float(v) for v in non_null if self._is_numeric(v)]
            if numeric:
                profile.min_value = min(numeric)
                profile.max_value = max(numeric)
                profile.mean_value = sum(numeric) / len(numeric)

        elif profile.inferred_type == "string":
            lengths = [len(str(v)) for v in non_null]
            profile.min_length = min(lengths)
            profile.max_length = max(lengths)
            profile.avg_length = sum(lengths) / len(lengths)

        # Pattern detection
        profile.detected_patterns = self._detect_patterns(non_null)

        return profile

    def _infer_type(self, values: list[Any]) -> str:
        """Infer the most likely data type."""
        type_counts = {"number": 0, "datetime": 0, "boolean": 0, "string": 0}

        for v in values[:100]:  # Sample for performance
            if isinstance(v, bool):
                type_counts["boolean"] += 1
            elif isinstance(v, (int, float)):
                type_counts["number"] += 1
            elif isinstance(v, datetime):
                type_counts["datetime"] += 1
            elif self._is_numeric(v):
                type_counts["number"] += 1
            elif self._is_datetime(v):
                type_counts["datetime"] += 1
            else:
                type_counts["string"] += 1

        # Return most common type
        return max(type_counts, key=type_counts.get)

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        if isinstance(value, (int, float)):
            return True
        try:
            float(str(value).replace(",", "").replace("$", ""))
            return True
        except (ValueError, TypeError):
            return False

    def _is_datetime(self, value: Any) -> bool:
        """Check if value looks like a datetime."""
        if isinstance(value, datetime):
            return True
        s = str(value)
        return bool(self.PATTERNS["date_iso"].match(s))

    def _detect_patterns(self, values: list[Any]) -> list[str]:
        """Detect semantic patterns in values."""
        patterns = []
        sample = [str(v) for v in values[:50]]

        for pattern_name, regex in self.PATTERNS.items():
            matches = sum(1 for v in sample if regex.match(v))
            if matches / len(sample) > 0.5:  # >50% match
                patterns.append(pattern_name)

        return patterns
```

#### 2. LLM Schema Mapper

**File**: `packages/shared-connectors/growthnav/connectors/discovery/mapper.py`

```python
"""LLM-assisted schema mapping using Claude."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from growthnav.connectors.discovery.profiler import ColumnProfile, ColumnProfiler

logger = logging.getLogger(__name__)


@dataclass
class MappingSuggestion:
    """A suggested field mapping from source to target schema."""

    source_field: str
    target_field: str | None  # None if no mapping found
    confidence: float  # 0.0 to 1.0
    reason: str
    sample_values: list[Any]


class LLMSchemaMapper:
    """Uses Claude to map source schemas to target Conversion schema.

    Analyzes column profiles and sample data to suggest field mappings
    with confidence scores.

    Example:
        mapper = LLMSchemaMapper()
        suggestions = await mapper.suggest_mappings(
            source_profiles=profiles,
            sample_rows=data[:10],
        )

        for s in suggestions:
            print(f"{s.source_field} -> {s.target_field} ({s.confidence:.0%})")
    """

    # Target Conversion schema fields
    TARGET_SCHEMA = {
        "transaction_id": "Unique transaction/order identifier",
        "user_id": "Customer/user identifier (email, ID, loyalty number)",
        "timestamp": "Transaction date/time",
        "value": "Transaction monetary value/amount",
        "currency": "Currency code (USD, EUR, etc.)",
        "quantity": "Number of items/units",
        "product_id": "Product/SKU identifier",
        "product_name": "Product name/description",
        "product_category": "Product category/type",
        "location_id": "Store/location identifier",
        "location_name": "Store/location name",
        "gclid": "Google Click ID",
        "fbclid": "Facebook Click ID",
        "utm_source": "UTM source parameter",
        "utm_medium": "UTM medium parameter",
        "utm_campaign": "UTM campaign parameter",
    }

    def __init__(self, anthropic_client: Any | None = None):
        """Initialize mapper with optional Anthropic client.

        Args:
            anthropic_client: Anthropic client instance. If None, will create one.
        """
        self._client = anthropic_client

    @property
    def client(self) -> Any:
        """Lazy-initialize Anthropic client."""
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic()
            except ImportError:
                raise ImportError(
                    "anthropic is required for LLM schema mapping. "
                    "Install with: pip install growthnav-connectors[llm]"
                )
        return self._client

    async def suggest_mappings(
        self,
        source_profiles: dict[str, ColumnProfile],
        sample_rows: list[dict[str, Any]],
        context: str | None = None,
    ) -> list[MappingSuggestion]:
        """Suggest field mappings using Claude.

        Args:
            source_profiles: Column profiles from ColumnProfiler.
            sample_rows: Sample data rows for context.
            context: Optional context about the data source (e.g., "Toast POS data").

        Returns:
            List of mapping suggestions with confidence scores.
        """
        # Build prompt
        prompt = self._build_prompt(source_profiles, sample_rows, context)

        # Call Claude
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse response
        return self._parse_response(response.content[0].text, source_profiles)

    def _build_prompt(
        self,
        profiles: dict[str, ColumnProfile],
        sample_rows: list[dict[str, Any]],
        context: str | None,
    ) -> str:
        """Build the prompt for Claude."""
        # Format profiles
        profile_text = []
        for name, p in profiles.items():
            profile_text.append(
                f"- {name}: type={p.inferred_type}, "
                f"patterns={p.detected_patterns}, "
                f"samples={p.sample_values[:3]}"
            )

        # Format target schema
        target_text = [f"- {k}: {v}" for k, v in self.TARGET_SCHEMA.items()]

        # Format sample rows
        sample_text = json.dumps(sample_rows[:3], indent=2, default=str)

        prompt = f"""Analyze this source data schema and map it to the target Conversion schema.

## Context
{context or "POS/transaction data for CLV analysis"}

## Source Schema Profiles
{chr(10).join(profile_text)}

## Sample Data
```json
{sample_text}
```

## Target Conversion Schema
{chr(10).join(target_text)}

## Task
For each source column, determine if it maps to a target field.
Return a JSON array of mappings:

```json
[
  {{
    "source_field": "source_column_name",
    "target_field": "target_field_name or null if no match",
    "confidence": 0.0 to 1.0,
    "reason": "brief explanation"
  }}
]
```

Important:
- transaction_id, timestamp, and value are required for CLV
- user_id enables customer-level analysis
- Be conservative with low-confidence mappings
- Return null for target_field if no clear mapping exists
"""
        return prompt

    def _parse_response(
        self,
        response_text: str,
        profiles: dict[str, ColumnProfile],
    ) -> list[MappingSuggestion]:
        """Parse Claude's response into MappingSuggestion objects."""
        suggestions = []

        # Extract JSON from response
        try:
            # Find JSON array in response
            start = response_text.find("[")
            end = response_text.rfind("]") + 1
            if start >= 0 and end > start:
                json_text = response_text[start:end]
                mappings = json.loads(json_text)

                for m in mappings:
                    source = m.get("source_field", "")
                    profile = profiles.get(source)

                    suggestions.append(MappingSuggestion(
                        source_field=source,
                        target_field=m.get("target_field"),
                        confidence=float(m.get("confidence", 0)),
                        reason=m.get("reason", ""),
                        sample_values=profile.sample_values if profile else [],
                    ))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to parse LLM response: {e}")

        return suggestions


class SchemaDiscovery:
    """High-level schema discovery service.

    Combines column profiling with LLM-assisted mapping.

    Example:
        discovery = SchemaDiscovery()
        result = await discovery.analyze(
            data=sample_records,
            context="Toast POS transaction data"
        )

        for mapping in result.suggestions:
            if mapping.confidence > 0.8:
                print(f"Map {mapping.source_field} -> {mapping.target_field}")
    """

    def __init__(self):
        """Initialize discovery service."""
        self.profiler = ColumnProfiler()
        self.mapper = LLMSchemaMapper()

    async def analyze(
        self,
        data: list[dict[str, Any]],
        context: str | None = None,
    ) -> dict[str, Any]:
        """Analyze data and suggest schema mappings.

        Args:
            data: Sample data records.
            context: Optional context about the data source.

        Returns:
            Dictionary with profiles and mapping suggestions.
        """
        # Profile columns
        profiles = self.profiler.profile(data)

        # Get LLM suggestions
        suggestions = await self.mapper.suggest_mappings(
            source_profiles=profiles,
            sample_rows=data[:10],
            context=context,
        )

        # Build field map from high-confidence suggestions
        field_map = {}
        for s in suggestions:
            if s.target_field and s.confidence >= 0.7:
                field_map[s.source_field] = s.target_field

        return {
            "profiles": profiles,
            "suggestions": suggestions,
            "field_map": field_map,
            "confidence_summary": {
                "high": len([s for s in suggestions if s.confidence >= 0.8]),
                "medium": len([s for s in suggestions if 0.5 <= s.confidence < 0.8]),
                "low": len([s for s in suggestions if s.confidence < 0.5]),
            },
        }
```

#### 3. Discovery __init__.py

**File**: `packages/shared-connectors/growthnav/connectors/discovery/__init__.py`

```python
"""Schema discovery and mapping services."""

from growthnav.connectors.discovery.profiler import ColumnProfile, ColumnProfiler
from growthnav.connectors.discovery.mapper import (
    LLMSchemaMapper,
    MappingSuggestion,
    SchemaDiscovery,
)

__all__ = [
    "ColumnProfile",
    "ColumnProfiler",
    "LLMSchemaMapper",
    "MappingSuggestion",
    "SchemaDiscovery",
]
```

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest packages/shared-connectors/tests/test_profiler.py -v` passes (48 tests)
- [x] `uv run pytest packages/shared-connectors/tests/test_mapper.py -v` passes (24 tests)
- [x] Column profiler correctly detects patterns (email, currency, etc.)

#### Manual Verification:
- [x] LLM mapper returns reasonable suggestions for sample POS data
- [x] High-confidence mappings are accurate for core CLV fields (11/16 fields mapped with >= 70% confidence)
- [x] Field map can be used directly with POSNormalizer

**Implementation Note**: After completing this phase and all automated verification passes, pause here for confirmation before proceeding to Phase 4.

---

## Phase 4: Identity Resolution with Splink

### Overview

Implement probabilistic identity resolution using Splink to link customer records across multiple identity fragments (email, phone, hashed CC, loyalty ID).

### Changes Required

#### 1. Identity Fragment Schema

**File**: `packages/shared-connectors/growthnav/connectors/identity/fragments.py`

```python
"""Identity fragment models for cross-system linking."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class IdentityType(str, Enum):
    """Types of identity fragments."""

    EMAIL = "email"
    PHONE = "phone"
    HASHED_CC = "hashed_cc"  # Hashed credit card
    LOYALTY_ID = "loyalty_id"
    DEVICE_ID = "device_id"
    COOKIE_ID = "cookie_id"
    CUSTOMER_ID = "customer_id"  # Source system ID
    NAME_ZIP = "name_zip"  # Name + zip code composite


@dataclass
class IdentityFragment:
    """A single identity identifier from a source system.

    Represents one way a customer can be identified, with
    metadata about the source and confidence level.

    Example:
        fragment = IdentityFragment(
            fragment_type=IdentityType.EMAIL,
            fragment_value="jane.doe@example.com",
            source_system="olo",
            confidence=1.0,  # Exact match
        )
    """

    fragment_type: IdentityType
    fragment_value: str
    source_system: str | None = None
    confidence: float = 1.0  # 0.0 to 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        """Hash by type and value."""
        return hash((self.fragment_type, self.fragment_value.lower()))

    def __eq__(self, other: object) -> bool:
        """Compare by type and normalized value."""
        if not isinstance(other, IdentityFragment):
            return False
        return (
            self.fragment_type == other.fragment_type
            and self.fragment_value.lower() == other.fragment_value.lower()
        )


@dataclass
class ResolvedIdentity:
    """A resolved customer identity linking multiple fragments.

    Represents a single real-world person identified across
    multiple systems and identity types.

    Example:
        identity = ResolvedIdentity(
            global_id="uuid-123",
            fragments=[
                IdentityFragment(IdentityType.EMAIL, "jane@example.com"),
                IdentityFragment(IdentityType.PHONE, "555-0199"),
                IdentityFragment(IdentityType.HASHED_CC, "8x9s8d..."),
            ],
            match_probability=0.95,
        )
    """

    global_id: str  # GrowthNav unified customer ID
    fragments: list[IdentityFragment] = field(default_factory=list)
    match_probability: float = 1.0  # Overall confidence

    @property
    def emails(self) -> list[str]:
        """Return all email fragments."""
        return [f.fragment_value for f in self.fragments if f.fragment_type == IdentityType.EMAIL]

    @property
    def phones(self) -> list[str]:
        """Return all phone fragments."""
        return [f.fragment_value for f in self.fragments if f.fragment_type == IdentityType.PHONE]

    def has_fragment_type(self, fragment_type: IdentityType) -> bool:
        """Check if identity has a specific fragment type."""
        return any(f.fragment_type == fragment_type for f in self.fragments)
```

#### 2. Splink Identity Linker

**File**: `packages/shared-connectors/growthnav/connectors/identity/linker.py`

```python
"""Probabilistic identity resolution using Splink."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from growthnav.connectors.identity.fragments import (
    IdentityFragment,
    IdentityType,
    ResolvedIdentity,
)

logger = logging.getLogger(__name__)


class IdentityLinker:
    """Probabilistic identity resolution using Splink.

    Links customer records across systems using fuzzy matching
    on identity fragments (email, phone, name, etc.).

    Uses the Fellegi-Sunter model:
    - m-probability: P(match | records are same person)
    - u-probability: P(match | records are different people)

    Example:
        linker = IdentityLinker()
        linker.add_records(pos_data, source="toast")
        linker.add_records(olo_data, source="olo")

        identities = linker.resolve()
        for identity in identities:
            print(f"Global ID: {identity.global_id}")
            print(f"  Emails: {identity.emails}")
            print(f"  Match confidence: {identity.match_probability:.0%}")
    """

    # Splink comparison settings for identity matching
    COMPARISON_SETTINGS = {
        "email": {
            "comparison_type": "levenshtein",
            "threshold": 2,  # Max edit distance
        },
        "phone": {
            "comparison_type": "exact",
            # Phone numbers should match exactly after normalization
        },
        "name": {
            "comparison_type": "jaro_winkler",
            "threshold": 0.88,
        },
    }

    def __init__(self):
        """Initialize identity linker."""
        self._records: list[dict[str, Any]] = []
        self._linker = None
        self._model_trained = False

    def add_records(
        self,
        records: list[dict[str, Any]],
        source: str,
        id_column: str = "id",
    ) -> None:
        """Add records from a source system for linking.

        Args:
            records: List of record dictionaries.
            source: Source system identifier (e.g., "toast", "olo").
            id_column: Column containing the source record ID.
        """
        for record in records:
            # Normalize and extract identity fields
            normalized = {
                "source_system": source,
                "source_id": str(record.get(id_column, "")),
                "email": self._normalize_email(record),
                "phone": self._normalize_phone(record),
                "first_name": self._normalize_name(record.get("first_name")),
                "last_name": self._normalize_name(record.get("last_name")),
                "hashed_cc": record.get("hashed_cc") or record.get("cc_hash"),
                "loyalty_id": record.get("loyalty_id") or record.get("member_id"),
            }
            self._records.append(normalized)

    def _normalize_email(self, record: dict[str, Any]) -> str | None:
        """Normalize email address."""
        email = record.get("email") or record.get("email_address")
        if email:
            return email.lower().strip()
        return None

    def _normalize_phone(self, record: dict[str, Any]) -> str | None:
        """Normalize phone number to digits only."""
        phone = record.get("phone") or record.get("phone_number")
        if phone:
            # Remove all non-digits
            digits = "".join(c for c in str(phone) if c.isdigit())
            # Keep last 10 digits (US format)
            if len(digits) >= 10:
                return digits[-10:]
        return None

    def _normalize_name(self, name: str | None) -> str | None:
        """Normalize name for comparison."""
        if name:
            return name.lower().strip()
        return None

    def resolve(
        self,
        match_threshold: float = 0.7,
    ) -> list[ResolvedIdentity]:
        """Resolve identities across all added records.

        Args:
            match_threshold: Minimum probability to consider a match.

        Returns:
            List of resolved identities with linked fragments.
        """
        if not self._records:
            return []

        try:
            import splink
            from splink import DuckDBAPI, Linker, SettingsCreator
            from splink import block_on
            import splink.comparison_library as cl
        except ImportError:
            raise ImportError(
                "splink is required for identity resolution. "
                "Install with: pip install growthnav-connectors[identity]"
            )

        # Build Splink settings
        settings = SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.EmailComparison("email"),
                cl.ExactMatch("phone").configure(term_frequency_adjustments=True),
                cl.JaroWinklerAtThresholds("first_name", [0.88]),
                cl.JaroWinklerAtThresholds("last_name", [0.88]),
                cl.ExactMatch("hashed_cc"),
                cl.ExactMatch("loyalty_id"),
            ],
            blocking_rules_to_generate_predictions=[
                block_on("email"),
                block_on("phone"),
                block_on("hashed_cc"),
                block_on("loyalty_id"),
                block_on("first_name", "last_name"),
            ],
        )

        # Create linker
        db_api = DuckDBAPI()
        linker = Linker(self._records, settings, db_api)

        # Train model using EM algorithm
        linker.training.estimate_probability_two_random_records_match(
            ["email", "phone"],
            recall=0.8,
        )
        linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

        # Train on blocking rules
        for rule in ["block_on(email)", "block_on(phone)"]:
            try:
                linker.training.estimate_parameters_using_expectation_maximisation(rule)
            except Exception:
                pass  # Skip if not enough data

        # Get predictions
        predictions = linker.inference.predict(threshold_match_probability=match_threshold)
        clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
            predictions,
            threshold_match_probability=match_threshold,
        )

        # Convert clusters to ResolvedIdentity objects
        return self._build_identities(clusters.as_pandas_dataframe())

    def _build_identities(self, cluster_df: Any) -> list[ResolvedIdentity]:
        """Build ResolvedIdentity objects from Splink clusters."""
        identities = []

        # Group by cluster_id
        for cluster_id, group in cluster_df.groupby("cluster_id"):
            fragments = []

            for _, row in group.iterrows():
                source = row.get("source_system", "unknown")

                # Add email fragment
                if row.get("email"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.EMAIL,
                        fragment_value=row["email"],
                        source_system=source,
                    ))

                # Add phone fragment
                if row.get("phone"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.PHONE,
                        fragment_value=row["phone"],
                        source_system=source,
                    ))

                # Add hashed CC fragment
                if row.get("hashed_cc"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.HASHED_CC,
                        fragment_value=row["hashed_cc"],
                        source_system=source,
                    ))

                # Add loyalty ID fragment
                if row.get("loyalty_id"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.LOYALTY_ID,
                        fragment_value=row["loyalty_id"],
                        source_system=source,
                    ))

            # Deduplicate fragments
            unique_fragments = list(set(fragments))

            identities.append(ResolvedIdentity(
                global_id=str(uuid.uuid4()),
                fragments=unique_fragments,
                match_probability=0.9,  # Would come from cluster confidence
            ))

        return identities

    def resolve_deterministic(self) -> list[ResolvedIdentity]:
        """Resolve identities using exact matching only.

        Faster alternative when probabilistic matching isn't needed.
        Links records with exact email, phone, or other ID matches.

        Returns:
            List of resolved identities.
        """
        # Build identity graph with exact matches
        from collections import defaultdict

        # Map identity values to record indices
        email_map: dict[str, list[int]] = defaultdict(list)
        phone_map: dict[str, list[int]] = defaultdict(list)
        hashed_cc_map: dict[str, list[int]] = defaultdict(list)
        loyalty_map: dict[str, list[int]] = defaultdict(list)

        for i, record in enumerate(self._records):
            if record.get("email"):
                email_map[record["email"]].append(i)
            if record.get("phone"):
                phone_map[record["phone"]].append(i)
            if record.get("hashed_cc"):
                hashed_cc_map[record["hashed_cc"]].append(i)
            if record.get("loyalty_id"):
                loyalty_map[record["loyalty_id"]].append(i)

        # Union-find for transitive linking
        parent = list(range(len(self._records)))

        def find(x: int) -> int:
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: int, y: int) -> None:
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Link by exact matches
        for indices in email_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in phone_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in hashed_cc_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        for indices in loyalty_map.values():
            for i in range(1, len(indices)):
                union(indices[0], indices[i])

        # Group by cluster
        clusters: dict[int, list[int]] = defaultdict(list)
        for i in range(len(self._records)):
            clusters[find(i)].append(i)

        # Build identities
        identities = []
        for indices in clusters.values():
            fragments = []
            for i in indices:
                record = self._records[i]
                source = record.get("source_system", "unknown")

                if record.get("email"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.EMAIL,
                        fragment_value=record["email"],
                        source_system=source,
                    ))
                if record.get("phone"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.PHONE,
                        fragment_value=record["phone"],
                        source_system=source,
                    ))
                if record.get("hashed_cc"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.HASHED_CC,
                        fragment_value=record["hashed_cc"],
                        source_system=source,
                    ))
                if record.get("loyalty_id"):
                    fragments.append(IdentityFragment(
                        fragment_type=IdentityType.LOYALTY_ID,
                        fragment_value=record["loyalty_id"],
                        source_system=source,
                    ))

            unique_fragments = list(set(fragments))
            identities.append(ResolvedIdentity(
                global_id=str(uuid.uuid4()),
                fragments=unique_fragments,
                match_probability=1.0,  # Exact matches
            ))

        return identities
```

#### 3. Identity __init__.py

**File**: `packages/shared-connectors/growthnav/connectors/identity/__init__.py`

```python
"""Identity resolution services."""

from growthnav.connectors.identity.fragments import (
    IdentityFragment,
    IdentityType,
    ResolvedIdentity,
)
from growthnav.connectors.identity.linker import IdentityLinker

__all__ = [
    "IdentityFragment",
    "IdentityLinker",
    "IdentityType",
    "ResolvedIdentity",
]
```

#### 4. Update Conversion Schema

Extend the Conversion dataclass to support identity fragments.

**File**: Update `packages/shared-conversions/growthnav/conversions/schema.py`

Add to the Conversion dataclass:

```python
# Add import at top
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from growthnav.connectors.identity import IdentityFragment

# Add field to Conversion dataclass (after user_id)
    # Identity resolution (populated after linking)
    identity_fragments: list["IdentityFragment"] = field(default_factory=list)
    global_customer_id: str | None = None  # Resolved cross-system ID
```

### Success Criteria

#### Automated Verification:
- [x] `uv run pytest packages/shared-connectors/tests/test_identity.py -v` passes (33 passed, 4 skipped)
- [x] Deterministic linking correctly clusters exact matches
- [x] Probabilistic linking runs without errors (with Splink installed) - skipped when Splink not installed

#### Manual Verification:
- [x] Sample data with overlapping emails/phones is correctly linked (tested with NBC Snowflake data - CARDFINGERPRINT links multiple emails)
- [x] Transitive linking works (A-email-B, B-phone-C → A,B,C linked) - verified in unit tests
- [x] ResolvedIdentity objects contain correct fragments (verified with real Snowflake data)

**Implementation Note**: After completing this phase and all automated verification passes, pause here for confirmation before proceeding to Phase 5.

---

## Phase 5: CRM Connectors

### Overview

Implement direct connectors for Salesforce, Zoho, and HubSpot CRM systems.

### Changes Required

#### 1. Salesforce Connector

**File**: `packages/shared-connectors/growthnav/connectors/adapters/salesforce.py`

```python
"""Salesforce CRM connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Generator

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)


class SalesforceConnector(BaseConnector):
    """Connector for Salesforce CRM.

    Fetches Opportunities, Leads, and Accounts from Salesforce
    and normalizes them to Conversions.

    Required credentials:
        - username: Salesforce username
        - password: Salesforce password
        - security_token: Salesforce security token

    Optional connection_params:
        - domain: "login" (production) or "test" (sandbox)
        - object_type: "Opportunity", "Lead", or "Account" (default: Opportunity)
        - query: Custom SOQL query (overrides object_type)

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.SALESFORCE,
            customer_id="acme",
            name="Salesforce Opportunities",
            credentials={
                "username": "user@company.com",
                "password": "password",
                "security_token": "token123",
            },
            connection_params={
                "domain": "login",
                "object_type": "Opportunity",
            }
        )
    """

    connector_type = ConnectorType.SALESFORCE

    def __init__(self, config: ConnectorConfig):
        """Initialize Salesforce connector."""
        super().__init__(config)

    def authenticate(self) -> None:
        """Connect to Salesforce."""
        try:
            from simple_salesforce import Salesforce
        except ImportError:
            raise ImportError(
                "simple-salesforce is required. "
                "Install with: pip install growthnav-connectors[salesforce]"
            )

        creds = self.config.credentials
        params = self.config.connection_params

        self._client = Salesforce(
            username=creds["username"],
            password=creds["password"],
            security_token=creds.get("security_token", ""),
            domain=params.get("domain", "login"),
        )
        self._authenticated = True
        logger.info("Connected to Salesforce")

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Salesforce."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        # Build SOQL query
        if "query" in params:
            query = params["query"]
        else:
            fields = self._get_fields_for_object(object_type)
            query = f"SELECT {', '.join(fields)} FROM {object_type}"

            conditions = []
            if since:
                conditions.append(f"LastModifiedDate >= {since.isoformat()}")
            if until:
                conditions.append(f"LastModifiedDate <= {until.isoformat()}")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY LastModifiedDate"

            if limit:
                query += f" LIMIT {limit}"

        logger.debug(f"Executing SOQL: {query}")

        # Execute query with pagination
        result = self._client.query(query)
        while True:
            for record in result.get("records", []):
                # Remove Salesforce metadata
                record.pop("attributes", None)
                yield record

            if result.get("done"):
                break
            result = self._client.query_more(result["nextRecordsUrl"])

    def _get_fields_for_object(self, object_type: str) -> list[str]:
        """Get relevant fields for an object type."""
        common_fields = ["Id", "Name", "CreatedDate", "LastModifiedDate"]

        if object_type == "Opportunity":
            return common_fields + [
                "Amount", "CloseDate", "StageName", "IsClosed", "IsWon",
                "AccountId", "ContactId", "LeadSource",
            ]
        elif object_type == "Lead":
            return common_fields + [
                "Email", "Phone", "Company", "Status", "LeadSource",
                "ConvertedDate", "ConvertedOpportunityId",
            ]
        elif object_type == "Account":
            return common_fields + [
                "Industry", "AnnualRevenue", "NumberOfEmployees",
                "BillingCity", "BillingState", "BillingCountry",
            ]
        else:
            return common_fields

    def get_schema(self) -> dict[str, str]:
        """Get schema for the configured object type."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        describe = self._client.__getattr__(object_type).describe()
        return {
            field["name"]: field["type"]
            for field in describe["fields"]
        }

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize Salesforce records to Conversions."""
        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        # Determine conversion type based on object
        if object_type == "Opportunity":
            conversion_type = ConversionType.PURCHASE
        elif object_type == "Lead":
            conversion_type = ConversionType.LEAD
        else:
            conversion_type = ConversionType.CUSTOM

        # Build field map for Salesforce fields
        field_map = {
            "Id": "transaction_id",
            "Amount": "value",
            "CloseDate": "timestamp",
            "CreatedDate": "timestamp",
            "AccountId": "user_id",
            "ContactId": "user_id",
            "Email": "user_id",
            "LeadSource": "utm_source",
        }
        field_map.update(self.config.field_overrides)

        normalizer = CRMNormalizer(
            customer_id=self.config.customer_id,
            conversion_type=conversion_type,
            field_map=field_map,
        )
        return normalizer.normalize(raw_records)


# Auto-register connector
get_registry().register(ConnectorType.SALESFORCE, SalesforceConnector)
```

#### 2. HubSpot Connector

**File**: `packages/shared-connectors/growthnav/connectors/adapters/hubspot.py`

```python
"""HubSpot CRM connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Generator

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)


class HubSpotConnector(BaseConnector):
    """Connector for HubSpot CRM.

    Fetches Deals, Contacts, and Companies from HubSpot
    and normalizes them to Conversions.

    Required credentials:
        - access_token: HubSpot private app access token

    Optional connection_params:
        - object_type: "deals", "contacts", or "companies" (default: deals)

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.HUBSPOT,
            customer_id="acme",
            name="HubSpot Deals",
            credentials={
                "access_token": "pat-na1-xxx",
            },
            connection_params={
                "object_type": "deals",
            }
        )
    """

    connector_type = ConnectorType.HUBSPOT

    def __init__(self, config: ConnectorConfig):
        """Initialize HubSpot connector."""
        super().__init__(config)

    def authenticate(self) -> None:
        """Connect to HubSpot."""
        try:
            from hubspot import HubSpot
        except ImportError:
            raise ImportError(
                "hubspot-api-client is required. "
                "Install with: pip install growthnav-connectors[hubspot]"
            )

        creds = self.config.credentials
        self._client = HubSpot(access_token=creds["access_token"])
        self._authenticated = True
        logger.info("Connected to HubSpot")

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from HubSpot."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "deals")

        # Get the appropriate API
        if object_type == "deals":
            api = self._client.crm.deals.basic_api
            properties = ["dealname", "amount", "closedate", "dealstage", "pipeline"]
        elif object_type == "contacts":
            api = self._client.crm.contacts.basic_api
            properties = ["email", "firstname", "lastname", "phone", "company"]
        elif object_type == "companies":
            api = self._client.crm.companies.basic_api
            properties = ["name", "domain", "industry", "annualrevenue"]
        else:
            raise ValueError(f"Unsupported object type: {object_type}")

        # Fetch with pagination
        after = None
        count = 0
        while True:
            response = api.get_page(
                limit=100,
                properties=properties,
                after=after,
            )

            for result in response.results:
                record = {"id": result.id, **result.properties}

                # Filter by date if specified
                updated = result.properties.get("hs_lastmodifieddate")
                if updated:
                    updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    if since and updated_dt < since:
                        continue
                    if until and updated_dt > until:
                        continue

                yield record
                count += 1

                if limit and count >= limit:
                    return

            if not response.paging or not response.paging.next:
                break
            after = response.paging.next.after

    def get_schema(self) -> dict[str, str]:
        """Get schema for the configured object type."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "deals")

        # Get properties for object type
        if object_type == "deals":
            api = self._client.crm.properties.core_api
            response = api.get_all(object_type="deals")
        elif object_type == "contacts":
            response = self._client.crm.properties.core_api.get_all(object_type="contacts")
        else:
            response = self._client.crm.properties.core_api.get_all(object_type="companies")

        return {
            prop.name: prop.type
            for prop in response.results
        }

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize HubSpot records to Conversions."""
        params = self.config.connection_params
        object_type = params.get("object_type", "deals")

        # Determine conversion type
        if object_type == "deals":
            conversion_type = ConversionType.PURCHASE
        elif object_type == "contacts":
            conversion_type = ConversionType.LEAD
        else:
            conversion_type = ConversionType.CUSTOM

        # Build field map for HubSpot fields
        field_map = {
            "id": "transaction_id",
            "amount": "value",
            "closedate": "timestamp",
            "email": "user_id",
            "hs_object_id": "transaction_id",
        }
        field_map.update(self.config.field_overrides)

        normalizer = CRMNormalizer(
            customer_id=self.config.customer_id,
            conversion_type=conversion_type,
            field_map=field_map,
        )
        return normalizer.normalize(raw_records)


# Auto-register connector
get_registry().register(ConnectorType.HUBSPOT, HubSpotConnector)
```

#### 3. Zoho Connector

**File**: `packages/shared-connectors/growthnav/connectors/adapters/zoho.py`

```python
"""Zoho CRM connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Generator

import httpx

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)


class ZohoConnector(BaseConnector):
    """Connector for Zoho CRM.

    Fetches Deals, Leads, and Accounts from Zoho CRM.

    Required credentials:
        - client_id: Zoho OAuth client ID
        - client_secret: Zoho OAuth client secret
        - refresh_token: Zoho OAuth refresh token

    Optional connection_params:
        - module: "Deals", "Leads", or "Accounts" (default: Deals)
        - domain: API domain (default: zohoapis.com)

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.ZOHO,
            customer_id="acme",
            name="Zoho Deals",
            credentials={
                "client_id": "xxx",
                "client_secret": "xxx",
                "refresh_token": "xxx",
            },
            connection_params={
                "module": "Deals",
            }
        )
    """

    connector_type = ConnectorType.ZOHO

    def __init__(self, config: ConnectorConfig):
        """Initialize Zoho connector."""
        super().__init__(config)
        self._access_token: str | None = None

    def authenticate(self) -> None:
        """Get access token from Zoho."""
        creds = self.config.credentials
        params = self.config.connection_params
        domain = params.get("domain", "zohoapis.com")

        # Refresh access token
        token_url = f"https://accounts.{domain}/oauth/v2/token"

        with httpx.Client() as client:
            response = client.post(
                token_url,
                data={
                    "grant_type": "refresh_token",
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                    "refresh_token": creds["refresh_token"],
                },
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data["access_token"]

        self._client = httpx.Client(
            base_url=f"https://www.{domain}/crm/v3",
            headers={"Authorization": f"Zoho-oauthtoken {self._access_token}"},
        )
        self._authenticated = True
        logger.info("Connected to Zoho CRM")

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Zoho."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        module = params.get("module", "Deals")

        # Fetch with pagination
        page = 1
        count = 0
        while True:
            response = self._client.get(
                f"/{module}",
                params={
                    "page": page,
                    "per_page": 200,
                },
            )
            response.raise_for_status()
            data = response.json()

            records = data.get("data", [])
            if not records:
                break

            for record in records:
                # Filter by date if specified
                modified = record.get("Modified_Time")
                if modified:
                    modified_dt = datetime.fromisoformat(modified.replace("Z", "+00:00"))
                    if since and modified_dt < since:
                        continue
                    if until and modified_dt > until:
                        continue

                yield record
                count += 1

                if limit and count >= limit:
                    return

            # Check for more pages
            info = data.get("info", {})
            if not info.get("more_records"):
                break
            page += 1

    def get_schema(self) -> dict[str, str]:
        """Get schema for the configured module."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        module = params.get("module", "Deals")

        response = self._client.get(f"/settings/fields", params={"module": module})
        response.raise_for_status()

        return {
            field["api_name"]: field["data_type"]
            for field in response.json().get("fields", [])
        }

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize Zoho records to Conversions."""
        params = self.config.connection_params
        module = params.get("module", "Deals")

        # Determine conversion type
        if module == "Deals":
            conversion_type = ConversionType.PURCHASE
        elif module == "Leads":
            conversion_type = ConversionType.LEAD
        else:
            conversion_type = ConversionType.CUSTOM

        # Build field map for Zoho fields
        field_map = {
            "id": "transaction_id",
            "Amount": "value",
            "Closing_Date": "timestamp",
            "Created_Time": "timestamp",
            "Email": "user_id",
            "Account_Name": "user_id",
        }
        field_map.update(self.config.field_overrides)

        normalizer = CRMNormalizer(
            customer_id=self.config.customer_id,
            conversion_type=conversion_type,
            field_map=field_map,
        )
        return normalizer.normalize(raw_records)

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
        super().close()


# Auto-register connector
get_registry().register(ConnectorType.ZOHO, ZohoConnector)
```

#### 4. Update Adapters __init__.py

Add new connectors to the adapters module.

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest packages/shared-connectors/tests/test_salesforce.py -v` passes
- [ ] `uv run pytest packages/shared-connectors/tests/test_hubspot.py -v` passes
- [ ] `uv run pytest packages/shared-connectors/tests/test_zoho.py -v` passes
- [ ] All connectors auto-register on import

#### Manual Verification:
- [ ] Salesforce connection works with test credentials
- [ ] HubSpot connection works with test credentials
- [ ] Records are fetched and normalized correctly

**Implementation Note**: After completing this phase and all automated verification passes, pause here for confirmation before proceeding to Phase 6.

---

## Phase 6: OLO Connector

### Overview

Implement connector for OLO (online ordering) platform integration.

### Changes Required

#### 1. OLO Connector

**File**: `packages/shared-connectors/growthnav/connectors/adapters/olo.py`

```python
"""OLO online ordering connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Generator

import httpx

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, POSNormalizer

logger = logging.getLogger(__name__)


class OLOConnector(BaseConnector):
    """Connector for OLO online ordering platform.

    Fetches orders from OLO and normalizes them to Conversions.

    Required credentials:
        - api_key: OLO API key

    Optional connection_params:
        - base_url: API base URL (default: https://api.olo.com)
        - brand_id: Filter orders by brand

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.OLO,
            customer_id="restaurant_chain",
            name="OLO Orders",
            credentials={
                "api_key": "xxx",
            },
            connection_params={
                "brand_id": "12345",
            }
        )
    """

    connector_type = ConnectorType.OLO

    def __init__(self, config: ConnectorConfig):
        """Initialize OLO connector."""
        super().__init__(config)

    def authenticate(self) -> None:
        """Set up OLO API client."""
        creds = self.config.credentials
        params = self.config.connection_params
        base_url = params.get("base_url", "https://api.olo.com")

        self._client = httpx.Client(
            base_url=base_url,
            headers={
                "Authorization": f"Bearer {creds['api_key']}",
                "Content-Type": "application/json",
            },
        )
        self._authenticated = True
        logger.info("Connected to OLO")

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch orders from OLO."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        brand_id = params.get("brand_id")

        # Build query parameters
        query = {}
        if brand_id:
            query["brand_id"] = brand_id
        if since:
            query["created_after"] = since.isoformat()
        if until:
            query["created_before"] = until.isoformat()

        # Fetch with pagination
        offset = 0
        page_size = 100
        count = 0

        while True:
            query["offset"] = offset
            query["limit"] = page_size

            response = self._client.get("/v1/orders", params=query)
            response.raise_for_status()
            data = response.json()

            orders = data.get("orders", [])
            if not orders:
                break

            for order in orders:
                yield order
                count += 1

                if limit and count >= limit:
                    return

            if len(orders) < page_size:
                break
            offset += page_size

    def get_schema(self) -> dict[str, str]:
        """Get OLO order schema."""
        # OLO has a fixed schema
        return {
            "id": "string",
            "order_number": "string",
            "customer_id": "string",
            "customer_email": "string",
            "customer_phone": "string",
            "subtotal": "number",
            "tax": "number",
            "total": "number",
            "created_at": "datetime",
            "completed_at": "datetime",
            "location_id": "string",
            "location_name": "string",
            "items": "array",
        }

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize OLO orders to Conversions."""
        # Build field map for OLO fields
        field_map = {
            "id": "transaction_id",
            "order_number": "transaction_id",
            "total": "value",
            "subtotal": "value",
            "created_at": "timestamp",
            "completed_at": "timestamp",
            "customer_id": "user_id",
            "customer_email": "user_id",
            "location_id": "location_id",
            "location_name": "location_name",
        }
        field_map.update(self.config.field_overrides)

        normalizer = POSNormalizer(
            customer_id=self.config.customer_id,
            field_map=field_map,
        )
        return normalizer.normalize(raw_records)

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
        super().close()


# Auto-register connector
get_registry().register(ConnectorType.OLO, OLOConnector)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest packages/shared-connectors/tests/test_olo.py -v` passes
- [ ] OLO connector auto-registers on import

#### Manual Verification:
- [ ] OLO connection works with test credentials
- [ ] Orders are fetched and normalized correctly

**Implementation Note**: After completing this phase and all automated verification passes, pause here for confirmation before proceeding to Phase 7.

---

## Phase 7: MCP Tools and Onboarding Integration

### Overview

Add MCP tools for connector management and integrate with the onboarding workflow.

### Changes Required

#### 1. MCP Connector Tools

**File**: Update `packages/mcp-server/growthnav_mcp/server.py`

Add new tools section:

```python
# =============================================================================
# Connector Tools
# =============================================================================


@mcp.tool()
def list_connectors() -> list[dict]:
    """
    List all available connector types.

    Returns:
        List of connector types with their registration status.
    """
    from growthnav.connectors import ConnectorType, get_registry

    registry = get_registry()

    return [
        {
            "type": ct.value,
            "registered": registry.is_registered(ct),
            "category": _get_connector_category(ct),
        }
        for ct in ConnectorType
    ]


def _get_connector_category(ct: "ConnectorType") -> str:
    """Get the category for a connector type."""
    from growthnav.connectors import ConnectorType

    categories = {
        ConnectorType.SNOWFLAKE: "data_lake",
        ConnectorType.BIGQUERY: "data_lake",
        ConnectorType.TOAST: "pos",
        ConnectorType.SQUARE: "pos",
        ConnectorType.CLOVER: "pos",
        ConnectorType.LIGHTSPEED: "pos",
        ConnectorType.SALESFORCE: "crm",
        ConnectorType.HUBSPOT: "crm",
        ConnectorType.ZOHO: "crm",
        ConnectorType.OLO: "olo",
        ConnectorType.OTTER: "olo",
        ConnectorType.CHOWLY: "olo",
        ConnectorType.FISHBOWL: "loyalty",
        ConnectorType.PUNCHH: "loyalty",
    }
    return categories.get(ct, "other")


@mcp.tool()
def configure_data_source(
    customer_id: str,
    connector_type: str,
    name: str,
    connection_params: dict,
    credentials: dict | None = None,
    credentials_secret_path: str | None = None,
    field_overrides: dict | None = None,
) -> dict:
    """
    Configure a new data source connector for a customer.

    Args:
        customer_id: Customer identifier
        connector_type: Connector type (snowflake, salesforce, hubspot, etc.)
        name: Human-readable name for this connection
        connection_params: Connection-specific parameters
        credentials: Direct credentials (use Secret Manager in production)
        credentials_secret_path: Path to credentials in Secret Manager
        field_overrides: Custom field mappings

    Returns:
        Configuration result with test connection status.
    """
    from growthnav.connectors import ConnectorConfig, ConnectorType, get_registry

    try:
        ct = ConnectorType(connector_type)
    except ValueError:
        return {"success": False, "error": f"Unknown connector type: {connector_type}"}

    registry = get_registry()
    if not registry.is_registered(ct):
        return {"success": False, "error": f"Connector not available: {connector_type}"}

    config = ConnectorConfig(
        connector_type=ct,
        customer_id=customer_id,
        name=name,
        connection_params=connection_params,
        credentials=credentials or {},
        credentials_secret_path=credentials_secret_path,
        field_overrides=field_overrides or {},
    )

    # Test connection
    connector = registry.create(config)
    if connector.test_connection():
        return {
            "success": True,
            "config": {
                "customer_id": config.customer_id,
                "connector_type": config.connector_type.value,
                "name": config.name,
            },
            "message": "Connection test successful",
        }
    else:
        return {
            "success": False,
            "error": "Connection test failed. Check credentials and connection parameters.",
        }


@mcp.tool()
async def discover_schema(
    customer_id: str,
    connector_type: str,
    connection_params: dict,
    credentials: dict,
    sample_size: int = 100,
) -> dict:
    """
    Discover and analyze schema from a data source.

    Uses LLM-assisted semantic detection to suggest field mappings.

    Args:
        customer_id: Customer identifier
        connector_type: Connector type
        connection_params: Connection parameters
        credentials: Credentials for the connection
        sample_size: Number of records to sample

    Returns:
        Schema analysis with mapping suggestions.
    """
    from growthnav.connectors import ConnectorConfig, ConnectorType, get_registry
    from growthnav.connectors.discovery import SchemaDiscovery

    try:
        ct = ConnectorType(connector_type)
    except ValueError:
        return {"success": False, "error": f"Unknown connector type: {connector_type}"}

    config = ConnectorConfig(
        connector_type=ct,
        customer_id=customer_id,
        name="schema_discovery",
        connection_params=connection_params,
        credentials=credentials,
    )

    registry = get_registry()
    connector = registry.create(config)

    try:
        connector.authenticate()

        # Fetch sample data
        sample_data = []
        for record in connector.fetch_records(limit=sample_size):
            sample_data.append(record)

        # Run schema discovery
        discovery = SchemaDiscovery()
        result = await discovery.analyze(
            data=sample_data,
            context=f"{connector_type} data for {customer_id}",
        )

        return {
            "success": True,
            "source_schema": connector.get_schema(),
            "suggested_mappings": [
                {
                    "source": s.source_field,
                    "target": s.target_field,
                    "confidence": s.confidence,
                    "reason": s.reason,
                }
                for s in result["suggestions"]
            ],
            "field_map": result["field_map"],
            "confidence_summary": result["confidence_summary"],
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        connector.close()


@mcp.tool()
def sync_data_source(
    customer_id: str,
    connector_type: str,
    connection_params: dict,
    credentials: dict,
    since: str | None = None,
    field_overrides: dict | None = None,
) -> dict:
    """
    Sync data from a configured connector.

    Args:
        customer_id: Customer identifier
        connector_type: Connector type
        connection_params: Connection parameters
        credentials: Credentials for the connection
        since: ISO datetime for incremental sync (optional)
        field_overrides: Custom field mappings

    Returns:
        Sync result with statistics.
    """
    from datetime import datetime
    from growthnav.connectors import ConnectorConfig, ConnectorType, SyncMode, get_registry

    try:
        ct = ConnectorType(connector_type)
    except ValueError:
        return {"success": False, "error": f"Unknown connector type: {connector_type}"}

    config = ConnectorConfig(
        connector_type=ct,
        customer_id=customer_id,
        name="sync",
        connection_params=connection_params,
        credentials=credentials,
        field_overrides=field_overrides or {},
        sync_mode=SyncMode.INCREMENTAL if since else SyncMode.FULL,
    )

    registry = get_registry()
    connector = registry.create(config)

    try:
        since_dt = datetime.fromisoformat(since) if since else None
        result = connector.sync(since=since_dt)

        return {
            "success": result.success,
            "records_fetched": result.records_fetched,
            "records_normalized": result.records_normalized,
            "records_failed": result.records_failed,
            "duration_seconds": result.duration_seconds,
            "error": result.error,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        connector.close()
```

#### 2. Update OnboardingRequest

**File**: Update `packages/shared-onboarding/growthnav/onboarding/orchestrator.py`

Add data source configuration to the onboarding request:

```python
@dataclass
class DataSourceConfig:
    """Configuration for a customer data source."""

    connector_type: str  # "snowflake", "salesforce", etc.
    name: str
    connection_params: dict[str, Any] = field(default_factory=dict)
    credentials_secret_path: str | None = None
    field_overrides: dict[str, str] = field(default_factory=dict)
    sync_schedule: str = "daily"


@dataclass
class OnboardingRequest:
    """Request to onboard a new customer."""

    customer_id: str
    customer_name: str
    industry: Industry
    gcp_project_id: str | None = None
    google_ads_customer_ids: list[str] = field(default_factory=list)
    meta_ad_account_ids: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    credentials: dict[str, str] = field(default_factory=dict)

    # NEW: Data source configurations
    data_sources: list[DataSourceConfig] = field(default_factory=list)
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest packages/mcp-server/tests/test_connector_tools.py -v` passes
- [ ] MCP tools are registered and callable
- [ ] discover_schema returns sensible suggestions

#### Manual Verification:
- [ ] list_connectors shows all available types
- [ ] configure_data_source validates connections
- [ ] sync_data_source fetches and normalizes data

**Implementation Note**: After completing this phase and all automated verification passes, the full implementation is complete.

---

## Testing Strategy

### Unit Tests

Each phase includes unit tests for:
- Configuration validation
- Field mapping
- Error handling
- Edge cases

### Integration Tests

Integration tests (skipped by default) for:
- Snowflake connection with real credentials
- CRM connections with sandbox accounts
- Schema discovery with sample data
- Identity resolution with test datasets

Run with: `RUN_CONNECTOR_INTEGRATION_TESTS=1 uv run pytest`

### Manual Testing Checklist

1. [ ] Configure a Snowflake connector for Toast data
2. [ ] Run schema discovery and review suggestions
3. [ ] Sync data and verify normalization
4. [ ] Link identities across POS and CRM
5. [ ] Verify resolved identities have correct fragments

---

## Performance Considerations

- **Pagination**: All connectors use cursor-based pagination
- **Streaming**: fetch_records yields records to avoid memory bloat
- **Connection pooling**: Connectors reuse authenticated clients
- **Incremental sync**: Use `since` parameter to avoid full refreshes

---

## Migration Notes

- Existing customers with manual data loads are unaffected
- New connector configuration is additive to OnboardingRequest
- Field override maps allow gradual migration from hardcoded mappings

---

## References

- Research document: [2025-12-05-client-onboarding-data-integration-architecture.md](../research/2025-12-05-client-onboarding-data-integration-architecture.md)
- CLV integration research: [Automating CLV Data Integration Across Data Lakes.md](../../Automating%20CLV%20Data%20Integration%20Across%20Data%20Lakes.md)
- Original architecture: [2025-11-27-shared-analytics-infrastructure-architecture.md](../research/2025-11-27-shared-analytics-infrastructure-architecture.md)
- Splink documentation: https://moj-analytical-services.github.io/splink/
- Simple Salesforce: https://simple-salesforce.readthedocs.io/
- HubSpot API: https://developers.hubspot.com/docs/api/overview
