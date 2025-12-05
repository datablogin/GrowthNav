"""Base connector abstract class."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

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
