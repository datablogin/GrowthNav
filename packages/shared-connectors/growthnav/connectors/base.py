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

# Default batch size for processing records to avoid memory accumulation
DEFAULT_BATCH_SIZE = 1000


class BaseConnector(ABC):
    """Abstract base class for data source connectors.

    Subclasses must implement:
    - authenticate(): Connect to the external system
    - fetch_records(): Yield raw records from the source
    - get_schema(): Return source system schema
    - normalize(): Convert raw records to Conversion objects

    Subclasses must set the class attribute:
    - connector_type: The ConnectorType enum value for this connector

    Optional overrides:
    - _cleanup_client(): Custom cleanup logic for the client connection

    Can be used as a context manager:
        with SnowflakeConnector(config) as connector:
            result = connector.sync()

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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate that subclasses define connector_type."""
        super().__init_subclass__(**kwargs)
        # Skip validation for abstract subclasses
        if ABC in cls.__bases__:
            return
        if not hasattr(cls, "connector_type") or cls.connector_type is None:
            raise TypeError(
                f"{cls.__name__} must define a 'connector_type' class attribute"
            )

    def __init__(self, config: ConnectorConfig):
        """Initialize connector with configuration.

        Args:
            config: Connector configuration including credentials and settings.
        """
        self.config = config
        self._client: Any = None
        self._authenticated = False

    def __enter__(self) -> BaseConnector:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and cleanup resources."""
        self.close()

    @property
    def is_authenticated(self) -> bool:
        """Return True if connector is authenticated."""
        return self._authenticated

    @abstractmethod
    def authenticate(self) -> None:
        """Authenticate with the external system.

        Raises:
            AuthenticationError: If authentication fails.
        """
        pass  # pragma: no cover

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
        pass  # pragma: no cover

    @abstractmethod
    def get_schema(self) -> dict[str, str]:
        """Return the source system's schema.

        Returns:
            Dictionary mapping column names to data types.
        """
        pass  # pragma: no cover

    @abstractmethod
    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize raw records to Conversion objects.

        Args:
            raw_records: List of raw records from fetch_records().

        Returns:
            List of normalized Conversion objects.
        """
        pass  # pragma: no cover

    def _cleanup_client(self) -> None:  # noqa: B027
        """Clean up the client connection.

        Override this method in subclasses to implement custom cleanup logic
        (e.g., closing database cursors, releasing connection pools).

        This is called by close() before resetting internal state.

        This is intentionally not abstract - it's an optional hook with a no-op default.
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
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> SyncResult:
        """Sync data from the source system.

        Processes records in batches to avoid memory accumulation.

        Args:
            mode: Sync mode (full or incremental). Defaults to config setting.
            since: Override start time for incremental sync.
            until: Override end time for sync.
            batch_size: Number of records to process per batch. Defaults to 1000.

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
                logger.info(
                    f"Starting incremental sync for {self.config.name} "
                    f"since {since.isoformat() if since else 'beginning'}"
                )

            # Validate time range
            if since and until and since >= until:
                raise ValueError(f"since ({since}) must be before until ({until})")

            # Process records in batches to avoid memory accumulation
            batch: list[dict[str, Any]] = []
            for record in self.fetch_records(since=since, until=until):
                batch.append(record)
                result.records_fetched += 1

                if len(batch) >= batch_size:
                    conversions = self.normalize(batch)
                    result.records_normalized += len(conversions)
                    batch = []

            # Process remaining records
            if batch:
                conversions = self.normalize(batch)
                result.records_normalized += len(conversions)

            result.records_failed = result.records_fetched - result.records_normalized

            # Update cursor for next incremental sync
            result.cursor = datetime.now(UTC).isoformat()

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
        logger.debug(f"Closing connector: {self.config.name}")
        self._cleanup_client()
        self._client = None
        self._authenticated = False
