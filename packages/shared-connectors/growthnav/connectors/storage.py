"""Connector configuration storage in BigQuery.

Stores and retrieves connector configurations for customers.
Credentials are stored separately in Secret Manager (not here).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from growthnav.connectors.config import ConnectorConfig, ConnectorType, SyncMode, SyncSchedule

if TYPE_CHECKING:
    from google.cloud import bigquery

logger = logging.getLogger(__name__)

# SQL for creating the connectors table
CREATE_CONNECTORS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{project_id}.growthnav_registry.connectors` (
    connector_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    connector_type STRING NOT NULL,
    name STRING NOT NULL,
    connection_params JSON,
    field_overrides JSON,
    sync_mode STRING DEFAULT 'incremental',
    sync_schedule STRING DEFAULT 'daily',
    last_sync TIMESTAMP,
    last_sync_cursor STRING,
    credentials_secret_path STRING,
    is_active BOOL DEFAULT TRUE,
    error_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


class ConnectorStorage:
    """Storage for connector configurations in BigQuery.

    Manages CRUD operations for connector configurations stored in
    the `growthnav_registry.connectors` table.

    Example:
        >>> storage = ConnectorStorage(project_id="my-project")
        >>> config = ConnectorConfig(
        ...     connector_type=ConnectorType.SNOWFLAKE,
        ...     customer_id="acme",
        ...     name="Toast POS",
        ...     connection_params={"account": "acme.snowflakecomputing.com"},
        ... )
        >>> connector_id = storage.save(config)
        >>> loaded = storage.get(connector_id)
    """

    def __init__(
        self,
        project_id: str,
        client: bigquery.Client | None = None,
    ):
        """Initialize connector storage.

        Args:
            project_id: GCP project ID containing the registry dataset.
            client: Optional BigQuery client. Will be created if not provided.
        """
        self.project_id = project_id
        self._client = client

    @property
    def client(self) -> bigquery.Client:
        """Lazy-initialize BigQuery client."""
        if self._client is None:
            from google.cloud import bigquery

            self._client = bigquery.Client(project=self.project_id)
        return self._client

    @property
    def table_id(self) -> str:
        """Full table ID for connectors table."""
        return f"{self.project_id}.growthnav_registry.connectors"

    def ensure_table_exists(self) -> None:
        """Create the connectors table if it doesn't exist."""
        sql = CREATE_CONNECTORS_TABLE_SQL.format(project_id=self.project_id)
        self.client.query(sql).result()
        logger.info(f"Ensured connectors table exists: {self.table_id}")

    def save(self, config: ConnectorConfig, connector_id: str | None = None) -> str:
        """Save a connector configuration.

        Args:
            config: The connector configuration to save.
            connector_id: Optional ID. If None, generates a new UUID.

        Returns:
            The connector_id of the saved configuration.
        """
        if connector_id is None:
            connector_id = str(uuid.uuid4())

        now = datetime.now(UTC)

        # Build the row to insert
        row = {
            "connector_id": connector_id,
            "customer_id": config.customer_id,
            "connector_type": config.connector_type.value,
            "name": config.name,
            "connection_params": json.dumps(config.connection_params),
            "field_overrides": json.dumps(config.field_overrides),
            "sync_mode": config.sync_mode.value,
            "sync_schedule": config.sync_schedule.value,
            "last_sync": config.last_sync.isoformat() if config.last_sync else None,
            "last_sync_cursor": config.last_sync_cursor,
            "credentials_secret_path": config.credentials_secret_path,
            "is_active": config.is_active,
            "error_message": config.error_message,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        # Use INSERT with ON CONFLICT for upsert behavior
        sql = f"""
        MERGE `{self.table_id}` AS target
        USING (SELECT @connector_id AS connector_id) AS source
        ON target.connector_id = source.connector_id
        WHEN MATCHED THEN
            UPDATE SET
                customer_id = @customer_id,
                connector_type = @connector_type,
                name = @name,
                connection_params = PARSE_JSON(@connection_params),
                field_overrides = PARSE_JSON(@field_overrides),
                sync_mode = @sync_mode,
                sync_schedule = @sync_schedule,
                last_sync = @last_sync,
                last_sync_cursor = @last_sync_cursor,
                credentials_secret_path = @credentials_secret_path,
                is_active = @is_active,
                error_message = @error_message,
                updated_at = @updated_at
        WHEN NOT MATCHED THEN
            INSERT (
                connector_id, customer_id, connector_type, name,
                connection_params, field_overrides, sync_mode, sync_schedule,
                last_sync, last_sync_cursor, credentials_secret_path,
                is_active, error_message, created_at, updated_at
            )
            VALUES (
                @connector_id, @customer_id, @connector_type, @name,
                PARSE_JSON(@connection_params), PARSE_JSON(@field_overrides),
                @sync_mode, @sync_schedule, @last_sync, @last_sync_cursor,
                @credentials_secret_path, @is_active, @error_message,
                @created_at, @updated_at
            )
        """

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", row["connector_id"]),
                bigquery.ScalarQueryParameter("customer_id", "STRING", row["customer_id"]),
                bigquery.ScalarQueryParameter("connector_type", "STRING", row["connector_type"]),
                bigquery.ScalarQueryParameter("name", "STRING", row["name"]),
                bigquery.ScalarQueryParameter("connection_params", "STRING", row["connection_params"]),
                bigquery.ScalarQueryParameter("field_overrides", "STRING", row["field_overrides"]),
                bigquery.ScalarQueryParameter("sync_mode", "STRING", row["sync_mode"]),
                bigquery.ScalarQueryParameter("sync_schedule", "STRING", row["sync_schedule"]),
                bigquery.ScalarQueryParameter("last_sync", "TIMESTAMP", row["last_sync"]),
                bigquery.ScalarQueryParameter("last_sync_cursor", "STRING", row["last_sync_cursor"]),
                bigquery.ScalarQueryParameter("credentials_secret_path", "STRING", row["credentials_secret_path"]),
                bigquery.ScalarQueryParameter("is_active", "BOOL", row["is_active"]),
                bigquery.ScalarQueryParameter("error_message", "STRING", row["error_message"]),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", row["created_at"]),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", row["updated_at"]),
            ]
        )

        self.client.query(sql, job_config=job_config).result()
        logger.info(f"Saved connector config: {connector_id}")
        return connector_id

    def get(self, connector_id: str) -> ConnectorConfig | None:
        """Get a connector configuration by ID.

        Args:
            connector_id: The connector ID to retrieve.

        Returns:
            The connector configuration, or None if not found.
        """
        sql = f"""
        SELECT *
        FROM `{self.table_id}`
        WHERE connector_id = @connector_id
        """

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", connector_id),
            ]
        )

        result = self.client.query(sql, job_config=job_config).result()
        rows = list(result)

        if not rows:
            return None

        return self._row_to_config(rows[0])

    def list_for_customer(self, customer_id: str, active_only: bool = True) -> list[ConnectorConfig]:
        """List all connectors for a customer.

        Args:
            customer_id: The customer ID to list connectors for.
            active_only: If True, only return active connectors.

        Returns:
            List of connector configurations.
        """
        sql = f"""
        SELECT *
        FROM `{self.table_id}`
        WHERE customer_id = @customer_id
        """
        if active_only:
            sql += " AND is_active = TRUE"
        sql += " ORDER BY created_at"

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id),
            ]
        )

        result = self.client.query(sql, job_config=job_config).result()
        return [self._row_to_config(row) for row in result]

    def delete(self, connector_id: str) -> bool:
        """Delete a connector configuration.

        Args:
            connector_id: The connector ID to delete.

        Returns:
            True if deleted, False if not found.
        """
        sql = f"""
        DELETE FROM `{self.table_id}`
        WHERE connector_id = @connector_id
        """

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", connector_id),
            ]
        )

        result = self.client.query(sql, job_config=job_config).result()
        deleted = (result.num_dml_affected_rows or 0) > 0
        if deleted:
            logger.info(f"Deleted connector config: {connector_id}")
        return deleted

    def deactivate(self, connector_id: str, error_message: str | None = None) -> bool:
        """Deactivate a connector (soft delete).

        Args:
            connector_id: The connector ID to deactivate.
            error_message: Optional error message explaining the deactivation.

        Returns:
            True if deactivated, False if not found.
        """
        sql = f"""
        UPDATE `{self.table_id}`
        SET is_active = FALSE, error_message = @error_message, updated_at = @updated_at
        WHERE connector_id = @connector_id
        """

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", connector_id),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.now(UTC).isoformat()),
            ]
        )

        result = self.client.query(sql, job_config=job_config).result()
        updated = (result.num_dml_affected_rows or 0) > 0
        if updated:
            logger.info(f"Deactivated connector: {connector_id}")
        return updated

    def update_sync_status(
        self,
        connector_id: str,
        last_sync: datetime,
        cursor: str | None = None,
        error_message: str | None = None,
    ) -> bool:
        """Update sync status for a connector.

        Args:
            connector_id: The connector ID to update.
            last_sync: The timestamp of the last sync.
            cursor: Optional cursor for incremental syncs.
            error_message: Optional error message if sync failed.

        Returns:
            True if updated, False if not found.
        """
        sql = f"""
        UPDATE `{self.table_id}`
        SET
            last_sync = @last_sync,
            last_sync_cursor = @cursor,
            error_message = @error_message,
            updated_at = @updated_at
        WHERE connector_id = @connector_id
        """

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", connector_id),
                bigquery.ScalarQueryParameter("last_sync", "TIMESTAMP", last_sync.isoformat()),
                bigquery.ScalarQueryParameter("cursor", "STRING", cursor),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.now(UTC).isoformat()),
            ]
        )

        result = self.client.query(sql, job_config=job_config).result()
        return (result.num_dml_affected_rows or 0) > 0

    def _row_to_config(self, row: Any) -> ConnectorConfig:
        """Convert a BigQuery row to ConnectorConfig.

        Args:
            row: BigQuery row containing connector configuration.

        Returns:
            ConnectorConfig object.

        Raises:
            TypeError: If connection_params or field_overrides have unexpected types.
        """
        # Parse JSON fields with explicit type validation
        connection_params = row.get("connection_params")
        if isinstance(connection_params, str):
            connection_params = json.loads(connection_params)
        elif isinstance(connection_params, dict):
            pass  # Already parsed
        elif connection_params is None:
            connection_params = {}
        else:
            raise TypeError(
                f"Unexpected type for connection_params: {type(connection_params).__name__}. "
                f"Expected str, dict, or None."
            )

        field_overrides = row.get("field_overrides")
        if isinstance(field_overrides, str):
            field_overrides = json.loads(field_overrides)
        elif isinstance(field_overrides, dict):
            pass  # Already parsed
        elif field_overrides is None:
            field_overrides = {}
        else:
            raise TypeError(
                f"Unexpected type for field_overrides: {type(field_overrides).__name__}. "
                f"Expected str, dict, or None."
            )

        return ConnectorConfig(
            connector_type=ConnectorType(row["connector_type"]),
            customer_id=row["customer_id"],
            name=row["name"],
            connection_params=connection_params,
            field_overrides=field_overrides,
            credentials_secret_path=row.get("credentials_secret_path"),
            sync_mode=SyncMode(row.get("sync_mode", "incremental")),
            sync_schedule=SyncSchedule(row.get("sync_schedule", "daily")),
            last_sync=row.get("last_sync"),
            last_sync_cursor=row.get("last_sync_cursor"),
            is_active=row.get("is_active", True),
            error_message=row.get("error_message"),
        )
