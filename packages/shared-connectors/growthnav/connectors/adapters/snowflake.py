"""Snowflake data lake connector."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator
from datetime import datetime
from typing import Any

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.exceptions import AuthenticationError, SchemaError
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, POSNormalizer

logger = logging.getLogger(__name__)

# Pattern for valid SQL identifiers (table/column names)
_SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, identifier_type: str = "identifier") -> str:
    """Validate a SQL identifier (table/column name).

    Args:
        name: The identifier to validate.
        identifier_type: Type of identifier for error message (e.g., "table", "column").

    Returns:
        The validated identifier.

    Raises:
        ValueError: If the identifier contains invalid characters.
    """
    if not _SQL_IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"Invalid SQL {identifier_type}: '{name}'. "
            f"Must start with letter/underscore and contain only alphanumeric/underscore characters."
        )
    return name


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake data warehouse.

    Used to access POS data (Toast, Square) and loyalty data (Fishbowl)
    that has been loaded into Snowflake.

    Required connection_params:
        - account: Snowflake account identifier (xxx.snowflakecomputing.com)
        - warehouse: Compute warehouse name
        - database: Database name
        - schema: Schema name (default: PUBLIC)

    Required credentials:
        - user: Snowflake username
        - password: Snowflake password (or use key pair auth)

    Optional connection_params:
        - table: Specific table to query (default: TRANSACTIONS)
        - timestamp_column: Column for incremental sync (default: UPDATED_AT)
        - role: Snowflake role to assume

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.SNOWFLAKE,
            customer_id="topgolf",
            name="Toast via Snowflake",
            connection_params={
                "account": "acme.us-east-1.snowflakecomputing.com",
                "warehouse": "ANALYTICS_WH",
                "database": "TOAST_DATA",
                "schema": "RAW",
                "table": "TRANSACTIONS",
            },
            credentials={
                "user": "growthnav_service",
                "password": "...",
            }
        )
    """

    connector_type = ConnectorType.SNOWFLAKE

    def __init__(self, config: ConnectorConfig):
        """Initialize Snowflake connector."""
        super().__init__(config)
        self._cursor = None

    def authenticate(self) -> None:
        """Connect to Snowflake.

        Raises:
            AuthenticationError: If authentication fails.
            ImportError: If snowflake-connector-python is not installed.
        """
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required. "
                "Install with: pip install growthnav-connectors[snowflake]"
            ) from e

        params = self.config.connection_params
        creds = self.config.credentials

        try:
            self._client = snowflake.connector.connect(
                account=params["account"],
                user=creds.get("user"),
                password=creds.get("password"),
                warehouse=params.get("warehouse"),
                database=params.get("database"),
                schema=params.get("schema", "PUBLIC"),
                role=params.get("role"),
            )
            self._authenticated = True
            logger.info(f"Connected to Snowflake: {params['account']}")
        except Exception as e:
            raise AuthenticationError(f"Failed to authenticate with Snowflake: {e}") from e

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Snowflake table.

        Args:
            since: Fetch records updated after this time (for incremental sync).
            until: Fetch records updated before this time.
            limit: Maximum records to fetch.

        Yields:
            Raw record dictionaries from the source system.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        # Validate identifiers to prevent SQL injection
        table = _validate_identifier(params.get("table", "TRANSACTIONS"), "table")
        ts_col = _validate_identifier(params.get("timestamp_column", "UPDATED_AT"), "column")

        # Build query with validated identifiers and parameterized values
        query = f"SELECT * FROM {table}"
        conditions = []
        query_params = []

        if since:
            conditions.append(f"{ts_col} >= %s")
            query_params.append(since)
        if until:
            conditions.append(f"{ts_col} <= %s")
            query_params.append(until)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {ts_col}"

        if limit:
            query += f" LIMIT {limit}"

        logger.debug(f"Executing Snowflake query: {query}")

        cursor = self._client.cursor()
        try:
            if query_params:
                cursor.execute(query, query_params)
            else:
                cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            for row in cursor:
                yield dict(zip(columns, row, strict=True))
        finally:
            cursor.close()

    def get_schema(self) -> dict[str, str]:
        """Get schema of the source table.

        Returns:
            Dictionary mapping column names to data types.

        Raises:
            SchemaError: If schema retrieval fails.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        # Validate identifier to prevent SQL injection
        table = _validate_identifier(params.get("table", "TRANSACTIONS"), "table")

        cursor = self._client.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {table}")
            schema = {}
            for row in cursor:
                col_name = row[0]
                col_type = row[1]
                schema[col_name] = col_type
            return schema
        except Exception as e:
            raise SchemaError(f"Failed to get schema for table {table}: {e}") from e
        finally:
            cursor.close()

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize Snowflake records to Conversions.

        Args:
            raw_records: List of raw records from fetch_records().

        Returns:
            List of normalized Conversion objects.
        """
        normalizer = POSNormalizer(
            customer_id=self.config.customer_id,
            field_map=self.config.field_overrides or None,
        )
        conversions: list[Conversion] = normalizer.normalize(raw_records)
        return conversions

    def _cleanup_client(self) -> None:
        """Close Snowflake connection."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")


# Auto-register connector
get_registry().register(ConnectorType.SNOWFLAKE, SnowflakeConnector)
