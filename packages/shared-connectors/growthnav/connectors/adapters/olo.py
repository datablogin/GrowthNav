"""OLO online ordering connector."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

import httpx

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.exceptions import AuthenticationError
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
        """Set up OLO API client.

        Raises:
            AuthenticationError: If authentication fails.
        """
        creds = self.config.credentials
        params = self.config.connection_params
        base_url = params.get("base_url", "https://api.olo.com")

        try:
            self._client = httpx.Client(
                base_url=base_url,
                headers={
                    "Authorization": f"Bearer {creds['api_key']}",
                    "Content-Type": "application/json",
                },
            )
            self._authenticated = True
            logger.info("Connected to OLO")
        except Exception as e:
            raise AuthenticationError(f"Failed to authenticate with OLO: {e}") from e

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch orders from OLO.

        Args:
            since: Fetch orders created after this time (for incremental sync).
            until: Fetch orders created before this time.
            limit: Maximum orders to fetch.

        Yields:
            Raw order dictionaries from OLO.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        brand_id = params.get("brand_id")

        # Build query parameters
        query: dict[str, Any] = {}
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
        """Get OLO order schema.

        Returns:
            Dictionary mapping field names to data types.

        Raises:
            SchemaError: If schema retrieval fails.
        """
        # OLO has a fixed/known schema
        return {
            "id": "string",
            "order_number": "string",
            "customer_id": "string",
            "customer_email": "string",
            "customer_phone": "string",
            "subtotal": "number",
            "tax": "number",
            "total": "number",
            "tip": "number",
            "discount": "number",
            "created_at": "datetime",
            "completed_at": "datetime",
            "status": "string",
            "order_type": "string",
            "location_id": "string",
            "location_name": "string",
            "items": "array",
            "payments": "array",
        }

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize OLO orders to Conversions.

        Args:
            raw_records: List of raw order records from OLO.

        Returns:
            List of normalized Conversion objects.
        """
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
        conversions: list[Conversion] = normalizer.normalize(raw_records)
        return conversions

    def _cleanup_client(self) -> None:
        """Close HTTP client."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:  # pragma: no cover
                logger.warning(f"Error closing OLO HTTP client: {e}")


# Auto-register connector
get_registry().register(ConnectorType.OLO, OLOConnector)
