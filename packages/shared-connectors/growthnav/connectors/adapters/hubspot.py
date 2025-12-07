"""HubSpot CRM connector."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.exceptions import AuthenticationError, SchemaError
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)

# Valid HubSpot object types
VALID_OBJECT_TYPES = {"deals", "contacts", "companies"}


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
        """Connect to HubSpot.

        Raises:
            ImportError: If hubspot-api-client is not installed.
            AuthenticationError: If authentication fails.
        """
        try:
            from hubspot import HubSpot
        except ImportError as e:
            raise ImportError(
                "hubspot-api-client is required. "
                "Install with: pip install growthnav-connectors[hubspot]"
            ) from e

        creds = self.config.credentials

        try:
            self._client = HubSpot(access_token=creds["access_token"])
            self._authenticated = True
            logger.info("Connected to HubSpot")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to authenticate with HubSpot: {e}"
            ) from e

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from HubSpot.

        Args:
            since: Fetch records updated after this time (for incremental sync).
            until: Fetch records updated before this time.
            limit: Maximum records to fetch.

        Yields:
            Raw record dictionaries from HubSpot.

        Raises:
            ValueError: If object_type is not supported.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "deals")

        # Validate object type
        if object_type not in VALID_OBJECT_TYPES:
            raise ValueError(
                f"Unsupported object type: '{object_type}'. "
                f"Valid types are: {', '.join(sorted(VALID_OBJECT_TYPES))}"
            )

        # Get the appropriate API
        if object_type == "deals":
            api = self._client.crm.deals.basic_api
            properties = ["dealname", "amount", "closedate", "dealstage", "pipeline"]
        elif object_type == "contacts":
            api = self._client.crm.contacts.basic_api
            properties = ["email", "firstname", "lastname", "phone", "company"]
        else:  # companies
            api = self._client.crm.companies.basic_api
            properties = ["name", "domain", "industry", "annualrevenue"]

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
                    try:
                        updated_dt = datetime.fromisoformat(
                            updated.replace("Z", "+00:00")
                        )
                        if since and updated_dt < since:
                            continue
                        if until and updated_dt > until:
                            continue
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Invalid date format in HubSpot record {result.id}: "
                            f"{updated} - {e}"
                        )
                        # Include record anyway if date parsing fails

                yield record
                count += 1

                if limit and count >= limit:
                    return

            if not response.paging or not response.paging.next:
                break
            after = response.paging.next.after

    def get_schema(self) -> dict[str, str]:
        """Get schema for the configured object type.

        Returns:
            Dictionary mapping property names to data types.

        Raises:
            SchemaError: If schema retrieval fails.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "deals")

        try:
            # Get properties for object type
            response = self._client.crm.properties.core_api.get_all(
                object_type=object_type
            )
            return {prop.name: prop.type for prop in response.results}
        except Exception as e:
            raise SchemaError(
                f"Failed to get schema for HubSpot object {object_type}: {e}"
            ) from e

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
        conversions: list[Conversion] = normalizer.normalize(raw_records)
        return conversions

    def _cleanup_client(self) -> None:
        """Clean up HubSpot client.

        Note: hubspot-api-client doesn't have an explicit close method,
        but we clear the client reference for consistency.
        """
        if self._client:
            try:
                # hubspot-api-client uses requests sessions internally
                # Clear client reference to allow garbage collection
                self._client = None
            except Exception as e:  # pragma: no cover
                logger.warning(f"Error cleaning up HubSpot client: {e}")


# Auto-register connector
get_registry().register(ConnectorType.HUBSPOT, HubSpotConnector)
