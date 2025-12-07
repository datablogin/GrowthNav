"""HubSpot CRM connector."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

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
        except ImportError as e:
            raise ImportError(
                "hubspot-api-client is required. "
                "Install with: pip install growthnav-connectors[hubspot]"
            ) from e

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
        response = self._client.crm.properties.core_api.get_all(object_type=object_type)

        return {prop.name: prop.type for prop in response.results}

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


# Auto-register connector
get_registry().register(ConnectorType.HUBSPOT, HubSpotConnector)
