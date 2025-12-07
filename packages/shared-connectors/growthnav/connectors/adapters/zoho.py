"""Zoho CRM connector."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

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

        response = self._client.get("/settings/fields", params={"module": module})
        response.raise_for_status()

        return {
            field["api_name"]: field["data_type"] for field in response.json().get("fields", [])
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
        conversions: list[Conversion] = normalizer.normalize(raw_records)
        return conversions

    def _cleanup_client(self) -> None:
        """Close HTTP client."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing Zoho HTTP client: {e}")


# Auto-register connector
get_registry().register(ConnectorType.ZOHO, ZohoConnector)
