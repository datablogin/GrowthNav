"""Zoho CRM connector."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

import httpx

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.exceptions import AuthenticationError, SchemaError
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)

# Valid Zoho CRM modules
VALID_MODULES = {"Deals", "Leads", "Accounts", "Contacts", "Campaigns", "Cases"}

# Valid Zoho API domains (regional data centers)
VALID_DOMAINS = {
    "zohoapis.com",  # US
    "zohoapis.eu",  # EU
    "zohoapis.com.au",  # Australia
    "zohoapis.in",  # India
    "zohoapis.jp",  # Japan
    "zohoapis.com.cn",  # China
}


def _validate_module(module: str) -> str:
    """Validate Zoho module name.

    Args:
        module: The module name to validate.

    Returns:
        The validated module name.

    Raises:
        ValueError: If the module is not supported.
    """
    if module not in VALID_MODULES:
        raise ValueError(
            f"Unsupported Zoho module: '{module}'. "
            f"Valid modules are: {', '.join(sorted(VALID_MODULES))}"
        )
    return module


def _validate_domain(domain: str) -> str:
    """Validate Zoho API domain.

    Args:
        domain: The domain to validate.

    Returns:
        The validated domain.

    Raises:
        ValueError: If the domain is not a valid Zoho data center.
    """
    if domain not in VALID_DOMAINS:
        raise ValueError(
            f"Invalid Zoho domain: '{domain}'. "
            f"Valid domains are: {', '.join(sorted(VALID_DOMAINS))}"
        )
    return domain


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
        """Get access token from Zoho.

        Raises:
            ValueError: If domain is invalid.
            AuthenticationError: If authentication fails.
        """
        creds = self.config.credentials
        params = self.config.connection_params
        domain = _validate_domain(params.get("domain", "zohoapis.com"))

        # Refresh access token
        token_url = f"https://accounts.{domain}/oauth/v2/token"

        try:
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
        except Exception as e:
            raise AuthenticationError(
                f"Failed to authenticate with Zoho CRM: {e}"
            ) from e

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Zoho.

        Args:
            since: Fetch records updated after this time (for incremental sync).
            until: Fetch records updated before this time.
            limit: Maximum records to fetch.

        Yields:
            Raw record dictionaries from Zoho.

        Raises:
            ValueError: If module is invalid.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        module = _validate_module(params.get("module", "Deals"))

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
                    try:
                        modified_dt = datetime.fromisoformat(
                            modified.replace("Z", "+00:00")
                        )
                        if since and modified_dt < since:
                            continue
                        if until and modified_dt > until:
                            continue
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Invalid date format in Zoho record {record.get('id')}: "
                            f"{modified} - {e}"
                        )
                        # Include record anyway if date parsing fails

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
        """Get schema for the configured module.

        Returns:
            Dictionary mapping field names to data types.

        Raises:
            ValueError: If module is invalid.
            SchemaError: If schema retrieval fails.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        module = _validate_module(params.get("module", "Deals"))

        try:
            response = self._client.get("/settings/fields", params={"module": module})
            response.raise_for_status()

            return {
                field["api_name"]: field["data_type"]
                for field in response.json().get("fields", [])
            }
        except Exception as e:
            raise SchemaError(
                f"Failed to get schema for Zoho module {module}: {e}"
            ) from e

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
