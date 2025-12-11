"""Zoho CRM connector."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from datetime import datetime
from typing import Any, TypeVar, cast

import httpx

from growthnav.connectors.base import BaseConnector
from growthnav.connectors.config import ConnectorConfig, ConnectorType
from growthnav.connectors.exceptions import AuthenticationError, SchemaError
from growthnav.connectors.registry import get_registry
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)

T = TypeVar("T")

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

    # Maximum number of token refresh retries
    MAX_TOKEN_REFRESH_RETRIES = 1

    def __init__(self, config: ConnectorConfig):
        """Initialize Zoho connector."""
        super().__init__(config)
        self._access_token: str | None = None
        self._domain: str | None = None

    def authenticate(self) -> None:
        """Get access token from Zoho.

        Raises:
            ValueError: If domain is invalid.
            AuthenticationError: If authentication fails.
        """
        params = self.config.connection_params
        self._domain = _validate_domain(params.get("domain", "zohoapis.com"))

        try:
            self._refresh_access_token()

            self._client = httpx.Client(
                base_url=f"https://www.{self._domain}/crm/v3",
                headers={"Authorization": f"Zoho-oauthtoken {self._access_token}"},
            )
            self._authenticated = True
            logger.info("Connected to Zoho CRM")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to authenticate with Zoho CRM: {e}"
            ) from e

    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token.

        This method is called during initial authentication and when a 401
        response indicates the token has expired.

        Raises:
            AuthenticationError: If token refresh fails.
        """
        if not self._domain:
            params = self.config.connection_params
            self._domain = _validate_domain(params.get("domain", "zohoapis.com"))

        creds = self.config.credentials
        token_url = f"https://accounts.{self._domain}/oauth/v2/token"

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
                logger.info("Zoho access token refreshed successfully")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to refresh Zoho access token: {e}"
            ) from e

    def _update_client_authorization(self) -> None:
        """Update the HTTP client with new authorization header.

        Called after token refresh to update the client's auth header.
        """
        if self._client and self._access_token:
            self._client.headers["Authorization"] = (
                f"Zoho-oauthtoken {self._access_token}"
            )

    def _execute_with_token_refresh(
        self, operation: Callable[[], T], operation_name: str = "API call"
    ) -> T:
        """Execute an API operation with automatic token refresh on 401.

        Args:
            operation: A callable that performs the API operation.
            operation_name: Description of the operation for logging.

        Returns:
            The result of the operation.

        Raises:
            httpx.HTTPStatusError: If the request fails after retry.
            AuthenticationError: If token refresh fails.
        """
        retries = 0
        while True:
            try:
                return operation()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and retries < self.MAX_TOKEN_REFRESH_RETRIES:
                    retries += 1
                    logger.warning(
                        f"Zoho {operation_name} received 401 Unauthorized. "
                        f"Refreshing token (attempt {retries}/{self.MAX_TOKEN_REFRESH_RETRIES})..."
                    )
                    self._refresh_access_token()
                    self._update_client_authorization()
                    continue
                raise

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
            # Use token refresh wrapper for API call
            # Capture current page value in closure to avoid B023 warning
            current_page = page

            def fetch_page(p: int = current_page) -> httpx.Response:
                resp = cast(
                    httpx.Response,
                    self._client.get(
                        f"/{module}",
                        params={
                            "page": p,
                            "per_page": 200,
                        },
                    ),
                )
                resp.raise_for_status()
                return resp

            response = self._execute_with_token_refresh(
                fetch_page, f"fetch {module} page {page}"
            )
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
            def fetch_schema() -> httpx.Response:
                resp = cast(
                    httpx.Response,
                    self._client.get("/settings/fields", params={"module": module}),
                )
                resp.raise_for_status()
                return resp

            response = self._execute_with_token_refresh(
                fetch_schema, f"get schema for {module}"
            )

            return {
                field["api_name"]: field["data_type"]
                for field in response.json().get("fields", [])
            }
        except AuthenticationError:
            # Re-raise authentication errors directly
            raise
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
