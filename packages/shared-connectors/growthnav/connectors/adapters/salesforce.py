"""Salesforce CRM connector."""

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
from growthnav.conversions import Conversion, CRMNormalizer
from growthnav.conversions.schema import ConversionType

logger = logging.getLogger(__name__)

# Valid Salesforce object types (standard objects and custom objects ending in __c)
_SALESFORCE_OBJECT_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*(__c)?$")

# Supported standard object types
VALID_OBJECT_TYPES = {"Opportunity", "Lead", "Account", "Contact", "Campaign", "Case"}


def _validate_object_type(object_type: str) -> str:
    """Validate Salesforce object type identifier.

    Args:
        object_type: The object type to validate.

    Returns:
        The validated object type.

    Raises:
        ValueError: If the object type contains invalid characters.
    """
    if not _SALESFORCE_OBJECT_PATTERN.match(object_type):
        raise ValueError(
            f"Invalid Salesforce object type: '{object_type}'. "
            f"Must start with letter and contain only alphanumeric/underscore characters."
        )
    return object_type


class SalesforceConnector(BaseConnector):
    """Connector for Salesforce CRM.

    Fetches Opportunities, Leads, and Accounts from Salesforce
    and normalizes them to Conversions.

    Required credentials:
        - username: Salesforce username
        - password: Salesforce password
        - security_token: Salesforce security token

    Optional connection_params:
        - domain: "login" (production) or "test" (sandbox)
        - object_type: "Opportunity", "Lead", or "Account" (default: Opportunity)
        - query: Custom SOQL query (overrides object_type)

    Example:
        config = ConnectorConfig(
            connector_type=ConnectorType.SALESFORCE,
            customer_id="acme",
            name="Salesforce Opportunities",
            credentials={
                "username": "user@company.com",
                "password": "password",
                "security_token": "token123",
            },
            connection_params={
                "domain": "login",
                "object_type": "Opportunity",
            }
        )
    """

    connector_type = ConnectorType.SALESFORCE

    def __init__(self, config: ConnectorConfig):
        """Initialize Salesforce connector."""
        super().__init__(config)

    def authenticate(self) -> None:
        """Connect to Salesforce.

        Raises:
            ImportError: If simple-salesforce is not installed.
            AuthenticationError: If authentication fails.
        """
        try:
            from simple_salesforce import Salesforce
        except ImportError as e:
            raise ImportError(
                "simple-salesforce is required. "
                "Install with: pip install growthnav-connectors[salesforce]"
            ) from e

        creds = self.config.credentials
        params = self.config.connection_params

        try:
            self._client = Salesforce(
                username=creds["username"],
                password=creds["password"],
                security_token=creds.get("security_token", ""),
                domain=params.get("domain", "login"),
            )
            self._authenticated = True
            logger.info("Connected to Salesforce")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to authenticate with Salesforce: {e}"
            ) from e

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Salesforce.

        Args:
            since: Fetch records updated after this time (for incremental sync).
            until: Fetch records updated before this time.
            limit: Maximum records to fetch.

        Yields:
            Raw record dictionaries from Salesforce.

        Raises:
            ValueError: If object_type is invalid.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = _validate_object_type(params.get("object_type", "Opportunity"))

        # Build SOQL query with validated object type
        fields = self._get_fields_for_object(object_type)
        query = f"SELECT {', '.join(fields)} FROM {object_type}"

        conditions = []
        if since:
            conditions.append(f"LastModifiedDate >= {since.isoformat()}")
        if until:
            conditions.append(f"LastModifiedDate <= {until.isoformat()}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY LastModifiedDate"

        if limit:
            query += f" LIMIT {limit}"

        logger.debug(f"Executing SOQL: {query}")

        # Execute query with pagination
        result = self._client.query(query)
        while True:
            for record in result.get("records", []):
                # Remove Salesforce metadata
                record.pop("attributes", None)
                yield record

            if result.get("done"):
                break
            result = self._client.query_more(result["nextRecordsUrl"])

    def _get_fields_for_object(self, object_type: str) -> list[str]:
        """Get relevant fields for an object type."""
        common_fields = ["Id", "Name", "CreatedDate", "LastModifiedDate"]

        if object_type == "Opportunity":
            return common_fields + [
                "Amount",
                "CloseDate",
                "StageName",
                "IsClosed",
                "IsWon",
                "AccountId",
                "ContactId",
                "LeadSource",
            ]
        elif object_type == "Lead":
            return common_fields + [
                "Email",
                "Phone",
                "Company",
                "Status",
                "LeadSource",
                "ConvertedDate",
                "ConvertedOpportunityId",
            ]
        elif object_type == "Account":
            return common_fields + [
                "Industry",
                "AnnualRevenue",
                "NumberOfEmployees",
                "BillingCity",
                "BillingState",
                "BillingCountry",
            ]
        else:
            return common_fields

    def get_schema(self) -> dict[str, str]:
        """Get schema for the configured object type.

        Returns:
            Dictionary mapping field names to data types.

        Raises:
            ValueError: If object_type is invalid.
            SchemaError: If schema retrieval fails.
        """
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = _validate_object_type(params.get("object_type", "Opportunity"))

        try:
            describe = getattr(self._client, object_type).describe()
            return {field["name"]: field["type"] for field in describe["fields"]}
        except Exception as e:
            raise SchemaError(
                f"Failed to get schema for Salesforce object {object_type}: {e}"
            ) from e

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[Conversion]:
        """Normalize Salesforce records to Conversions."""
        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        # Determine conversion type based on object
        if object_type == "Opportunity":
            conversion_type = ConversionType.PURCHASE
        elif object_type == "Lead":
            conversion_type = ConversionType.LEAD
        else:
            conversion_type = ConversionType.CUSTOM

        # Build field map for Salesforce fields
        field_map = {
            "Id": "transaction_id",
            "Amount": "value",
            "CloseDate": "timestamp",
            "CreatedDate": "timestamp",
            "AccountId": "user_id",
            "ContactId": "user_id",
            "Email": "user_id",
            "LeadSource": "utm_source",
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
        """Clean up Salesforce client.

        Note: simple-salesforce doesn't have an explicit close method,
        but we clear the client reference for consistency.
        """
        if self._client:
            try:
                # simple-salesforce uses requests sessions internally
                # Clear client reference to allow garbage collection
                self._client = None
            except Exception as e:  # pragma: no cover
                logger.warning(f"Error cleaning up Salesforce client: {e}")


# Auto-register connector
get_registry().register(ConnectorType.SALESFORCE, SalesforceConnector)
