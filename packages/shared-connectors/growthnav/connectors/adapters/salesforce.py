"""Salesforce CRM connector."""

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
        """Connect to Salesforce."""
        try:
            from simple_salesforce import Salesforce
        except ImportError as e:
            raise ImportError(
                "simple-salesforce is required. "
                "Install with: pip install growthnav-connectors[salesforce]"
            ) from e

        creds = self.config.credentials
        params = self.config.connection_params

        self._client = Salesforce(
            username=creds["username"],
            password=creds["password"],
            security_token=creds.get("security_token", ""),
            domain=params.get("domain", "login"),
        )
        self._authenticated = True
        logger.info("Connected to Salesforce")

    def fetch_records(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch records from Salesforce."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        # Build SOQL query
        if "query" in params:
            query = params["query"]
        else:
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
        """Get schema for the configured object type."""
        if not self.is_authenticated:
            self.authenticate()

        params = self.config.connection_params
        object_type = params.get("object_type", "Opportunity")

        describe = getattr(self._client, object_type).describe()
        return {field["name"]: field["type"] for field in describe["fields"]}

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


# Auto-register connector
get_registry().register(ConnectorType.SALESFORCE, SalesforceConnector)
