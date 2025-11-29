"""
Conversion normalizers - transform source data into unified Conversion schema.

Each normalizer handles a specific data source:
- POSNormalizer: Point of Sale systems (Square, Toast, Lightspeed, etc.)
- CRMNormalizer: CRM systems (Salesforce, HubSpot, etc.)
- LoyaltyNormalizer: Loyalty programs
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd
from growthnav.conversions.schema import (
    Conversion,
    ConversionSource,
    ConversionType,
)


class ConversionNormalizer(ABC):
    """Base class for conversion normalizers."""

    source: ConversionSource

    def __init__(self, customer_id: str):
        """
        Initialize normalizer.

        Args:
            customer_id: GrowthNav customer identifier
        """
        self.customer_id = customer_id

    @abstractmethod
    def normalize(
        self,
        data: pd.DataFrame | list[dict[str, Any]],
    ) -> list[Conversion]:
        """
        Normalize source data to Conversion objects.

        Args:
            data: Source data as DataFrame or list of dicts

        Returns:
            List of normalized Conversion objects
        """
        pass

    def _to_dataframe(
        self,
        data: pd.DataFrame | list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Convert input to DataFrame."""
        if isinstance(data, pd.DataFrame):
            return data
        return pd.DataFrame(data)


class POSNormalizer(ConversionNormalizer):
    """
    Normalize Point of Sale transaction data.

    Supports common POS field mappings:
    - Square, Toast, Lightspeed, Clover
    - Custom mappings via field_map parameter

    Example:
        normalizer = POSNormalizer(
            customer_id="topgolf",
            field_map={
                "order_id": "transaction_id",
                "total_amount": "value",
                "created_at": "timestamp",
            }
        )
        conversions = normalizer.normalize(pos_data)
    """

    source = ConversionSource.POS

    def __init__(
        self,
        customer_id: str,
        field_map: dict[str, str] | None = None,
    ):
        """
        Initialize POS normalizer.

        Args:
            customer_id: GrowthNav customer identifier
            field_map: Mapping of source fields to Conversion fields
        """
        super().__init__(customer_id)
        self.field_map = field_map or self._default_field_map()

    def _default_field_map(self) -> dict[str, str]:
        """Default field mappings for common POS systems."""
        return {
            # Transaction ID variants
            "order_id": "transaction_id",
            "transaction_id": "transaction_id",
            "receipt_number": "transaction_id",
            "check_number": "transaction_id",
            # Value variants
            "total": "value",
            "total_amount": "value",
            "amount": "value",
            "subtotal": "value",
            # Timestamp variants
            "created_at": "timestamp",
            "order_date": "timestamp",
            "transaction_date": "timestamp",
            "date": "timestamp",
            # Customer variants
            "customer_id": "user_id",
            "guest_id": "user_id",
            "member_id": "user_id",
            # Location variants
            "store_id": "location_id",
            "location_id": "location_id",
            "store_name": "location_name",
        }

    def normalize(
        self,
        data: pd.DataFrame | list[dict[str, Any]],
    ) -> list[Conversion]:
        """Normalize POS data to Conversions."""
        df = self._to_dataframe(data)
        conversions = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # Map fields
            mapped = {}
            for source_field, target_field in self.field_map.items():
                if source_field in row_dict:
                    mapped[target_field] = row_dict[source_field]

            # Parse timestamp
            timestamp = mapped.get("timestamp")
            if timestamp:
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif not isinstance(timestamp, datetime):
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()

            conversion = Conversion(
                customer_id=self.customer_id,
                user_id=mapped.get("user_id"),
                transaction_id=str(mapped.get("transaction_id", "")),
                conversion_type=ConversionType.PURCHASE,
                source=self.source,
                timestamp=timestamp,
                value=float(mapped.get("value", 0)),
                location_id=mapped.get("location_id"),
                location_name=mapped.get("location_name"),
                raw_data=row_dict,
            )
            conversions.append(conversion)

        return conversions


class CRMNormalizer(ConversionNormalizer):
    """
    Normalize CRM lead/opportunity data.

    Supports Salesforce, HubSpot, and similar CRMs.
    """

    source = ConversionSource.CRM

    def __init__(
        self,
        customer_id: str,
        conversion_type: ConversionType = ConversionType.LEAD,
        field_map: dict[str, str] | None = None,
    ):
        super().__init__(customer_id)
        self.conversion_type = conversion_type
        self.field_map = field_map or self._default_field_map()

    def _default_field_map(self) -> dict[str, str]:
        """Default field mappings for CRM systems."""
        return {
            # IDs
            "opportunity_id": "transaction_id",
            "lead_id": "transaction_id",
            "deal_id": "transaction_id",
            "contact_id": "user_id",
            # Value
            "amount": "value",
            "deal_value": "value",
            "opportunity_amount": "value",
            # Dates
            "close_date": "timestamp",
            "created_date": "timestamp",
            "conversion_date": "timestamp",
            # UTM tracking
            "utm_source": "utm_source",
            "utm_medium": "utm_medium",
            "utm_campaign": "utm_campaign",
            # Click IDs
            "gclid": "gclid",
            "fbclid": "fbclid",
        }

    def normalize(
        self,
        data: pd.DataFrame | list[dict[str, Any]],
    ) -> list[Conversion]:
        """Normalize CRM data to Conversions."""
        df = self._to_dataframe(data)
        conversions = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # Map fields
            mapped = {}
            for source_field, target_field in self.field_map.items():
                if source_field in row_dict:
                    mapped[target_field] = row_dict[source_field]

            # Parse timestamp
            timestamp = mapped.get("timestamp")
            if timestamp:
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif not isinstance(timestamp, datetime):
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()

            conversion = Conversion(
                customer_id=self.customer_id,
                user_id=mapped.get("user_id"),
                transaction_id=str(mapped.get("transaction_id", "")),
                conversion_type=self.conversion_type,
                source=self.source,
                timestamp=timestamp,
                value=float(mapped.get("value", 0)),
                gclid=mapped.get("gclid"),
                fbclid=mapped.get("fbclid"),
                utm_source=mapped.get("utm_source"),
                utm_medium=mapped.get("utm_medium"),
                utm_campaign=mapped.get("utm_campaign"),
                raw_data=row_dict,
            )
            conversions.append(conversion)

        return conversions


class LoyaltyNormalizer(ConversionNormalizer):
    """
    Normalize Loyalty Program data.

    Handles member signups, point redemptions, and reward claims.
    """

    source = ConversionSource.LOYALTY

    def __init__(
        self,
        customer_id: str,
        field_map: dict[str, str] | None = None,
    ):
        super().__init__(customer_id)
        self.field_map = field_map or self._default_field_map()

    def _default_field_map(self) -> dict[str, str]:
        """Default field mappings for loyalty programs."""
        return {
            "member_id": "user_id",
            "loyalty_id": "user_id",
            "transaction_id": "transaction_id",
            "redemption_id": "transaction_id",
            "points_value": "value",
            "reward_value": "value",
            "created_at": "timestamp",
            "redemption_date": "timestamp",
        }

    def normalize(
        self,
        data: pd.DataFrame | list[dict[str, Any]],
    ) -> list[Conversion]:
        """Normalize loyalty data to Conversions."""
        df = self._to_dataframe(data)
        conversions = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # Map fields
            mapped = {}
            for source_field, target_field in self.field_map.items():
                if source_field in row_dict:
                    mapped[target_field] = row_dict[source_field]

            # Determine conversion type
            if "redemption" in str(row_dict).lower():
                conversion_type = ConversionType.PURCHASE
            elif "signup" in str(row_dict).lower():
                conversion_type = ConversionType.SIGNUP
            else:
                conversion_type = ConversionType.CUSTOM

            # Parse timestamp
            timestamp = mapped.get("timestamp")
            if timestamp:
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif not isinstance(timestamp, datetime):
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()

            conversion = Conversion(
                customer_id=self.customer_id,
                user_id=mapped.get("user_id"),
                transaction_id=str(mapped.get("transaction_id", "")),
                conversion_type=conversion_type,
                source=self.source,
                timestamp=timestamp,
                value=float(mapped.get("value", 0)),
                raw_data=row_dict,
            )
            conversions.append(conversion)

        return conversions
