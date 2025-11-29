"""
CustomerRegistry - Multi-tenant customer management with industry tagging.

Provides:
- Customer lookup with caching
- Industry-based grouping for cross-learning
- Dataset provisioning
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any

from google.cloud import bigquery


class Industry(str, Enum):
    """Industry verticals for customer grouping."""

    GOLF = "golf"
    MEDICAL = "medical"
    RESTAURANT = "restaurant"
    RETAIL = "retail"
    ECOMMERCE = "ecommerce"
    HOSPITALITY = "hospitality"
    FITNESS = "fitness"
    ENTERTAINMENT = "entertainment"
    OTHER = "other"


class CustomerStatus(str, Enum):
    """Customer account status."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    ONBOARDING = "onboarding"
    SUSPENDED = "suspended"


@dataclass
class Customer:
    """Customer data model with industry tagging."""

    customer_id: str
    customer_name: str
    gcp_project_id: str
    dataset: str
    industry: Industry
    status: CustomerStatus = CustomerStatus.ACTIVE
    tags: list[str] = field(default_factory=list)
    onboarded_at: datetime | None = None
    updated_at: datetime | None = None

    # Platform-specific account IDs
    google_ads_customer_ids: list[str] = field(default_factory=list)
    meta_ad_account_ids: list[str] = field(default_factory=list)

    # Secrets references (not actual tokens)
    google_ads_token_secret: str | None = None
    meta_access_token_secret: str | None = None

    @property
    def full_dataset_id(self) -> str:
        """Get fully-qualified dataset ID."""
        return f"{self.gcp_project_id}.{self.dataset}"


class CustomerRegistry:
    """
    Registry for managing customer configurations.

    Supports:
    - BigQuery-backed storage
    - LRU caching with TTL
    - Industry-based queries

    Example:
        registry = CustomerRegistry()
        customer = registry.get_customer("topgolf")
        golf_customers = registry.get_customers_by_industry(Industry.GOLF)
    """

    REGISTRY_DATASET = "growthnav_registry"
    REGISTRY_TABLE = "customers"

    def __init__(
        self,
        registry_project_id: str | None = None,
        cache_ttl: int = 3600,
    ):
        self.registry_project_id = registry_project_id or os.getenv(
            "GROWTNAV_REGISTRY_PROJECT_ID",
            os.getenv("GCP_PROJECT_ID"),
        )
        self.cache_ttl = cache_ttl
        self._client: bigquery.Client | None = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy initialization of BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.registry_project_id)
        return self._client

    @property
    def table_ref(self) -> str:
        """Get fully-qualified registry table reference."""
        return f"{self.registry_project_id}.{self.REGISTRY_DATASET}.{self.REGISTRY_TABLE}"

    @lru_cache(maxsize=100)
    def get_customer(self, customer_id: str) -> Customer | None:
        """
        Get customer configuration by ID.

        Args:
            customer_id: Unique customer identifier

        Returns:
            Customer object or None if not found
        """
        query = f"""
            SELECT *
            FROM `{self.table_ref}`
            WHERE customer_id = @customer_id
              AND status != 'suspended'
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id),
            ]
        )

        result = self.client.query(query, job_config=job_config).result()

        for row in result:
            return self._row_to_customer(dict(row.items()))

        return None

    def get_customers_by_industry(self, industry: Industry) -> list[Customer]:
        """
        Get all active customers in an industry.

        Useful for:
        - Industry benchmarking
        - Cross-customer learning
        - Cohort analysis
        """
        query = f"""
            SELECT *
            FROM `{self.table_ref}`
            WHERE industry = @industry
              AND status = 'active'
            ORDER BY customer_name
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("industry", "STRING", industry.value),
            ]
        )

        result = self.client.query(query, job_config=job_config).result()

        return [self._row_to_customer(dict(row.items())) for row in result]

    def add_customer(self, customer: Customer) -> Customer:
        """
        Add a new customer to the registry.

        Args:
            customer: Customer object to add

        Returns:
            Customer with updated timestamps
        """
        now = datetime.utcnow()
        row = {
            "customer_id": customer.customer_id,
            "customer_name": customer.customer_name,
            "gcp_project_id": customer.gcp_project_id,
            "dataset": customer.dataset,
            "industry": customer.industry.value,
            "status": customer.status.value,
            "tags": customer.tags,
            "google_ads_customer_ids": customer.google_ads_customer_ids,
            "meta_ad_account_ids": customer.meta_ad_account_ids,
            "google_ads_token_secret": customer.google_ads_token_secret,
            "meta_access_token_secret": customer.meta_access_token_secret,
            "onboarded_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        errors = self.client.insert_rows_json(self.table_ref, [row])
        if errors:
            raise RuntimeError(f"Failed to insert customer: {errors}")

        # Clear cache for this customer
        self.get_customer.cache_clear()

        customer.onboarded_at = now
        customer.updated_at = now
        return customer

    def update_customer(self, customer_id: str, updates: dict[str, Any]) -> Customer | None:
        """
        Update customer fields.

        Args:
            customer_id: Customer to update
            updates: Dictionary of fields to update

        Returns:
            Updated Customer or None if not found
        """
        set_clauses = []
        params = [bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id)]

        for key, value in updates.items():
            set_clauses.append(f"{key} = @{key}")
            param_type = self._infer_bq_type(value)
            params.append(bigquery.ScalarQueryParameter(key, param_type, value))

        # Always update timestamp
        set_clauses.append("updated_at = CURRENT_TIMESTAMP()")

        query = f"""
            UPDATE `{self.table_ref}`
            SET {', '.join(set_clauses)}
            WHERE customer_id = @customer_id
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(query, job_config=job_config).result()

        # Clear cache
        self.get_customer.cache_clear()

        return self.get_customer(customer_id)

    def _row_to_customer(self, row: dict[str, Any]) -> Customer:
        """Convert BigQuery row to Customer object."""
        return Customer(
            customer_id=row["customer_id"],
            customer_name=row["customer_name"],
            gcp_project_id=row["gcp_project_id"],
            dataset=row["dataset"],
            industry=Industry(row["industry"]),
            status=CustomerStatus(row["status"]),
            tags=row.get("tags") or [],
            google_ads_customer_ids=row.get("google_ads_customer_ids") or [],
            meta_ad_account_ids=row.get("meta_ad_account_ids") or [],
            google_ads_token_secret=row.get("google_ads_token_secret"),
            meta_access_token_secret=row.get("meta_access_token_secret"),
            onboarded_at=row.get("onboarded_at"),
            updated_at=row.get("updated_at"),
        )

    def _infer_bq_type(self, value: Any) -> str:
        """Infer BigQuery type from Python value."""
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        if isinstance(value, list):
            return "STRING"  # JSON serialized
        return "STRING"
