"""BigQuery dataset provisioning for new customers.

Handles the creation and management of BigQuery datasets
following the GrowthNav tenant isolation pattern.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import bigquery


logger = logging.getLogger(__name__)


@dataclass
class ProvisioningConfig:
    """Configuration for dataset provisioning.

    Attributes:
        project_id: GCP project ID for BigQuery.
        location: BigQuery dataset location (default: US).
        default_table_expiration_ms: Optional table expiration time.
        labels: Labels to apply to created datasets.
    """

    project_id: str
    location: str = "US"
    default_table_expiration_ms: int | None = None
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> ProvisioningConfig:
        """Create configuration from environment variables.

        Environment variables:
            GCP_PROJECT_ID or GROWTNAV_PROJECT_ID: GCP project ID
            GROWTNAV_BQ_LOCATION: BigQuery location (default: US)
        """
        project_id = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GROWTNAV_PROJECT_ID")
        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID or GROWTNAV_PROJECT_ID environment variable required"
            )

        location = os.environ.get("GROWTNAV_BQ_LOCATION", "US")

        return cls(
            project_id=project_id,
            location=location,
            labels={"managed_by": "growthnav"},
        )


class DatasetProvisioner:
    """Provisions BigQuery datasets for new customers.

    Creates datasets following the GrowthNav naming convention:
    `growthnav_{customer_id}`

    Example:
        >>> provisioner = DatasetProvisioner()
        >>> dataset_id = provisioner.create_dataset("acme_corp")
        >>> print(dataset_id)
        'my-project.growthnav_acme_corp'
    """

    def __init__(self, config: ProvisioningConfig | None = None):
        """Initialize the provisioner.

        Args:
            config: Provisioning configuration. If None, loads from environment.
        """
        self._config = config
        self._client: bigquery.Client | None = None

    @property
    def config(self) -> ProvisioningConfig:
        """Lazy-initialize configuration from environment."""
        if self._config is None:
            self._config = ProvisioningConfig.from_env()
        return self._config

    @property
    def client(self) -> bigquery.Client:
        """Lazy-initialize BigQuery client."""
        if self._client is None:
            from google.cloud import bigquery

            self._client = bigquery.Client(project=self.config.project_id)
        return self._client

    def _get_dataset_id(self, customer_id: str) -> str:
        """Get the dataset ID for a customer."""
        return f"growthnav_{customer_id}"

    def _get_full_dataset_id(self, customer_id: str) -> str:
        """Get the fully qualified dataset ID."""
        return f"{self.config.project_id}.{self._get_dataset_id(customer_id)}"

    def _sanitize_label_value(self, value: str, max_length: int = 63) -> str:
        """Sanitize a string for use as a GCP label value.

        GCP labels must match [a-z0-9_-]{1,63} pattern.

        Args:
            value: The value to sanitize.
            max_length: Maximum length for the label (default: 63).

        Returns:
            Sanitized label value.
        """
        # Convert to lowercase and replace underscores with hyphens
        sanitized = value.lower().replace("_", "-")
        # Remove any characters that aren't lowercase letters, numbers, or hyphens
        sanitized = "".join(c for c in sanitized if c.isalnum() or c == "-")
        return sanitized[:max_length]

    def create_dataset(self, customer_id: str) -> str:
        """Create a new dataset for a customer.

        Args:
            customer_id: The customer identifier.

        Returns:
            The fully qualified dataset ID (project.dataset).

        Raises:
            ValueError: If customer_id is invalid.
        """
        from google.cloud import bigquery

        if not customer_id:
            raise ValueError("customer_id is required")

        full_dataset_id = self._get_full_dataset_id(customer_id)

        # Create dataset reference
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = self.config.location

        # Apply labels (sanitized for GCP compatibility)
        labels = dict(self.config.labels)
        labels["customer_id"] = self._sanitize_label_value(customer_id)
        dataset.labels = labels

        # Set table expiration if configured
        if self.config.default_table_expiration_ms:
            dataset.default_table_expiration_ms = self.config.default_table_expiration_ms

        # Create the dataset (exists_ok to be idempotent)
        self.client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Created dataset: {full_dataset_id}")

        return full_dataset_id

    def create_standard_tables(self, customer_id: str) -> list[str]:
        """Create standard tables for a customer dataset.

        Creates the following tables:
        - conversions: Unified conversion data
        - daily_metrics: Aggregated daily metrics

        Args:
            customer_id: The customer identifier.

        Returns:
            List of created table IDs.
        """
        from google.cloud import bigquery

        dataset_id = self._get_full_dataset_id(customer_id)
        created_tables = []

        # Conversions table schema
        conversions_schema = [
            bigquery.SchemaField("conversion_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transaction_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("conversion_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("gclid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fbclid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("utm_source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("utm_medium", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("utm_campaign", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attributed_platform", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attribution_model", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        conversions_table = bigquery.Table(
            f"{dataset_id}.conversions",
            schema=conversions_schema,
        )
        conversions_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        conversions_table.clustering_fields = ["conversion_type", "source"]

        self.client.create_table(conversions_table, exists_ok=True)
        created_tables.append(f"{dataset_id}.conversions")
        logger.info(f"Created table: {dataset_id}.conversions")

        # Daily metrics table schema
        metrics_schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("impressions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("spend", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("conversions", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        metrics_table = bigquery.Table(
            f"{dataset_id}.daily_metrics",
            schema=metrics_schema,
        )
        metrics_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        metrics_table.clustering_fields = ["platform", "campaign_id"]

        self.client.create_table(metrics_table, exists_ok=True)
        created_tables.append(f"{dataset_id}.daily_metrics")
        logger.info(f"Created table: {dataset_id}.daily_metrics")

        return created_tables

    def dataset_exists(self, customer_id: str) -> bool:
        """Check if a dataset exists for a customer.

        Args:
            customer_id: The customer identifier.

        Returns:
            True if the dataset exists.
        """
        from google.cloud.exceptions import NotFound

        full_dataset_id = self._get_full_dataset_id(customer_id)

        try:
            self.client.get_dataset(full_dataset_id)
            return True
        except NotFound:
            return False

    def delete_dataset(self, customer_id: str, delete_contents: bool = True) -> bool:
        """Delete a customer's dataset.

        WARNING: This is a destructive operation!

        Args:
            customer_id: The customer identifier.
            delete_contents: If True, delete all tables in the dataset.

        Returns:
            True if deleted successfully.
        """
        from google.cloud.exceptions import NotFound

        full_dataset_id = self._get_full_dataset_id(customer_id)

        try:
            self.client.delete_dataset(full_dataset_id, delete_contents=delete_contents)
            logger.warning(f"Deleted dataset: {full_dataset_id}")
            return True
        except NotFound:
            logger.warning(f"Dataset not found for deletion: {full_dataset_id}")
            return False

    def list_tables(self, customer_id: str) -> list[str]:
        """List all tables in a customer's dataset.

        Args:
            customer_id: The customer identifier.

        Returns:
            List of table IDs.
        """
        full_dataset_id = self._get_full_dataset_id(customer_id)

        tables = self.client.list_tables(full_dataset_id)
        return [table.table_id for table in tables]
