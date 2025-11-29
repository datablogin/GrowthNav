"""
TenantBigQueryClient - Multi-tenant BigQuery client with automatic isolation.

Provides:
- Dataset-per-customer isolation (growthnav_{customer_id})
- Query validation to prevent destructive operations
- Cost estimation via dry-run queries
- Async query execution support
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

from google.cloud import bigquery
from pydantic import BaseModel


class BigQueryConfig(BaseModel):
    """Configuration for BigQuery client."""

    project_id: str | None = None
    credentials_path: str | None = None
    location: str = "US"
    max_results: int = 10_000
    timeout: int = 300  # 5 minutes

    @classmethod
    def from_env(cls) -> BigQueryConfig:
        """Load configuration from environment variables."""
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID") or os.getenv("GROWTNAV_PROJECT_ID"),
            credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            location=os.getenv("GROWTNAV_BQ_LOCATION", "US"),
        )


@dataclass
class QueryResult:
    """Result of a BigQuery query."""

    rows: list[dict[str, Any]]
    total_rows: int
    bytes_processed: int
    cache_hit: bool


class TenantBigQueryClient:
    """
    BigQuery client with automatic tenant isolation.

    Each customer gets their own dataset: growthnav_{customer_id}

    Example:
        client = TenantBigQueryClient(customer_id="topgolf")
        results = client.query("SELECT * FROM metrics LIMIT 10")
    """

    def __init__(
        self,
        customer_id: str,
        config: BigQueryConfig | None = None,
    ):
        self.customer_id = customer_id
        self.config = config or BigQueryConfig.from_env()
        self._client: bigquery.Client | None = None

    @property
    def dataset_id(self) -> str:
        """Get the customer's dataset ID."""
        return f"growthnav_{self.customer_id}"

    @property
    def client(self) -> bigquery.Client:
        """Lazy initialization of BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(
                project=self.config.project_id,
                location=self.config.location,
            )
        return self._client

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        max_results: int | None = None,
    ) -> QueryResult:
        """
        Execute a query with automatic tenant isolation.

        Args:
            sql: SQL query string
            params: Query parameters for parameterized queries
            max_results: Maximum rows to return (default: 10,000)

        Returns:
            QueryResult with rows, metadata, and cost info
        """
        from growthnav.bigquery.validation import QueryValidator

        # Validate query for safety
        QueryValidator.validate(sql)

        # Build job config
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, self._infer_type(value), value)
                for name, value in params.items()
            ]

        # Execute query
        query_job = self.client.query(sql, job_config=job_config)
        result = query_job.result(
            max_results=max_results or self.config.max_results,
            timeout=self.config.timeout,
        )

        rows = [dict(row.items()) for row in result]

        return QueryResult(
            rows=rows,
            total_rows=result.total_rows or len(rows),
            bytes_processed=query_job.total_bytes_processed or 0,
            cache_hit=query_job.cache_hit or False,
        )

    async def query_async(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        max_results: int | None = None,
    ) -> QueryResult:
        """Async wrapper for query execution."""
        return await asyncio.to_thread(self.query, sql, params, max_results)

    def estimate_cost(self, sql: str) -> dict[str, Any]:
        """
        Estimate query cost via dry-run.

        Returns:
            Dict with bytes_processed, estimated_cost_usd, is_cached
        """
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.client.query(sql, job_config=job_config)

        bytes_processed = query_job.total_bytes_processed or 0
        # BigQuery pricing: $6.25 per TB (as of 2024)
        cost_per_tb = 6.25
        estimated_cost = (bytes_processed / (1024**4)) * cost_per_tb

        return {
            "bytes_processed": bytes_processed,
            "estimated_cost_usd": round(estimated_cost, 6),
            "is_cached": query_job.cache_hit or False,
        }

    def get_table_schema(self, table_id: str) -> list[dict[str, Any]]:
        """
        Get schema for a table in the customer's dataset.

        Args:
            table_id: Table name (without dataset prefix)

        Returns:
            List of field definitions
        """
        full_table_id = f"{self.config.project_id}.{self.dataset_id}.{table_id}"
        table = self.client.get_table(full_table_id)

        return [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description,
            }
            for field in table.schema
        ]

    def _infer_type(self, value: Any) -> str:
        """Infer BigQuery type from Python value."""
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        return "STRING"
