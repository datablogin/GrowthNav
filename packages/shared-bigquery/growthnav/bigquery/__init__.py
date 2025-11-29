"""
GrowthNav BigQuery - Shared BigQuery client with multi-tenant support.

Usage:
    from growthnav.bigquery import TenantBigQueryClient, CustomerRegistry

    # Query with tenant isolation
    client = TenantBigQueryClient(customer_id="topgolf")
    results = client.query("SELECT * FROM metrics")

    # Customer registry operations
    registry = CustomerRegistry()
    customer = registry.get_customer("topgolf")
"""

from growthnav.bigquery.client import TenantBigQueryClient
from growthnav.bigquery.registry import Customer, CustomerRegistry, CustomerStatus, Industry
from growthnav.bigquery.validation import QueryValidator

__all__ = [
    "TenantBigQueryClient",
    "CustomerRegistry",
    "Customer",
    "QueryValidator",
    "Industry",
    "CustomerStatus",
]
