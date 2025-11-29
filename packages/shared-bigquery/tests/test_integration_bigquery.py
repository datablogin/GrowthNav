"""
Real integration tests for shared-bigquery package.

These tests hit actual GCP BigQuery services - no mocks.

Prerequisites:
- GCP authentication via `gcloud auth application-default login`
- growthnav_test dataset with test_metrics table
- growthnav_registry dataset with customers table

Run with: uv run pytest packages/shared-bigquery/tests/test_integration_bigquery.py -v
"""

import os
import pytest
from datetime import datetime, timezone

from growthnav.bigquery.client import TenantBigQueryClient, BigQueryConfig, QueryResult
from growthnav.bigquery.registry import CustomerRegistry, Customer, Industry, CustomerStatus
from growthnav.bigquery.validation import QueryValidator, ValidationResult, ValidationSeverity


# Skip all tests in this file if GCP credentials are not available
pytestmark = pytest.mark.skipif(
    not os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
    reason="GCP credentials not available"
)


class TestTenantBigQueryClientIntegration:
    """Real integration tests for TenantBigQueryClient."""

    @pytest.fixture
    def client(self):
        """Create a real BigQuery client for the test customer."""
        return TenantBigQueryClient(customer_id="test")

    def test_client_connects_to_bigquery(self, client):
        """Verify client can connect to BigQuery."""
        # Simple query that should always work
        result = client.query("SELECT 1 as test_value")

        assert isinstance(result, QueryResult)
        assert len(result.rows) == 1
        assert result.rows[0]["test_value"] == 1
        assert result.total_rows == 1

    def test_query_returns_query_result(self, client):
        """Verify query returns proper QueryResult with metadata."""
        result = client.query("SELECT 1 as num, 'hello' as greeting")

        assert isinstance(result, QueryResult)
        assert result.rows == [{"num": 1, "greeting": "hello"}]
        assert result.total_rows == 1
        assert isinstance(result.bytes_processed, int)
        assert isinstance(result.cache_hit, bool)

    def test_query_with_real_table(self, client):
        """Query data from the test_metrics table."""
        result = client.query("""
            SELECT id, name, value
            FROM `topgolf-460202.growthnav_test.test_metrics`
            ORDER BY id
            LIMIT 10
        """)

        assert isinstance(result, QueryResult)
        assert len(result.rows) >= 1
        # Verify structure of returned data
        first_row = result.rows[0]
        assert "id" in first_row
        assert "name" in first_row
        assert "value" in first_row

    def test_query_with_parameters(self, client):
        """Query with parameterized values."""
        result = client.query(
            "SELECT @name as name, @value as value",
            params={"name": "test_param", "value": 42}
        )

        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "test_param"
        assert result.rows[0]["value"] == 42

    def test_query_with_different_param_types(self, client):
        """Verify type inference for different parameter types."""
        result = client.query(
            """
            SELECT
                @str_val as str_val,
                @int_val as int_val,
                @float_val as float_val,
                @bool_val as bool_val
            """,
            params={
                "str_val": "hello",
                "int_val": 123,
                "float_val": 45.67,
                "bool_val": True,
            }
        )

        row = result.rows[0]
        assert row["str_val"] == "hello"
        assert row["int_val"] == 123
        assert row["float_val"] == 45.67
        assert row["bool_val"] is True

    def test_query_blocks_destructive_sql(self, client):
        """Verify destructive SQL is blocked."""
        with pytest.raises(ValueError, match="DROP statements are not allowed"):
            client.query("DROP TABLE test_table")

        with pytest.raises(ValueError, match="DELETE statements are not allowed"):
            client.query("DELETE FROM test_table WHERE 1=1")

        with pytest.raises(ValueError, match="UPDATE statements are not allowed"):
            client.query("UPDATE test_table SET x = 1")

    def test_estimate_cost_returns_estimate(self, client):
        """Verify cost estimation works with real BigQuery."""
        estimate = client.estimate_cost("""
            SELECT *
            FROM `topgolf-460202.growthnav_test.test_metrics`
        """)

        assert "bytes_processed" in estimate
        assert "estimated_cost_usd" in estimate
        assert "is_cached" in estimate
        assert isinstance(estimate["bytes_processed"], int)
        assert isinstance(estimate["estimated_cost_usd"], float)

    def test_dataset_id_follows_pattern(self, client):
        """Verify dataset_id follows growthnav_{customer_id} pattern."""
        assert client.dataset_id == "growthnav_test"

        another_client = TenantBigQueryClient(customer_id="topgolf")
        assert another_client.dataset_id == "growthnav_topgolf"

    def test_get_table_schema(self, client):
        """Get schema from a real table."""
        # Use the project ID from config
        config = BigQueryConfig.from_env()
        client_with_project = TenantBigQueryClient(
            customer_id="test",
            config=BigQueryConfig(project_id=config.project_id or "topgolf-460202")
        )

        # Get schema from the test table (in growthnav_test dataset)
        schema = client_with_project.get_table_schema("test_metrics")

        assert isinstance(schema, list)
        assert len(schema) >= 1

        # Find the 'id' field
        field_names = [f["name"] for f in schema]
        assert "id" in field_names
        assert "name" in field_names
        assert "value" in field_names

    def test_max_results_limits_rows(self, client):
        """Verify max_results parameter limits returned rows."""
        result = client.query(
            "SELECT * FROM UNNEST(GENERATE_ARRAY(1, 100)) as num",
            max_results=5
        )

        assert len(result.rows) == 5

    def test_config_from_env(self):
        """Verify config loads from environment."""
        config = BigQueryConfig.from_env()

        # Should have some default values
        assert config.location == "US" or config.location is not None
        assert config.max_results == 10_000
        assert config.timeout == 300


class TestCustomerRegistryIntegration:
    """Real integration tests for CustomerRegistry."""

    @pytest.fixture
    def registry(self):
        """Create a real CustomerRegistry."""
        registry = CustomerRegistry()
        # Clear any cached data
        registry.get_customer.cache_clear()
        return registry

    def test_get_customer_returns_customer(self, registry):
        """Get a customer that exists in the registry."""
        customer = registry.get_customer("test_customer")

        assert customer is not None
        assert isinstance(customer, Customer)
        assert customer.customer_id == "test_customer"
        assert customer.customer_name == "Test Customer Inc"
        assert customer.industry == Industry.GOLF
        assert customer.status == CustomerStatus.ACTIVE

    def test_get_customer_returns_none_for_nonexistent(self, registry):
        """Return None for customer that doesn't exist."""
        customer = registry.get_customer("definitely_not_a_real_customer_12345")

        assert customer is None

    def test_get_customers_by_industry(self, registry):
        """Get all customers in an industry."""
        customers = registry.get_customers_by_industry(Industry.GOLF)

        assert isinstance(customers, list)
        assert len(customers) >= 1

        # All returned customers should be in the golf industry
        for customer in customers:
            assert customer.industry == Industry.GOLF
            assert customer.status == CustomerStatus.ACTIVE

    def test_customer_has_full_dataset_id(self, registry):
        """Verify full_dataset_id property works."""
        customer = registry.get_customer("test_customer")

        assert customer is not None
        assert "." in customer.full_dataset_id
        assert customer.dataset in customer.full_dataset_id

    def test_registry_table_ref_format(self, registry):
        """Verify table_ref has correct format."""
        table_ref = registry.table_ref

        assert "growthnav_registry" in table_ref
        assert "customers" in table_ref
        # Should be project.dataset.table format
        parts = table_ref.split(".")
        assert len(parts) == 3

    def test_customer_caching(self, registry):
        """Verify get_customer uses LRU cache."""
        # First call
        customer1 = registry.get_customer("test_customer")

        # Second call should hit cache
        customer2 = registry.get_customer("test_customer")

        assert customer1 is not None
        assert customer2 is not None
        assert customer1.customer_id == customer2.customer_id

    def test_customer_tags_loaded(self, registry):
        """Verify customer tags are loaded properly."""
        customer = registry.get_customer("test_customer")

        assert customer is not None
        assert isinstance(customer.tags, list)
        assert "test" in customer.tags
        assert "integration" in customer.tags


class TestQueryValidatorIntegration:
    """Integration tests for QueryValidator with real queries."""

    def test_validate_safe_query(self):
        """Validate a safe SELECT query."""
        result = QueryValidator.validate("SELECT * FROM users LIMIT 10")

        assert result.is_valid is True

    def test_validate_blocks_drop(self):
        """Block DROP statements."""
        with pytest.raises(ValueError, match="DROP statements are not allowed"):
            QueryValidator.validate("DROP TABLE users")

    def test_validate_blocks_delete(self):
        """Block DELETE statements."""
        with pytest.raises(ValueError, match="DELETE statements are not allowed"):
            QueryValidator.validate("DELETE FROM users WHERE 1=1")

    def test_validate_blocks_truncate(self):
        """Block TRUNCATE statements."""
        with pytest.raises(ValueError, match="TRUNCATE statements are not allowed"):
            QueryValidator.validate("TRUNCATE TABLE users")

    def test_validate_blocks_update(self):
        """Block UPDATE statements."""
        with pytest.raises(ValueError, match="UPDATE statements are not allowed"):
            QueryValidator.validate("UPDATE users SET active = false")

    def test_validate_blocks_insert(self):
        """Block INSERT statements."""
        with pytest.raises(ValueError, match="INSERT statements are not allowed"):
            QueryValidator.validate("INSERT INTO users VALUES (1, 'test')")

    def test_validate_allows_writes_when_enabled(self):
        """Allow writes when allow_writes=True."""
        result = QueryValidator.validate("DELETE FROM temp_table", allow_writes=True)

        assert result.is_valid is True

    def test_validate_warns_on_select_star(self):
        """Warn on SELECT * usage."""
        result = QueryValidator.validate("SELECT * FROM users LIMIT 10")

        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "SELECT *" in result.message

    def test_validate_warns_on_no_limit(self):
        """Warn when SELECT has no LIMIT."""
        result = QueryValidator.validate("SELECT id, name FROM users")

        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "LIMIT" in result.message

    def test_sanitize_valid_identifier(self):
        """Sanitize valid identifiers."""
        assert QueryValidator.sanitize_identifier("users") == "users"
        assert QueryValidator.sanitize_identifier("user_data") == "user_data"
        assert QueryValidator.sanitize_identifier("Table_123") == "Table_123"

    def test_sanitize_rejects_invalid_identifier(self):
        """Reject invalid identifiers."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            QueryValidator.sanitize_identifier("123_invalid")

        with pytest.raises(ValueError, match="Invalid identifier"):
            QueryValidator.sanitize_identifier("user;DROP")

        with pytest.raises(ValueError, match="Invalid identifier"):
            QueryValidator.sanitize_identifier("")

    def test_validate_project_dataset_valid(self):
        """Validate correct project and dataset."""
        result = QueryValidator.validate_project_dataset(
            "topgolf-460202",
            "growthnav_test"
        )

        assert result is True

    def test_validate_project_dataset_invalid_project(self):
        """Reject invalid project ID."""
        with pytest.raises(ValueError, match="Invalid project ID"):
            QueryValidator.validate_project_dataset("Invalid_Project", "dataset")

    def test_validate_project_dataset_invalid_dataset(self):
        """Reject invalid dataset."""
        with pytest.raises(ValueError, match="Invalid dataset"):
            QueryValidator.validate_project_dataset("valid-project", "123invalid")


class TestBigQueryAsyncIntegration:
    """Test async query execution."""

    @pytest.fixture
    def client(self):
        """Create a real BigQuery client."""
        return TenantBigQueryClient(customer_id="test")

    @pytest.mark.asyncio
    async def test_query_async(self, client):
        """Execute async query."""
        result = await client.query_async("SELECT 1 as value, 'async' as type")

        assert isinstance(result, QueryResult)
        assert len(result.rows) == 1
        assert result.rows[0]["value"] == 1
        assert result.rows[0]["type"] == "async"

    @pytest.mark.asyncio
    async def test_query_async_with_params(self, client):
        """Execute async query with parameters."""
        result = await client.query_async(
            "SELECT @num * 2 as doubled",
            params={"num": 21}
        )

        assert result.rows[0]["doubled"] == 42


class TestEndToEndWorkflow:
    """End-to-end workflow tests."""

    def test_tenant_isolation_workflow(self):
        """Test complete tenant isolation workflow."""
        # Create client for test tenant
        client = TenantBigQueryClient(customer_id="test")

        # Verify dataset ID
        assert client.dataset_id == "growthnav_test"

        # Query some data
        result = client.query("SELECT 'tenant_test' as workflow LIMIT 1")
        assert result.rows[0]["workflow"] == "tenant_test"

        # Estimate cost
        estimate = client.estimate_cost("SELECT 1")
        assert "bytes_processed" in estimate

    def test_registry_to_client_workflow(self):
        """Get customer from registry and create client for them."""
        # Get customer from registry
        registry = CustomerRegistry()
        registry.get_customer.cache_clear()

        customer = registry.get_customer("test_customer")
        assert customer is not None

        # Create client for this customer
        client = TenantBigQueryClient(customer_id=customer.customer_id)

        # Verify the client's dataset matches
        assert f"growthnav_{customer.customer_id}" == client.dataset_id

        # Execute a query
        result = client.query("SELECT CURRENT_TIMESTAMP() as now")
        assert len(result.rows) == 1
