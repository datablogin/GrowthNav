"""Tests for TenantBigQueryClient."""

from unittest.mock import MagicMock, Mock, patch

from growthnav.bigquery.client import (
    BigQueryConfig,
    QueryResult,
    TenantBigQueryClient,
)


class TestBigQueryConfig:
    """Test suite for BigQueryConfig."""

    def test_default_values(self):
        """Test BigQueryConfig default values."""
        config = BigQueryConfig()
        assert config.project_id is None
        assert config.credentials_path is None
        assert config.location == "US"
        assert config.max_results == 10_000
        assert config.timeout == 300

    def test_custom_values(self):
        """Test BigQueryConfig with custom values."""
        config = BigQueryConfig(
            project_id="test-project",
            credentials_path="/path/to/creds.json",
            location="EU",
            max_results=5000,
            timeout=600,
        )
        assert config.project_id == "test-project"
        assert config.credentials_path == "/path/to/creds.json"
        assert config.location == "EU"
        assert config.max_results == 5000
        assert config.timeout == 600

    def test_from_env_with_gcp_project_id(self, monkeypatch):
        """Test BigQueryConfig.from_env() with GCP_PROJECT_ID."""
        monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
        monkeypatch.setenv("GROWTNAV_BQ_LOCATION", "EU")

        config = BigQueryConfig.from_env()
        assert config.project_id == "my-project"
        assert config.credentials_path == "/path/to/creds.json"
        assert config.location == "EU"

    def test_from_env_with_growthnav_project_id(self, monkeypatch):
        """Test BigQueryConfig.from_env() with GROWTNAV_PROJECT_ID fallback."""
        # Clear GCP_PROJECT_ID so GROWTNAV_PROJECT_ID is used as fallback
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
        monkeypatch.setenv("GROWTNAV_PROJECT_ID", "growthnav-project")
        monkeypatch.setenv("GROWTNAV_BQ_LOCATION", "US")

        config = BigQueryConfig.from_env()
        assert config.project_id == "growthnav-project"
        assert config.location == "US"

    def test_from_env_defaults(self, monkeypatch):
        """Test BigQueryConfig.from_env() with no environment variables."""
        # Clear any existing env vars
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("GROWTNAV_PROJECT_ID", raising=False)
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        monkeypatch.delenv("GROWTNAV_BQ_LOCATION", raising=False)

        config = BigQueryConfig.from_env()
        assert config.project_id is None
        assert config.credentials_path is None
        assert config.location == "US"


class TestTenantBigQueryClient:
    """Test suite for TenantBigQueryClient."""

    def test_dataset_id_format(self):
        """Test dataset_id returns growthnav_{customer_id}."""
        client = TenantBigQueryClient(customer_id="topgolf")
        assert client.dataset_id == "growthnav_topgolf"

        client = TenantBigQueryClient(customer_id="test-customer")
        assert client.dataset_id == "growthnav_test-customer"

    def test_initialization_with_default_config(self):
        """Test client initialization with default config."""
        with patch("growthnav.bigquery.client.BigQueryConfig.from_env") as mock_from_env:
            mock_config = BigQueryConfig(project_id="test-project")
            mock_from_env.return_value = mock_config

            client = TenantBigQueryClient(customer_id="topgolf")
            assert client.customer_id == "topgolf"
            assert client.config.project_id == "test-project"
            mock_from_env.assert_called_once()

    def test_initialization_with_custom_config(self):
        """Test client initialization with custom config."""
        config = BigQueryConfig(project_id="custom-project", location="EU")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client.customer_id == "topgolf"
        assert client.config.project_id == "custom-project"
        assert client.config.location == "EU"

    @patch("growthnav.bigquery.client.bigquery.Client")
    def test_client_lazy_initialization(self, mock_bq_client):
        """Test BigQuery client is lazily initialized."""
        config = BigQueryConfig(project_id="test-project", location="US")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        # Client should not be initialized yet
        assert client._client is None

        # Access client property
        _ = client.client

        # Now it should be initialized
        mock_bq_client.assert_called_once_with(
            project="test-project",
            location="US",
        )

    @patch("growthnav.bigquery.client.bigquery.Client")
    @patch("growthnav.bigquery.validation.QueryValidator.validate")
    def test_query_validates_sql(self, mock_validate, mock_bq_client):
        """Test query validates SQL before execution."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        # Mock the BigQuery client
        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([])
        mock_result.total_rows = 0

        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        mock_job.total_bytes_processed = 1000
        mock_job.cache_hit = False
        mock_bq_client.return_value.query.return_value = mock_job

        sql = "SELECT * FROM customers LIMIT 10"
        client.query(sql)

        # Verify validation was called
        mock_validate.assert_called_once_with(sql)

    @patch("growthnav.bigquery.client.bigquery.Client")
    @patch("growthnav.bigquery.validation.QueryValidator.validate")
    def test_query_returns_query_result(self, mock_validate, mock_bq_client):
        """Test query returns QueryResult with correct fields."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        # Mock query result
        mock_row1 = Mock()
        mock_row1.items.return_value = [("id", 1), ("name", "Test")]
        mock_row2 = Mock()
        mock_row2.items.return_value = [("id", 2), ("name", "Test2")]

        mock_result = MagicMock()
        mock_result.__iter__.return_value = [mock_row1, mock_row2]
        mock_result.total_rows = 2

        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        mock_job.total_bytes_processed = 1500
        mock_job.cache_hit = True

        mock_bq_client.return_value.query.return_value = mock_job

        result = client.query("SELECT id, name FROM customers LIMIT 10")

        assert isinstance(result, QueryResult)
        assert result.rows == [{"id": 1, "name": "Test"}, {"id": 2, "name": "Test2"}]
        assert result.total_rows == 2
        assert result.bytes_processed == 1500
        assert result.cache_hit is True

    @patch("growthnav.bigquery.client.bigquery.Client")
    @patch("growthnav.bigquery.validation.QueryValidator.validate")
    def test_query_with_params(self, mock_validate, mock_bq_client):
        """Test query with parameters."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([])
        mock_result.total_rows = 0

        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        mock_job.total_bytes_processed = 1000
        mock_job.cache_hit = False
        mock_bq_client.return_value.query.return_value = mock_job

        params = {"customer_id": "topgolf", "min_value": 100}
        client.query("SELECT * FROM customers WHERE id = @customer_id", params=params)

        # Verify query was called with job config containing parameters
        call_args = mock_bq_client.return_value.query.call_args
        assert call_args is not None
        job_config = call_args[1]["job_config"]
        assert len(job_config.query_parameters) == 2

    @patch("growthnav.bigquery.client.bigquery.Client")
    def test_estimate_cost(self, mock_bq_client):
        """Test estimate_cost returns cost dict."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        # Mock dry run query
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 1024**4  # 1 TB
        mock_job.cache_hit = False
        mock_bq_client.return_value.query.return_value = mock_job

        result = client.estimate_cost("SELECT * FROM big_table")

        assert "bytes_processed" in result
        assert "estimated_cost_usd" in result
        assert "is_cached" in result
        assert result["bytes_processed"] == 1024**4
        assert result["estimated_cost_usd"] == 6.25  # 1 TB * $6.25
        assert result["is_cached"] is False

    @patch("growthnav.bigquery.client.bigquery.Client")
    def test_estimate_cost_cached(self, mock_bq_client):
        """Test estimate_cost with cached query."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.cache_hit = True
        mock_bq_client.return_value.query.return_value = mock_job

        result = client.estimate_cost("SELECT * FROM small_table")

        assert result["bytes_processed"] == 0
        assert result["estimated_cost_usd"] == 0.0
        assert result["is_cached"] is True

    def test_infer_type_bool(self):
        """Test _infer_type for boolean values."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client._infer_type(True) == "BOOL"
        assert client._infer_type(False) == "BOOL"

    def test_infer_type_int(self):
        """Test _infer_type for integer values."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client._infer_type(42) == "INT64"
        assert client._infer_type(0) == "INT64"
        assert client._infer_type(-100) == "INT64"

    def test_infer_type_float(self):
        """Test _infer_type for float values."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client._infer_type(3.14) == "FLOAT64"
        assert client._infer_type(0.0) == "FLOAT64"
        assert client._infer_type(-2.5) == "FLOAT64"

    def test_infer_type_string(self):
        """Test _infer_type for string values."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client._infer_type("hello") == "STRING"
        assert client._infer_type("") == "STRING"

    def test_infer_type_other(self):
        """Test _infer_type for other types defaults to STRING."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        assert client._infer_type([1, 2, 3]) == "STRING"
        assert client._infer_type({"key": "value"}) == "STRING"
        assert client._infer_type(None) == "STRING"

    @patch("growthnav.bigquery.client.bigquery.Client")
    def test_get_table_schema(self, mock_bq_client):
        """Test get_table_schema returns schema fields."""
        config = BigQueryConfig(project_id="test-project")
        client = TenantBigQueryClient(customer_id="topgolf", config=config)

        # Mock table schema
        mock_field1 = Mock()
        mock_field1.name = "id"
        mock_field1.field_type = "INT64"
        mock_field1.mode = "REQUIRED"
        mock_field1.description = "Primary key"

        mock_field2 = Mock()
        mock_field2.name = "name"
        mock_field2.field_type = "STRING"
        mock_field2.mode = "NULLABLE"
        mock_field2.description = "Customer name"

        mock_table = Mock()
        mock_table.schema = [mock_field1, mock_field2]
        mock_bq_client.return_value.get_table.return_value = mock_table

        schema = client.get_table_schema("customers")

        assert len(schema) == 2
        assert schema[0] == {
            "name": "id",
            "type": "INT64",
            "mode": "REQUIRED",
            "description": "Primary key",
        }
        assert schema[1] == {
            "name": "name",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Customer name",
        }

        # Verify correct table reference was used
        expected_table_id = "test-project.growthnav_topgolf.customers"
        mock_bq_client.return_value.get_table.assert_called_once_with(expected_table_id)
