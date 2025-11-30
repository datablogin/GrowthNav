"""Tests for DatasetProvisioner."""

from unittest.mock import MagicMock, patch

import pytest
from growthnav.onboarding import DatasetProvisioner, ProvisioningConfig


class TestProvisioningConfig:
    """Test ProvisioningConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ProvisioningConfig(project_id="test-project")

        assert config.project_id == "test-project"
        assert config.location == "US"
        assert config.default_table_expiration_ms is None
        assert config.labels == {}

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ProvisioningConfig(
            project_id="custom-project",
            location="EU",
            default_table_expiration_ms=86400000,
            labels={"env": "test"},
        )

        assert config.project_id == "custom-project"
        assert config.location == "EU"
        assert config.default_table_expiration_ms == 86400000
        assert config.labels == {"env": "test"}

    def test_from_env_with_gcp_project_id(self, monkeypatch):
        """Test loading config from GCP_PROJECT_ID env var."""
        monkeypatch.setenv("GCP_PROJECT_ID", "env-project")
        monkeypatch.delenv("GROWTNAV_PROJECT_ID", raising=False)

        config = ProvisioningConfig.from_env()

        assert config.project_id == "env-project"
        assert config.location == "US"
        assert config.labels == {"managed_by": "growthnav"}

    def test_from_env_with_growthnav_project_id(self, monkeypatch):
        """Test loading config from GROWTNAV_PROJECT_ID env var."""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.setenv("GROWTNAV_PROJECT_ID", "growthnav-project")

        config = ProvisioningConfig.from_env()

        assert config.project_id == "growthnav-project"

    def test_from_env_with_custom_location(self, monkeypatch):
        """Test loading location from env var."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GROWTNAV_BQ_LOCATION", "EU")

        config = ProvisioningConfig.from_env()

        assert config.location == "EU"

    def test_from_env_raises_without_project_id(self, monkeypatch):
        """Test from_env raises when no project ID is set."""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("GROWTNAV_PROJECT_ID", raising=False)

        with pytest.raises(ValueError, match="environment variable required"):
            ProvisioningConfig.from_env()


class TestDatasetProvisioner:
    """Test DatasetProvisioner class."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(
            project_id="test-project",
            location="US",
            labels={"managed_by": "growthnav"},
        )

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_config_lazy_initialization(self, monkeypatch):
        """Test config is lazily initialized from env."""
        monkeypatch.setenv("GCP_PROJECT_ID", "lazy-project")

        provisioner = DatasetProvisioner()

        # Access config triggers initialization
        config = provisioner.config

        assert config.project_id == "lazy-project"

    def test_client_lazy_initialization(self, config, mock_bq_client):
        """Test BigQuery client is lazily initialized."""
        provisioner = DatasetProvisioner(config=config)

        # Client not created yet
        assert provisioner._client is None

        # Access client triggers initialization
        client = provisioner.client

        assert client is mock_bq_client

    def test_get_dataset_id(self, config):
        """Test dataset ID generation."""
        provisioner = DatasetProvisioner(config=config)

        dataset_id = provisioner._get_dataset_id("test_customer")

        assert dataset_id == "growthnav_test_customer"

    def test_get_full_dataset_id(self, config):
        """Test full dataset ID generation."""
        provisioner = DatasetProvisioner(config=config)

        full_id = provisioner._get_full_dataset_id("test_customer")

        assert full_id == "test-project.growthnav_test_customer"


class TestDatasetProvisionerCreateDataset:
    """Test create_dataset method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(
            project_id="test-project",
            location="US",
            labels={"managed_by": "growthnav"},
        )

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_create_dataset_success(self, config, mock_bq_client):
        """Test successful dataset creation."""
        provisioner = DatasetProvisioner(config=config)

        result = provisioner.create_dataset("test_customer")

        assert result == "test-project.growthnav_test_customer"
        mock_bq_client.create_dataset.assert_called_once()

        # Verify dataset configuration
        call_args = mock_bq_client.create_dataset.call_args
        dataset = call_args[0][0]
        assert dataset.location == "US"
        assert "customer_id" in dataset.labels
        assert dataset.labels["customer_id"] == "test_customer"

    def test_create_dataset_with_exists_ok(self, config, mock_bq_client):
        """Test create_dataset uses exists_ok=True."""
        provisioner = DatasetProvisioner(config=config)

        provisioner.create_dataset("test_customer")

        call_args = mock_bq_client.create_dataset.call_args
        assert call_args[1]["exists_ok"] is True

    def test_create_dataset_with_table_expiration(self, mock_bq_client):
        """Test create_dataset with table expiration."""
        config = ProvisioningConfig(
            project_id="test-project",
            default_table_expiration_ms=86400000,  # 1 day
        )
        provisioner = DatasetProvisioner(config=config)

        provisioner.create_dataset("test_customer")

        call_args = mock_bq_client.create_dataset.call_args
        dataset = call_args[0][0]
        assert dataset.default_table_expiration_ms == 86400000

    def test_create_dataset_raises_for_empty_customer_id(self, config, mock_bq_client):
        """Test create_dataset raises for empty customer_id."""
        provisioner = DatasetProvisioner(config=config)

        with pytest.raises(ValueError, match="customer_id is required"):
            provisioner.create_dataset("")


class TestDatasetProvisionerCreateStandardTables:
    """Test create_standard_tables method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(project_id="test-project")

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_create_standard_tables(self, config, mock_bq_client):
        """Test creating standard tables."""
        provisioner = DatasetProvisioner(config=config)

        tables = provisioner.create_standard_tables("test_customer")

        assert len(tables) == 2
        assert "test-project.growthnav_test_customer.conversions" in tables
        assert "test-project.growthnav_test_customer.daily_metrics" in tables
        assert mock_bq_client.create_table.call_count == 2

    def test_conversions_table_has_partitioning(self, config, mock_bq_client):
        """Test conversions table has time partitioning."""
        provisioner = DatasetProvisioner(config=config)

        provisioner.create_standard_tables("test_customer")

        # Check conversions table (first call)
        call_args = mock_bq_client.create_table.call_args_list[0]
        table = call_args[0][0]

        assert table.time_partitioning is not None
        assert table.time_partitioning.field == "timestamp"

    def test_tables_have_clustering(self, config, mock_bq_client):
        """Test tables have clustering fields."""
        provisioner = DatasetProvisioner(config=config)

        provisioner.create_standard_tables("test_customer")

        # Check conversions table clustering
        conversions_table = mock_bq_client.create_table.call_args_list[0][0][0]
        assert conversions_table.clustering_fields == ["conversion_type", "source"]

        # Check metrics table clustering
        metrics_table = mock_bq_client.create_table.call_args_list[1][0][0]
        assert metrics_table.clustering_fields == ["platform", "campaign_id"]


class TestDatasetProvisionerDatasetExists:
    """Test dataset_exists method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(project_id="test-project")

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_dataset_exists_returns_true(self, config, mock_bq_client):
        """Test dataset_exists returns True when dataset exists."""
        mock_bq_client.get_dataset.return_value = MagicMock()

        provisioner = DatasetProvisioner(config=config)
        result = provisioner.dataset_exists("test_customer")

        assert result is True
        mock_bq_client.get_dataset.assert_called_once_with(
            "test-project.growthnav_test_customer"
        )

    def test_dataset_exists_returns_false(self, config, mock_bq_client):
        """Test dataset_exists returns False when dataset doesn't exist."""
        from google.cloud.exceptions import NotFound

        mock_bq_client.get_dataset.side_effect = NotFound("Dataset not found")

        provisioner = DatasetProvisioner(config=config)
        result = provisioner.dataset_exists("test_customer")

        assert result is False


class TestDatasetProvisionerDeleteDataset:
    """Test delete_dataset method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(project_id="test-project")

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_delete_dataset_success(self, config, mock_bq_client):
        """Test successful dataset deletion."""
        provisioner = DatasetProvisioner(config=config)

        result = provisioner.delete_dataset("test_customer")

        assert result is True
        mock_bq_client.delete_dataset.assert_called_once_with(
            "test-project.growthnav_test_customer",
            delete_contents=True,
        )

    def test_delete_dataset_not_found(self, config, mock_bq_client):
        """Test delete_dataset returns False when dataset not found."""
        from google.cloud.exceptions import NotFound

        mock_bq_client.delete_dataset.side_effect = NotFound("Dataset not found")

        provisioner = DatasetProvisioner(config=config)
        result = provisioner.delete_dataset("test_customer")

        assert result is False


class TestDatasetProvisionerListTables:
    """Test list_tables method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ProvisioningConfig(project_id="test-project")

    @pytest.fixture
    def mock_bq_client(self):
        """Create mock BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_list_tables(self, config, mock_bq_client):
        """Test listing tables in a dataset."""
        mock_table1 = MagicMock()
        mock_table1.table_id = "conversions"
        mock_table2 = MagicMock()
        mock_table2.table_id = "daily_metrics"
        mock_bq_client.list_tables.return_value = [mock_table1, mock_table2]

        provisioner = DatasetProvisioner(config=config)
        tables = provisioner.list_tables("test_customer")

        assert tables == ["conversions", "daily_metrics"]
        mock_bq_client.list_tables.assert_called_once_with(
            "test-project.growthnav_test_customer"
        )
