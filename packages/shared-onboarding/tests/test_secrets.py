"""Tests for CredentialStore."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core import exceptions as google_exceptions
from growthnav.onboarding import CredentialConfig, CredentialStore


class TestCredentialConfig:
    """Test CredentialConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = CredentialConfig(project_id="test-project")

        assert config.project_id == "test-project"
        assert config.secret_prefix == "growthnav"

    def test_custom_values(self):
        """Test custom configuration values."""
        config = CredentialConfig(
            project_id="custom-project",
            secret_prefix="myprefix",
        )

        assert config.project_id == "custom-project"
        assert config.secret_prefix == "myprefix"

    def test_from_env_with_gcp_project_id(self, monkeypatch):
        """Test loading config from GCP_PROJECT_ID env var."""
        monkeypatch.setenv("GCP_PROJECT_ID", "env-project")
        monkeypatch.delenv("GROWTNAV_PROJECT_ID", raising=False)

        config = CredentialConfig.from_env()

        assert config.project_id == "env-project"
        assert config.secret_prefix == "growthnav"

    def test_from_env_with_custom_prefix(self, monkeypatch):
        """Test loading prefix from env var."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GROWTNAV_SECRET_PREFIX", "custom")

        config = CredentialConfig.from_env()

        assert config.secret_prefix == "custom"

    def test_from_env_raises_without_project_id(self, monkeypatch):
        """Test from_env raises when no project ID is set."""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("GROWTNAV_PROJECT_ID", raising=False)

        with pytest.raises(ValueError, match="environment variable required"):
            CredentialConfig.from_env()


class TestCredentialStore:
    """Test CredentialStore class."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(
            project_id="test-project",
            secret_prefix="growthnav",
        )

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_config_lazy_initialization(self, monkeypatch):
        """Test config is lazily initialized from env."""
        monkeypatch.setenv("GCP_PROJECT_ID", "lazy-project")

        store = CredentialStore()

        # Access config triggers initialization
        config = store.config

        assert config.project_id == "lazy-project"

    def test_client_lazy_initialization(self, config, mock_sm_client):
        """Test Secret Manager client is lazily initialized."""
        store = CredentialStore(config=config)

        # Client not created yet
        assert store._client is None

        # Access client triggers initialization
        client = store.client

        assert client is mock_sm_client

    def test_get_secret_id(self, config):
        """Test secret ID generation."""
        store = CredentialStore(config=config)

        secret_id = store._get_secret_id("test_customer", "google_ads_token")

        assert secret_id == "growthnav-test_customer-google_ads_token"

    def test_get_parent(self, config):
        """Test parent resource path generation."""
        store = CredentialStore(config=config)

        parent = store._get_parent()

        assert parent == "projects/test-project"


class TestCredentialStoreStoreCredential:
    """Test store_credential method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(project_id="test-project")

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client

            # Mock successful version creation
            version_response = MagicMock()
            version_response.name = "projects/test-project/secrets/test-secret/versions/1"
            client.add_secret_version.return_value = version_response

            yield client

    def test_store_credential_creates_secret(self, config, mock_sm_client):
        """Test storing credential creates secret and adds version."""
        store = CredentialStore(config=config)

        result = store.store_credential(
            customer_id="test_customer",
            credential_type="refresh_token",
            credential_value="token123",
        )

        assert "versions/1" in result
        mock_sm_client.create_secret.assert_called_once()
        mock_sm_client.add_secret_version.assert_called_once()

    def test_store_credential_secret_already_exists(self, config, mock_sm_client):
        """Test storing when secret already exists."""
        # Secret already exists
        mock_sm_client.create_secret.side_effect = google_exceptions.AlreadyExists("Secret already exists")

        store = CredentialStore(config=config)

        # Should still succeed
        result = store.store_credential(
            customer_id="test_customer",
            credential_type="refresh_token",
            credential_value="token123",
        )

        assert result is not None
        mock_sm_client.add_secret_version.assert_called_once()

    def test_store_credential_validates_inputs(self, config, mock_sm_client):
        """Test store_credential validates required inputs."""
        store = CredentialStore(config=config)

        with pytest.raises(ValueError, match="required"):
            store.store_credential("", "type", "value")

        with pytest.raises(ValueError, match="required"):
            store.store_credential("customer", "", "value")

        with pytest.raises(ValueError, match="required"):
            store.store_credential("customer", "type", "")

    def test_store_credential_secret_has_labels(self, config, mock_sm_client):
        """Test stored secret has appropriate labels."""
        store = CredentialStore(config=config)

        store.store_credential(
            customer_id="test_customer",
            credential_type="refresh_token",
            credential_value="token123",
        )

        call_args = mock_sm_client.create_secret.call_args
        request = call_args[1]["request"]
        labels = request["secret"]["labels"]

        # Labels are sanitized: underscores replaced with hyphens for GCP compliance
        assert labels["customer_id"] == "test-customer"
        assert labels["credential_type"] == "refresh-token"
        assert labels["managed_by"] == "growthnav"


class TestCredentialStoreGetCredential:
    """Test get_credential method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(project_id="test-project")

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_get_credential_success(self, config, mock_sm_client):
        """Test successful credential retrieval."""
        response = MagicMock()
        response.payload.data = b"secret_value"
        mock_sm_client.access_secret_version.return_value = response

        store = CredentialStore(config=config)

        result = store.get_credential("test_customer", "refresh_token")

        assert result == "secret_value"
        mock_sm_client.access_secret_version.assert_called_once()

    def test_get_credential_not_found(self, config, mock_sm_client):
        """Test get_credential returns None when not found."""
        mock_sm_client.access_secret_version.side_effect = google_exceptions.NotFound("Secret not found")

        store = CredentialStore(config=config)

        result = store.get_credential("test_customer", "refresh_token")

        assert result is None

    def test_get_credential_specific_version(self, config, mock_sm_client):
        """Test getting specific version of credential."""
        response = MagicMock()
        response.payload.data = b"old_value"
        mock_sm_client.access_secret_version.return_value = response

        store = CredentialStore(config=config)

        store.get_credential("test_customer", "refresh_token", version="1")

        call_args = mock_sm_client.access_secret_version.call_args
        assert "/versions/1" in call_args[1]["request"]["name"]


class TestCredentialStoreDeleteCredential:
    """Test delete_credential method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(project_id="test-project")

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_delete_credential_success(self, config, mock_sm_client):
        """Test successful credential deletion."""
        store = CredentialStore(config=config)

        result = store.delete_credential("test_customer", "refresh_token")

        assert result is True
        mock_sm_client.delete_secret.assert_called_once()

    def test_delete_credential_not_found(self, config, mock_sm_client):
        """Test delete_credential returns False when not found."""
        mock_sm_client.delete_secret.side_effect = google_exceptions.NotFound("Secret not found")

        store = CredentialStore(config=config)

        result = store.delete_credential("test_customer", "refresh_token")

        assert result is False


class TestCredentialStoreListCredentials:
    """Test list_customer_credentials method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(project_id="test-project")

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_list_customer_credentials(self, config, mock_sm_client):
        """Test listing customer credentials."""
        secret1 = MagicMock()
        secret1.name = "projects/test-project/secrets/growthnav-test_customer-refresh_token"
        secret2 = MagicMock()
        secret2.name = "projects/test-project/secrets/growthnav-test_customer-access_token"
        secret3 = MagicMock()
        secret3.name = "projects/test-project/secrets/growthnav-other_customer-token"

        mock_sm_client.list_secrets.return_value = [secret1, secret2, secret3]

        store = CredentialStore(config=config)

        result = store.list_customer_credentials("test_customer")

        assert len(result) == 2
        assert "refresh_token" in result
        assert "access_token" in result

    def test_list_customer_credentials_empty(self, config, mock_sm_client):
        """Test listing when customer has no credentials."""
        mock_sm_client.list_secrets.return_value = []

        store = CredentialStore(config=config)

        result = store.list_customer_credentials("test_customer")

        assert result == []


class TestCredentialStoreCredentialExists:
    """Test credential_exists method."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return CredentialConfig(project_id="test-project")

    @pytest.fixture
    def mock_sm_client(self):
        """Create mock Secret Manager client."""
        with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_credential_exists_true(self, config, mock_sm_client):
        """Test credential_exists returns True when credential exists."""
        mock_sm_client.get_secret.return_value = MagicMock()

        store = CredentialStore(config=config)

        result = store.credential_exists("test_customer", "refresh_token")

        assert result is True

    def test_credential_exists_false(self, config, mock_sm_client):
        """Test credential_exists returns False when credential doesn't exist."""
        mock_sm_client.get_secret.side_effect = google_exceptions.NotFound("Secret not found")

        store = CredentialStore(config=config)

        result = store.credential_exists("test_customer", "refresh_token")

        assert result is False
