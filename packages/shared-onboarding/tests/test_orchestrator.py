"""Tests for OnboardingOrchestrator."""

from datetime import UTC
from unittest.mock import MagicMock, patch

import pytest
from growthnav.bigquery import Customer, CustomerRegistry, Industry
from growthnav.onboarding import (
    DataSourceConfig,
    OnboardingOrchestrator,
    OnboardingRequest,
    OnboardingResult,
    OnboardingStatus,
)


class TestOnboardingRequest:
    """Test OnboardingRequest dataclass."""

    def test_minimal_request(self):
        """Test creating request with minimal fields."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
        )

        assert request.customer_id == "test"
        assert request.customer_name == "Test"
        assert request.industry == Industry.GOLF
        assert request.gcp_project_id is None
        assert request.google_ads_customer_ids == []
        assert request.meta_ad_account_ids == []
        assert request.tags == []
        assert request.credentials == {}
        assert request.data_sources == []

    def test_full_request(self):
        """Test creating request with all fields."""
        request = OnboardingRequest(
            customer_id="topgolf",
            customer_name="Topgolf Entertainment",
            industry=Industry.GOLF,
            gcp_project_id="growthnav-prod",
            google_ads_customer_ids=["123-456-7890", "098-765-4321"],
            meta_ad_account_ids=["act_12345"],
            tags=["enterprise", "q1_2025"],
            credentials={"google_ads_refresh_token": "token123"},
        )

        assert len(request.google_ads_customer_ids) == 2
        assert len(request.meta_ad_account_ids) == 1
        assert len(request.credentials) == 1


class TestOnboardingResult:
    """Test OnboardingResult dataclass."""

    def test_is_success_true_when_completed(self):
        """Test is_success property."""
        result = OnboardingResult(status=OnboardingStatus.COMPLETED)
        assert result.is_success is True

    def test_is_success_false_when_failed(self):
        """Test is_success when failed."""
        result = OnboardingResult(status=OnboardingStatus.FAILED)
        assert result.is_success is False

    def test_is_success_false_when_pending(self):
        """Test is_success when pending."""
        result = OnboardingResult(status=OnboardingStatus.PENDING)
        assert result.is_success is False

    def test_duration_seconds(self):
        """Test duration calculation."""
        from datetime import datetime, timedelta

        start = datetime.now(UTC)
        end = start + timedelta(seconds=5.5)

        result = OnboardingResult(
            status=OnboardingStatus.COMPLETED,
            started_at=start,
            completed_at=end,
        )

        assert result.duration_seconds == pytest.approx(5.5, rel=0.01)

    def test_duration_seconds_none_when_not_complete(self):
        """Test duration is None when timestamps missing."""
        result = OnboardingResult(status=OnboardingStatus.PENDING)
        assert result.duration_seconds is None


class TestOnboardingStatus:
    """Test OnboardingStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        assert OnboardingStatus.PENDING.value == "pending"
        assert OnboardingStatus.VALIDATING.value == "validating"
        assert OnboardingStatus.PROVISIONING.value == "provisioning"
        assert OnboardingStatus.REGISTERING.value == "registering"
        assert OnboardingStatus.STORING_CREDENTIALS.value == "storing_credentials"
        assert OnboardingStatus.CONFIGURING_DATA_SOURCES.value == "configuring_data_sources"
        assert OnboardingStatus.COMPLETED.value == "completed"
        assert OnboardingStatus.FAILED.value == "failed"


class TestOnboardingOrchestratorValidation:
    """Test request validation."""

    def test_validate_valid_request(self, sample_onboarding_request):
        """Test validation passes for valid request."""
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(sample_onboarding_request)
        assert errors == []

    def test_validate_missing_customer_id(self):
        """Test validation fails for missing customer_id."""
        request = OnboardingRequest(
            customer_id="",
            customer_name="Test",
            industry=Industry.GOLF,
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert "customer_id is required" in errors

    def test_validate_missing_customer_name(self):
        """Test validation fails for missing customer_name."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="",
            industry=Industry.GOLF,
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert "customer_name is required" in errors

    def test_validate_invalid_customer_id_characters(self):
        """Test validation fails for invalid customer_id characters."""
        request = OnboardingRequest(
            customer_id="test@customer!",
            customer_name="Test",
            industry=Industry.GOLF,
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert any("start with lowercase letter" in e for e in errors)

    def test_validate_invalid_google_ads_id_format(self):
        """Test validation fails for invalid Google Ads ID."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            google_ads_customer_ids=["invalid_format"],
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert any("Google Ads customer ID" in e for e in errors)

    def test_validate_valid_google_ads_id_format(self):
        """Test validation passes for valid Google Ads ID."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            google_ads_customer_ids=["123-456-7890"],
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert errors == []

    def test_validate_invalid_meta_id_format(self):
        """Test validation fails for invalid Meta ID."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            meta_ad_account_ids=["12345"],  # Missing act_ prefix
        )
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert any("Meta ad account ID" in e for e in errors)

    def test_validate_invalid_industry_type(self):
        """Test validation fails for invalid industry type."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
        )
        # Manually replace industry with invalid type
        request.industry = "not_an_enum"
        orchestrator = OnboardingOrchestrator()
        errors = orchestrator.validate_request(request)
        assert any("industry must be an Industry enum" in e for e in errors)


class TestOnboardingOrchestratorOnboard:
    """Test onboard workflow."""

    @pytest.fixture
    def mock_provisioner(self):
        """Create mock provisioner."""
        provisioner = MagicMock()
        provisioner.create_dataset.return_value = "test-project.growthnav_test_customer"
        return provisioner

    @pytest.fixture
    def mock_registry(self):
        """Create mock registry."""
        registry = MagicMock(spec=CustomerRegistry)
        registry.get_customer.return_value = None  # Customer doesn't exist
        return registry

    def test_onboard_success(
        self, sample_onboarding_request, mock_provisioner, mock_registry
    ):
        """Test successful onboarding."""
        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(sample_onboarding_request)

        assert result.status == OnboardingStatus.COMPLETED
        assert result.is_success
        assert result.dataset_id == "test-project.growthnav_test_customer"
        assert result.customer is not None
        assert result.customer.customer_id == "test_customer"
        assert result.errors == []
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None

        mock_provisioner.create_dataset.assert_called_once_with("test_customer")
        mock_registry.add_customer.assert_called_once()

    def test_onboard_fails_validation(self, mock_provisioner, mock_registry):
        """Test onboarding fails on validation errors."""
        request = OnboardingRequest(
            customer_id="",  # Invalid
            customer_name="Test",
            industry=Industry.GOLF,
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        assert not result.is_success
        assert "customer_id is required" in result.errors
        mock_provisioner.create_dataset.assert_not_called()

    def test_onboard_fails_customer_exists(
        self, sample_onboarding_request, mock_provisioner, mock_registry
    ):
        """Test onboarding fails when customer already exists."""
        # Customer already exists
        mock_registry.get_customer.return_value = MagicMock(spec=Customer)

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(sample_onboarding_request)

        assert result.status == OnboardingStatus.FAILED
        assert "already exists" in result.errors[0]
        mock_provisioner.create_dataset.assert_not_called()

    def test_onboard_fails_no_project_id(self, mock_provisioner, mock_registry):
        """Test onboarding fails when no project ID available."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id=None,  # No project ID
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            default_project_id=None,  # No default either
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        assert "gcp_project_id is required" in result.errors[0]

    def test_onboard_uses_default_project_id(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding uses default project ID."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id=None,  # No project ID in request
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            default_project_id="default-project",  # Use default
        )

        result = orchestrator.onboard(request)

        assert result.is_success
        assert result.customer.gcp_project_id == "default-project"

    def test_onboard_stores_credentials(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding stores credentials when provided."""
        mock_credential_store = MagicMock()

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            credentials={
                "google_ads_refresh_token": "token123",
                "meta_access_token": "token456",
            },
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            credential_store=mock_credential_store,
        )

        result = orchestrator.onboard(request)

        assert result.is_success
        assert mock_credential_store.store_credential.call_count == 2

    def test_onboard_handles_provisioner_exception(
        self, sample_onboarding_request, mock_registry
    ):
        """Test onboarding handles provisioner exceptions."""
        mock_provisioner = MagicMock()
        mock_provisioner.create_dataset.side_effect = Exception("BigQuery error")

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(sample_onboarding_request)

        assert result.status == OnboardingStatus.FAILED
        assert "BigQuery error" in result.errors[0]

    def test_onboard_credentials_without_store_logs_warning(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding logs warning when credentials provided but no store configured."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            credentials={
                "google_ads_refresh_token": "token123",
            },
        )

        # No credential_store configured
        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            credential_store=None,
        )

        result = orchestrator.onboard(request)

        # Should still succeed, but credentials were skipped
        assert result.is_success
        # Credentials were not stored (no store to call)

    def test_onboard_handles_credential_store_exception(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding handles credential store exceptions."""
        mock_credential_store = MagicMock()
        mock_credential_store.store_credential.side_effect = Exception("Secret Manager error")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            credentials={
                "google_ads_refresh_token": "token123",
            },
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            credential_store=mock_credential_store,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Error message should mention credential failure but not expose details
        assert any("Failed to store credentials" in error for error in result.errors)

    def test_onboard_sanitizes_credential_errors(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding sanitizes credential-related error messages."""
        mock_provisioner.create_dataset.side_effect = Exception(
            "Error with credential token123 in request"
        )

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Error should be sanitized - not contain the original message with "credential"
        assert any("Credential-related error" in error for error in result.errors)
        # Should not contain the actual token
        assert not any("token123" in error for error in result.errors)

    def test_onboard_rollback_on_registry_failure(
        self, mock_provisioner, mock_registry
    ):
        """Test that dataset is rolled back when registry fails after creation."""
        # Dataset creation succeeds
        mock_provisioner.create_dataset.return_value = "test-project.growthnav_test"
        # Registry fails
        mock_registry.add_customer.side_effect = Exception("Registry unavailable")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        assert "Registry unavailable" in result.errors[0]
        # Dataset should have been rolled back
        mock_provisioner.delete_dataset.assert_called_once_with("test", delete_contents=True)

    def test_onboard_rollback_failure_logs_error(
        self, mock_provisioner, mock_registry
    ):
        """Test that rollback failure is logged but doesn't raise."""
        # Dataset creation succeeds
        mock_provisioner.create_dataset.return_value = "test-project.growthnav_test"
        # Registry fails
        mock_registry.add_customer.side_effect = Exception("Registry unavailable")
        # Rollback also fails
        mock_provisioner.delete_dataset.side_effect = Exception("Delete failed")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        # Should not raise even if rollback fails
        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        mock_provisioner.delete_dataset.assert_called_once()

    def test_onboard_rollback_registry_on_credential_failure(
        self, mock_provisioner, mock_registry
    ):
        """Test that registry entry is marked inactive when credential storage fails."""
        mock_credential_store = MagicMock()
        # Credential storage fails with an exception that triggers the outer except block
        mock_credential_store.store_credential.side_effect = RuntimeError("Connection lost")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            credentials={"token": "value"},
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            credential_store=mock_credential_store,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Registry should be updated to mark customer as inactive
        mock_registry.update_customer.assert_called()

    def test_onboard_registry_rollback_failure_logs_error(
        self, mock_provisioner, mock_registry
    ):
        """Test that registry rollback failure is logged but doesn't raise."""
        # Dataset and registry succeed, but then something fails in the outer try
        mock_provisioner.create_dataset.return_value = "test-project.growthnav_test"
        mock_registry.add_customer.return_value = None

        mock_credential_store = MagicMock()
        # Use a RuntimeError (not caught by inner try/except) to trigger outer except
        mock_credential_store.store_credential.side_effect = RuntimeError("Unexpected error")

        # Reset the update_customer mock from fixture and make it fail during rollback
        mock_registry.update_customer.reset_mock()
        mock_registry.update_customer.side_effect = Exception("Registry update failed")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            credentials={"token": "value"},
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            credential_store=mock_credential_store,
        )

        # Should not raise even if registry rollback fails
        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Registry update should have been attempted
        mock_registry.update_customer.assert_called_once()


    def test_onboard_outer_except_registry_rollback(
        self, mock_provisioner, mock_registry
    ):
        """Test outer except block registry rollback when unexpected exception after add_customer."""
        import growthnav.onboarding.orchestrator as orchestrator_module

        # Dataset creation succeeds
        mock_provisioner.create_dataset.return_value = "test-project.growthnav_test"
        mock_registry.add_customer.return_value = None

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            # No credentials - so we don't enter the credential storage block
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        # Mock logger.info to raise after add_customer succeeds but customer is set
        original_info = orchestrator_module.logger.info
        call_count = [0]

        def mock_info(msg, *args, **kwargs):
            call_count[0] += 1
            # Raise on the "Registered customer" log message (after result.customer is set)
            if "Registered customer" in msg:
                raise RuntimeError("Unexpected logging failure")
            return original_info(msg, *args, **kwargs)

        with patch.object(orchestrator_module.logger, "info", side_effect=mock_info):
            result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        assert "Unexpected logging failure" in result.errors[0]
        # Outer except block registry rollback should have been attempted
        mock_registry.update_customer.assert_called()

    def test_onboard_outer_except_registry_rollback_failure(
        self, mock_provisioner, mock_registry
    ):
        """Test outer except block continues when registry rollback fails."""
        import growthnav.onboarding.orchestrator as orchestrator_module

        # Dataset creation succeeds
        mock_provisioner.create_dataset.return_value = "test-project.growthnav_test"
        mock_registry.add_customer.return_value = None
        # Registry rollback will fail
        mock_registry.update_customer.side_effect = Exception("Registry update failed")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        # Mock logger.info to raise after add_customer
        original_info = orchestrator_module.logger.info

        def mock_info(msg, *args, **kwargs):
            if "Registered customer" in msg:
                raise RuntimeError("Unexpected failure")
            return original_info(msg, *args, **kwargs)

        with patch.object(orchestrator_module.logger, "info", side_effect=mock_info):
            result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Should have tried to rollback (even though it failed)
        mock_registry.update_customer.assert_called_once()


class TestOnboardingOrchestratorOffboard:
    """Test offboard workflow."""

    def test_offboard_marks_inactive(self):
        """Test offboarding marks customer as inactive."""
        mock_registry = MagicMock(spec=CustomerRegistry)
        mock_registry.get_customer.return_value = MagicMock(spec=Customer)
        mock_provisioner = MagicMock()

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.offboard("test_customer")

        assert result is True
        mock_registry.update_customer.assert_called_once()
        call_args = mock_registry.update_customer.call_args
        assert call_args[0][0] == "test_customer"
        assert call_args[0][1]["status"] == "inactive"
        mock_provisioner.delete_dataset.assert_not_called()

    def test_offboard_deletes_data_when_requested(self):
        """Test offboarding deletes dataset when requested."""
        mock_registry = MagicMock(spec=CustomerRegistry)
        mock_registry.get_customer.return_value = MagicMock(spec=Customer)
        mock_provisioner = MagicMock()

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
        )

        result = orchestrator.offboard("test_customer", delete_data=True)

        assert result is True
        mock_provisioner.delete_dataset.assert_called_once_with("test_customer")

    def test_offboard_returns_false_when_not_found(self):
        """Test offboarding returns False when customer not found."""
        mock_registry = MagicMock(spec=CustomerRegistry)
        mock_registry.get_customer.return_value = None

        orchestrator = OnboardingOrchestrator(registry=mock_registry)

        result = orchestrator.offboard("nonexistent")

        assert result is False


class TestOnboardingOrchestratorLazyInit:
    """Test lazy initialization of dependencies."""

    def test_registry_lazy_init(self):
        """Test registry is lazily initialized."""
        with patch("growthnav.onboarding.orchestrator.CustomerRegistry") as mock:
            orchestrator = OnboardingOrchestrator()

            # Registry not created yet
            mock.assert_not_called()

            # Access registry
            _ = orchestrator.registry

            # Now it should be created
            mock.assert_called_once()

    def test_provisioner_lazy_init(self):
        """Test provisioner is lazily initialized."""
        with patch("growthnav.onboarding.provisioning.DatasetProvisioner") as mock:
            orchestrator = OnboardingOrchestrator()

            # Provisioner not created yet
            mock.assert_not_called()

            # Access provisioner
            _ = orchestrator.provisioner

            # Now it should be created
            mock.assert_called_once()


class TestDataSourceConfig:
    """Test DataSourceConfig dataclass."""

    def test_minimal_config(self):
        """Test creating config with minimal fields."""
        config = DataSourceConfig(
            connector_type="snowflake",
            name="My Snowflake",
        )

        assert config.connector_type == "snowflake"
        assert config.name == "My Snowflake"
        assert config.connection_params == {}
        assert config.credentials_secret_path is None
        assert config.field_overrides == {}
        assert config.sync_schedule == "daily"

    def test_full_config(self):
        """Test creating config with all fields."""
        config = DataSourceConfig(
            connector_type="snowflake",
            name="Toast POS via Snowflake",
            connection_params={
                "account": "acme.snowflakecomputing.com",
                "warehouse": "ANALYTICS_WH",
                "database": "TOAST_DATA",
                "schema": "RAW",
            },
            credentials_secret_path="growthnav-acme-connector-snowflake",
            field_overrides={
                "SALE_ID": "transaction_id",
                "SALE_AMOUNT": "value",
            },
            sync_schedule="hourly",
        )

        assert config.connector_type == "snowflake"
        assert config.name == "Toast POS via Snowflake"
        assert config.connection_params["account"] == "acme.snowflakecomputing.com"
        assert config.credentials_secret_path == "growthnav-acme-connector-snowflake"
        assert len(config.field_overrides) == 2
        assert config.sync_schedule == "hourly"


class TestOnboardingOrchestratorDataSources:
    """Test data source configuration during onboarding."""

    @pytest.fixture
    def mock_provisioner(self):
        """Create mock provisioner."""
        provisioner = MagicMock()
        provisioner.create_dataset.return_value = "test-project.growthnav_test_customer"
        return provisioner

    @pytest.fixture
    def mock_registry(self):
        """Create mock registry."""
        registry = MagicMock(spec=CustomerRegistry)
        registry.get_customer.return_value = None  # Customer doesn't exist
        return registry

    @pytest.fixture
    def mock_connector_storage(self):
        """Create mock connector storage."""
        storage = MagicMock()
        storage.save.return_value = "connector-uuid-123"
        return storage

    def test_onboard_with_data_sources(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test onboarding with data sources configured."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                    connection_params={"account": "test.snowflakecomputing.com"},
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.is_success
        mock_connector_storage.save.assert_called_once()

    def test_onboard_with_multiple_data_sources(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test onboarding with multiple data sources."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                    connection_params={"account": "test.snowflakecomputing.com"},
                ),
                DataSourceConfig(
                    connector_type="salesforce",
                    name="Salesforce CRM",
                    connection_params={"domain": "login"},
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.is_success
        assert mock_connector_storage.save.call_count == 2

    def test_onboard_data_sources_without_storage_logs_warning(
        self, mock_provisioner, mock_registry
    ):
        """Test onboarding logs warning when data sources provided but no storage configured."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                ),
            ],
        )

        # No connector_storage configured
        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=None,
        )

        result = orchestrator.onboard(request)

        # Should still succeed, but data sources were skipped
        assert result.is_success

    def test_onboard_handles_data_source_exception(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test onboarding handles data source configuration exceptions."""
        mock_connector_storage.save.side_effect = Exception("Storage error")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        assert any("Failed to configure data sources" in error for error in result.errors)

    def test_onboard_skips_invalid_connector_type(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test onboarding skips data sources with unknown connector types."""
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="unknown_connector",
                    name="Unknown",
                ),
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Valid Snowflake",
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.is_success
        # Only the valid snowflake connector should be saved
        mock_connector_storage.save.assert_called_once()

    def test_onboard_data_sources_rollback_on_failure(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test that registry entry is marked inactive when data source config fails."""
        mock_connector_storage.save.side_effect = Exception("Storage error")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Registry should be updated to mark customer as inactive
        mock_registry.update_customer.assert_called()

    def test_onboard_data_sources_rollback_failure_adds_error(
        self, mock_provisioner, mock_registry, mock_connector_storage
    ):
        """Test that rollback failure is added to errors when both data source and rollback fail."""
        mock_connector_storage.save.side_effect = Exception("Storage error")
        mock_registry.update_customer.side_effect = Exception("Registry update failed")

        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
            gcp_project_id="test-project",
            data_sources=[
                DataSourceConfig(
                    connector_type="snowflake",
                    name="Toast POS",
                ),
            ],
        )

        orchestrator = OnboardingOrchestrator(
            registry=mock_registry,
            provisioner=mock_provisioner,
            connector_storage=mock_connector_storage,
        )

        result = orchestrator.onboard(request)

        assert result.status == OnboardingStatus.FAILED
        # Should have both the original error and the rollback failure error
        assert len(result.errors) == 2
        assert any("Storage error" in e for e in result.errors)
        assert any("Registry rollback failed" in e for e in result.errors)

    def test_connector_storage_property(self):
        """Test connector_storage property returns configured storage."""
        mock_storage = MagicMock()
        orchestrator = OnboardingOrchestrator(connector_storage=mock_storage)

        assert orchestrator.connector_storage is mock_storage

    def test_connector_storage_property_none(self):
        """Test connector_storage property returns None when not configured."""
        orchestrator = OnboardingOrchestrator()

        assert orchestrator.connector_storage is None
