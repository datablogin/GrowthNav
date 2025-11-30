"""Tests for package public API exports."""



class TestPackageExports:
    """Test that all expected classes are exported."""

    def test_orchestrator_exports(self):
        """Test orchestrator classes are exported."""
        from growthnav.onboarding import (
            OnboardingOrchestrator,
            OnboardingRequest,
            OnboardingResult,
            OnboardingStatus,
        )

        assert OnboardingOrchestrator is not None
        assert OnboardingRequest is not None
        assert OnboardingResult is not None
        assert OnboardingStatus is not None

    def test_provisioning_exports(self):
        """Test provisioning classes are exported."""
        from growthnav.onboarding import (
            DatasetProvisioner,
            ProvisioningConfig,
        )

        assert DatasetProvisioner is not None
        assert ProvisioningConfig is not None

    def test_secrets_exports(self):
        """Test secrets classes are exported."""
        from growthnav.onboarding import (
            CredentialConfig,
            CredentialStore,
        )

        assert CredentialStore is not None
        assert CredentialConfig is not None

    def test_all_exports_complete(self):
        """Test __all__ includes all expected exports."""
        from growthnav import onboarding

        expected_exports = [
            "OnboardingOrchestrator",
            "OnboardingRequest",
            "OnboardingResult",
            "OnboardingStatus",
            "DatasetProvisioner",
            "ProvisioningConfig",
            "CredentialStore",
            "CredentialConfig",
        ]

        for export in expected_exports:
            assert export in onboarding.__all__, f"{export} not in __all__"

    def test_submodule_imports(self):
        """Test direct submodule imports work."""
        from growthnav.onboarding.orchestrator import OnboardingOrchestrator
        from growthnav.onboarding.provisioning import DatasetProvisioner
        from growthnav.onboarding.secrets import CredentialStore

        assert OnboardingOrchestrator is not None
        assert DatasetProvisioner is not None
        assert CredentialStore is not None


class TestCrossPackageIntegration:
    """Test integration with other GrowthNav packages."""

    def test_imports_from_bigquery_package(self):
        """Test onboarding can import from bigquery package."""
        from growthnav.bigquery import Industry
        from growthnav.onboarding import OnboardingRequest

        # Create request with industry from bigquery package
        request = OnboardingRequest(
            customer_id="test",
            customer_name="Test",
            industry=Industry.GOLF,
        )

        assert request.industry == Industry.GOLF

    def test_customer_from_registry(self):
        """Test Customer type is compatible."""
        from growthnav.bigquery import Customer, Industry
        from growthnav.onboarding import OnboardingResult, OnboardingStatus

        customer = Customer(
            customer_id="test",
            customer_name="Test",
            gcp_project_id="project",
            dataset="dataset",
            industry=Industry.GOLF,
        )

        result = OnboardingResult(
            status=OnboardingStatus.COMPLETED,
            customer=customer,
        )

        assert result.customer.customer_id == "test"
