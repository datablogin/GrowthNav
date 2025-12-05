"""GrowthNav Customer Onboarding Package.

Provides customer onboarding orchestration including:
- OnboardingOrchestrator: Main workflow controller
- DatasetProvisioner: BigQuery dataset creation
- CredentialStore: Secret Manager integration
- DataSourceConfig: Data source connector configuration

Usage:
    from growthnav.onboarding import OnboardingOrchestrator, OnboardingRequest

    orchestrator = OnboardingOrchestrator()
    result = orchestrator.onboard(OnboardingRequest(...))
"""

from growthnav.onboarding.orchestrator import (
    DataSourceConfig,
    OnboardingOrchestrator,
    OnboardingRequest,
    OnboardingResult,
    OnboardingStatus,
)
from growthnav.onboarding.provisioning import (
    DatasetProvisioner,
    ProvisioningConfig,
)
from growthnav.onboarding.secrets import CredentialConfig, CredentialStore

__all__ = [
    "DataSourceConfig",
    "OnboardingOrchestrator",
    "OnboardingRequest",
    "OnboardingResult",
    "OnboardingStatus",
    "DatasetProvisioner",
    "ProvisioningConfig",
    "CredentialStore",
    "CredentialConfig",
]
