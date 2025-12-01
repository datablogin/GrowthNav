"""Customer onboarding orchestrator.

Coordinates the complete onboarding workflow for new GrowthNav customers,
including dataset creation, registry registration, and credential storage.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

from growthnav.bigquery import Customer, CustomerRegistry, Industry

if TYPE_CHECKING:
    from growthnav.onboarding.provisioning import DatasetProvisioner
    from growthnav.onboarding.secrets import CredentialStore


logger = logging.getLogger(__name__)


class OnboardingStatus(str, Enum):
    """Status of customer onboarding process."""

    PENDING = "pending"
    VALIDATING = "validating"
    PROVISIONING = "provisioning"
    REGISTERING = "registering"
    STORING_CREDENTIALS = "storing_credentials"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class OnboardingRequest:
    """Request to onboard a new customer."""

    customer_id: str
    customer_name: str
    industry: Industry
    gcp_project_id: str | None = None
    google_ads_customer_ids: list[str] = field(default_factory=list)
    meta_ad_account_ids: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    credentials: dict[str, str] = field(default_factory=dict)


@dataclass
class OnboardingResult:
    """Result of customer onboarding process."""

    status: OnboardingStatus
    customer: Customer | None = None
    dataset_id: str | None = None
    errors: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def is_success(self) -> bool:
        """Return True if onboarding completed successfully."""
        return self.status == OnboardingStatus.COMPLETED

    @property
    def duration_seconds(self) -> float | None:
        """Return duration of onboarding in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class OnboardingOrchestrator:
    """Orchestrates the customer onboarding workflow.

    The onboarding process includes:
    1. Validating the onboarding request
    2. Creating a BigQuery dataset for the customer
    3. Registering the customer in the registry
    4. Storing any credentials in Secret Manager

    Example:
        >>> orchestrator = OnboardingOrchestrator()
        >>> request = OnboardingRequest(
        ...     customer_id="acme_corp",
        ...     customer_name="Acme Corporation",
        ...     industry=Industry.ECOMMERCE,
        ... )
        >>> result = orchestrator.onboard(request)
        >>> if result.is_success:
        ...     print(f"Customer onboarded: {result.dataset_id}")
    """

    def __init__(
        self,
        registry: CustomerRegistry | None = None,
        provisioner: DatasetProvisioner | None = None,
        credential_store: CredentialStore | None = None,
        default_project_id: str | None = None,
    ):
        """Initialize the orchestrator.

        Args:
            registry: Customer registry for storing customer records.
            provisioner: Dataset provisioner for creating BigQuery datasets.
            credential_store: Credential store for storing secrets.
            default_project_id: Default GCP project ID if not specified in request.
        """
        self._registry = registry
        self._provisioner = provisioner
        self._credential_store = credential_store
        self.default_project_id = default_project_id

    @property
    def registry(self) -> CustomerRegistry:
        """Lazy-initialize customer registry."""
        if self._registry is None:
            self._registry = CustomerRegistry()
        return self._registry

    @property
    def provisioner(self) -> DatasetProvisioner:
        """Lazy-initialize dataset provisioner."""
        if self._provisioner is None:
            from growthnav.onboarding.provisioning import DatasetProvisioner

            self._provisioner = DatasetProvisioner()
        return self._provisioner

    @property
    def credential_store(self) -> CredentialStore | None:
        """Return credential store (may be None if not configured)."""
        return self._credential_store

    def validate_request(self, request: OnboardingRequest) -> list[str]:
        """Validate an onboarding request.

        Args:
            request: The onboarding request to validate.

        Returns:
            List of validation error messages. Empty list if valid.
        """
        errors = []

        # Required fields
        if not request.customer_id:
            errors.append("customer_id is required")
        elif not request.customer_id.replace("_", "").replace("-", "").isalnum():
            errors.append(
                "customer_id must contain only alphanumeric characters, underscores, and hyphens"
            )

        if not request.customer_name:
            errors.append("customer_name is required")

        if not isinstance(request.industry, Industry):
            errors.append(f"industry must be an Industry enum, got {type(request.industry)}")

        # Validate Google Ads customer IDs format (XXX-XXX-XXXX)
        for gads_id in request.google_ads_customer_ids:
            if not self._is_valid_google_ads_id(gads_id):
                errors.append(f"Invalid Google Ads customer ID format: {gads_id}")

        # Validate Meta ad account IDs format (act_XXXXX)
        for meta_id in request.meta_ad_account_ids:
            if not meta_id.startswith("act_"):
                errors.append(f"Invalid Meta ad account ID format: {meta_id}")

        return errors

    def _is_valid_google_ads_id(self, gads_id: str) -> bool:
        """Check if a Google Ads customer ID is valid format."""
        import re

        return bool(re.match(r"^\d{3}-\d{3}-\d{4}$", gads_id))

    def onboard(self, request: OnboardingRequest) -> OnboardingResult:
        """Onboard a new customer.

        Performs the complete onboarding workflow:
        1. Validate request
        2. Check customer doesn't already exist
        3. Create BigQuery dataset
        4. Register customer
        5. Store credentials (if provided)

        Args:
            request: The onboarding request.

        Returns:
            OnboardingResult with status and details.
        """
        result = OnboardingResult(
            status=OnboardingStatus.PENDING,
            started_at=datetime.now(UTC),
        )

        try:
            # Step 1: Validate request
            result.status = OnboardingStatus.VALIDATING
            errors = self.validate_request(request)
            if errors:
                result.status = OnboardingStatus.FAILED
                result.errors = errors
                result.completed_at = datetime.now(UTC)
                return result

            # Step 2: Check customer doesn't already exist
            existing = self.registry.get_customer(request.customer_id)
            if existing:
                result.status = OnboardingStatus.FAILED
                result.errors = [f"Customer '{request.customer_id}' already exists"]
                result.completed_at = datetime.now(UTC)
                return result

            # Determine project ID
            project_id = request.gcp_project_id or self.default_project_id
            if not project_id:
                result.status = OnboardingStatus.FAILED
                result.errors = ["gcp_project_id is required (no default configured)"]
                result.completed_at = datetime.now(UTC)
                return result

            # Step 3: Create BigQuery dataset
            result.status = OnboardingStatus.PROVISIONING
            dataset_id = self.provisioner.create_dataset(request.customer_id)
            result.dataset_id = dataset_id
            logger.info(f"Created dataset: {dataset_id}")

            # Step 4: Register customer
            result.status = OnboardingStatus.REGISTERING
            customer = Customer(
                customer_id=request.customer_id,
                customer_name=request.customer_name,
                gcp_project_id=project_id,
                dataset=f"growthnav_{request.customer_id}",
                industry=request.industry,
                google_ads_customer_ids=request.google_ads_customer_ids,
                meta_ad_account_ids=request.meta_ad_account_ids,
                tags=request.tags,
            )
            self.registry.add_customer(customer)
            result.customer = customer
            logger.info(f"Registered customer: {request.customer_id}")

            # Step 5: Store credentials (if provided and store is configured)
            if request.credentials:
                if not self.credential_store:
                    logger.warning(
                        f"Skipping {len(request.credentials)} credentials for {request.customer_id}: "
                        "credential store not configured"
                    )
                else:
                    result.status = OnboardingStatus.STORING_CREDENTIALS
                    try:
                        for cred_type, cred_value in request.credentials.items():
                            self.credential_store.store_credential(
                                customer_id=request.customer_id,
                                credential_type=cred_type,
                                credential_value=cred_value,
                            )
                        logger.info(f"Stored {len(request.credentials)} credentials for {request.customer_id}")
                    except Exception as cred_error:
                        # Handle credential storage errors separately to avoid logging credentials
                        result.status = OnboardingStatus.FAILED
                        result.errors.append(f"Failed to store credentials: {type(cred_error).__name__}")
                        result.completed_at = datetime.now(UTC)
                        logger.exception(
                            "Credential storage failed",
                            extra={"customer_id": request.customer_id}
                        )
                        return result

            # Success
            result.status = OnboardingStatus.COMPLETED
            result.completed_at = datetime.now(UTC)
            logger.info(
                f"Onboarding completed for {request.customer_id} "
                f"in {result.duration_seconds:.2f}s"
            )
            return result

        except Exception as e:
            result.status = OnboardingStatus.FAILED
            # Sanitize error message to avoid logging credentials
            error_msg = str(e)
            if "credential" in error_msg.lower():
                error_msg = f"{type(e).__name__}: Credential-related error (details redacted)"
            result.errors.append(error_msg)
            result.completed_at = datetime.now(UTC)
            logger.exception(
                f"Onboarding failed for {request.customer_id}",
                extra={"sanitized_error": error_msg}
            )
            return result

    def offboard(self, customer_id: str, delete_data: bool = False) -> bool:
        """Offboard a customer (mark as inactive).

        Args:
            customer_id: The customer to offboard.
            delete_data: If True, also delete the customer's dataset (dangerous!).

        Returns:
            True if successful, False otherwise.
        """
        customer = self.registry.get_customer(customer_id)
        if not customer:
            logger.warning(f"Customer not found for offboarding: {customer_id}")
            return False

        # Update status to inactive
        from growthnav.bigquery import CustomerStatus

        self.registry.update_customer(customer_id, {"status": CustomerStatus.INACTIVE.value})
        logger.info(f"Customer {customer_id} marked as inactive")

        if delete_data:
            # Delete the dataset (this is destructive!)
            self.provisioner.delete_dataset(customer_id)
            logger.warning(f"Deleted dataset for {customer_id}")

        return True
