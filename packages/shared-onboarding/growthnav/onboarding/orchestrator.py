"""Customer onboarding orchestrator.

Coordinates the complete onboarding workflow for new GrowthNav customers,
including dataset creation, registry registration, and credential storage.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

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
    CONFIGURING_DATA_SOURCES = "configuring_data_sources"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DataSourceConfig:
    """Configuration for a customer data source.

    Used during onboarding to configure connectors to external data systems
    (Snowflake, Salesforce, HubSpot, etc.).

    Example:
        >>> config = DataSourceConfig(
        ...     connector_type="snowflake",
        ...     name="Toast POS via Snowflake",
        ...     connection_params={
        ...         "account": "acme.snowflakecomputing.com",
        ...         "warehouse": "ANALYTICS_WH",
        ...         "database": "TOAST_DATA",
        ...         "schema": "RAW",
        ...         "table": "TRANSACTIONS",
        ...     },
        ...     credentials_secret_path="growthnav-acme-connector-snowflake",
        ... )
    """

    connector_type: str  # "snowflake", "salesforce", "hubspot", etc.
    name: str
    connection_params: dict[str, Any] = field(default_factory=dict)
    credentials_secret_path: str | None = None  # Path to Secret Manager secret
    field_overrides: dict[str, str] = field(default_factory=dict)
    sync_schedule: str = "daily"  # "hourly", "daily", "weekly", "manual"


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
    data_sources: list[DataSourceConfig] = field(default_factory=list)


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
    5. Configuring data source connectors (if provided)

    Example:
        >>> orchestrator = OnboardingOrchestrator()
        >>> request = OnboardingRequest(
        ...     customer_id="acme_corp",
        ...     customer_name="Acme Corporation",
        ...     industry=Industry.ECOMMERCE,
        ...     data_sources=[
        ...         DataSourceConfig(
        ...             connector_type="snowflake",
        ...             name="Toast POS",
        ...             connection_params={"account": "acme.snowflakecomputing.com"},
        ...         )
        ...     ],
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
        connector_storage: Any | None = None,
        default_project_id: str | None = None,
    ):
        """Initialize the orchestrator.

        Args:
            registry: Customer registry for storing customer records.
            provisioner: Dataset provisioner for creating BigQuery datasets.
            credential_store: Credential store for storing secrets.
            connector_storage: Storage for connector configurations.
            default_project_id: Default GCP project ID if not specified in request.
        """
        self._registry = registry
        self._provisioner = provisioner
        self._credential_store = credential_store
        self._connector_storage = connector_storage
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

    @property
    def connector_storage(self) -> Any | None:
        """Return connector storage (may be None if not configured)."""
        return self._connector_storage

    def validate_request(self, request: OnboardingRequest) -> list[str]:
        """Validate an onboarding request.

        Args:
            request: The onboarding request to validate.

        Returns:
            List of validation error messages. Empty list if valid.
        """
        errors = []

        # Required fields - customer_id validation
        if not request.customer_id:
            errors.append("customer_id is required")
        elif not re.match(r"^[a-z][a-z0-9_]{2,31}$", request.customer_id):
            errors.append(
                "customer_id must: start with lowercase letter, "
                "contain only lowercase letters/numbers/underscores, "
                "be 3-32 characters long"
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
                        # Rollback: Mark customer as inactive since credential storage failed
                        if result.customer and self._registry:
                            try:
                                from growthnav.bigquery import CustomerStatus

                                logger.warning(
                                    f"Rolling back registry entry for {request.customer_id} due to credential failure"
                                )
                                self._registry.update_customer(
                                    request.customer_id, {"status": CustomerStatus.INACTIVE.value}
                                )
                                logger.info(
                                    f"Rollback successful: marked registry entry inactive for {request.customer_id}"
                                )
                            except Exception as reg_rollback_error:
                                logger.error(
                                    f"Registry rollback failed for {request.customer_id}: {reg_rollback_error}. "
                                    f"Manual cleanup may be required."
                                )
                        return result

            # Step 6: Configure data sources (if provided)
            if request.data_sources:
                if not self.connector_storage:
                    logger.warning(
                        f"Skipping {len(request.data_sources)} data sources for {request.customer_id}: "
                        "connector storage not configured"
                    )
                else:
                    result.status = OnboardingStatus.CONFIGURING_DATA_SOURCES
                    try:
                        configured_connectors = self._configure_data_sources(
                            request.customer_id,
                            request.data_sources,
                        )
                        logger.info(
                            f"Configured {len(configured_connectors)} data sources for {request.customer_id}"
                        )
                    except Exception as ds_error:
                        result.status = OnboardingStatus.FAILED
                        result.errors.append(f"Failed to configure data sources: {ds_error}")
                        result.completed_at = datetime.now(UTC)
                        logger.exception(
                            "Data source configuration failed",
                            extra={"customer_id": request.customer_id}
                        )
                        # Rollback: Mark customer as inactive
                        if result.customer and self._registry:
                            try:
                                from growthnav.bigquery import CustomerStatus

                                logger.warning(
                                    f"Rolling back registry entry for {request.customer_id} "
                                    "due to data source configuration failure"
                                )
                                self._registry.update_customer(
                                    request.customer_id, {"status": CustomerStatus.INACTIVE.value}
                                )
                            except Exception as reg_rollback_error:
                                rollback_msg = f"Registry rollback failed: {reg_rollback_error}"
                                logger.error(
                                    f"{rollback_msg} for {request.customer_id}. "
                                    f"Manual cleanup may be required."
                                )
                                result.errors.append(rollback_msg)
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

            # Rollback: Clean up created resources on failure
            # First, try to mark registry entry as inactive if it was created
            if result.customer and self._registry:
                try:
                    from growthnav.bigquery import CustomerStatus

                    logger.warning(f"Rolling back registry entry for {request.customer_id}")
                    self._registry.update_customer(
                        request.customer_id, {"status": CustomerStatus.INACTIVE.value}
                    )
                    logger.info(
                        f"Rollback successful: marked registry entry inactive for {request.customer_id}"
                    )
                except Exception as reg_rollback_error:
                    logger.error(
                        f"Registry rollback failed for {request.customer_id}: {reg_rollback_error}. "
                        f"Manual cleanup may be required."
                    )

            # Then, delete dataset if it was created
            if result.dataset_id and self.provisioner:
                try:
                    logger.warning(
                        f"Rolling back dataset creation for {request.customer_id}: {result.dataset_id}"
                    )
                    self.provisioner.delete_dataset(request.customer_id, delete_contents=True)
                    logger.info(f"Rollback successful: deleted dataset {result.dataset_id}")
                except Exception as rollback_error:
                    logger.error(
                        f"Rollback failed for {request.customer_id}: {rollback_error}. "
                        f"Manual cleanup may be required for dataset: {result.dataset_id}"
                    )

            return result

    def _configure_data_sources(
        self,
        customer_id: str,
        data_sources: list[DataSourceConfig],
    ) -> list[str]:
        """Configure data source connectors for a customer.

        Args:
            customer_id: The customer ID.
            data_sources: List of data source configurations.

        Returns:
            List of connector IDs that were configured.

        Raises:
            Exception: If connector storage fails.
        """
        from growthnav.connectors import (
            ConnectorConfig,
            ConnectorType,
            SyncMode,
            SyncSchedule,
        )

        connector_ids = []

        for ds_config in data_sources:
            # Convert DataSourceConfig to ConnectorConfig
            try:
                connector_type = ConnectorType(ds_config.connector_type)
            except ValueError:
                logger.warning(
                    f"Unknown connector type '{ds_config.connector_type}' for {customer_id}, skipping"
                )
                continue

            # Map sync schedule string to enum
            sync_schedule_map = {
                "hourly": SyncSchedule.HOURLY,
                "daily": SyncSchedule.DAILY,
                "weekly": SyncSchedule.WEEKLY,
                "manual": SyncSchedule.MANUAL,
            }
            sync_schedule = sync_schedule_map.get(ds_config.sync_schedule)
            if sync_schedule is None:
                logger.warning(
                    f"Unknown sync schedule '{ds_config.sync_schedule}' for {customer_id}, "
                    f"defaulting to 'daily'"
                )
                sync_schedule = SyncSchedule.DAILY

            connector_config = ConnectorConfig(
                connector_type=connector_type,
                customer_id=customer_id,
                name=ds_config.name,
                connection_params=ds_config.connection_params,
                credentials_secret_path=ds_config.credentials_secret_path,
                field_overrides=ds_config.field_overrides,
                sync_mode=SyncMode.INCREMENTAL,
                sync_schedule=sync_schedule,
            )

            # Save to storage (connector_storage is guaranteed non-None by caller)
            assert self.connector_storage is not None
            connector_id = self.connector_storage.save(connector_config)
            connector_ids.append(connector_id)
            logger.debug(f"Configured connector {connector_id} for {customer_id}")

        return connector_ids

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
