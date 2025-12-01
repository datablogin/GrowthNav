"""Secret Manager integration for credential storage.

Provides secure storage and retrieval of customer credentials
using Google Cloud Secret Manager.
"""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass

from google.api_core import exceptions
from google.cloud import secretmanager


@dataclass
class CredentialConfig:
    """Configuration for credential storage.

    Attributes:
        project_id: GCP project ID for Secret Manager.
        secret_prefix: Prefix for secret names. Defaults to "growthnav".
    """

    project_id: str
    secret_prefix: str = "growthnav"

    @classmethod
    def from_env(cls) -> CredentialConfig:
        """Create configuration from environment variables.

        Uses GCP_PROJECT_ID or GROWTNAV_PROJECT_ID for project.
        Uses GROWTNAV_SECRET_PREFIX for prefix (optional).

        Returns:
            CredentialConfig instance.

        Raises:
            ValueError: If no project ID is set.
        """
        project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("GROWTNAV_PROJECT_ID")
        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID or GROWTNAV_PROJECT_ID environment variable required"
            )

        secret_prefix = os.getenv("GROWTNAV_SECRET_PREFIX", "growthnav")

        return cls(project_id=project_id, secret_prefix=secret_prefix)


class CredentialStore:
    """Stores and retrieves customer credentials in Secret Manager.

    Credentials are stored with names following the pattern:
    `{prefix}-{customer_id}-{credential_type}`

    Example:
        >>> config = CredentialConfig(project_id="my-project")
        >>> store = CredentialStore(config=config)
        >>> version_name = store.store_credential(
        ...     customer_id="acme_corp",
        ...     credential_type="google_ads_refresh_token",
        ...     credential_value="1//0x..."
        ... )
        >>> token = store.get_credential("acme_corp", "google_ads_refresh_token")
        >>> types = store.list_customer_credentials("acme_corp")
        >>> store.delete_credential("acme_corp", "google_ads_refresh_token")
    """

    def __init__(self, config: CredentialConfig | None = None):
        """Initialize the credential store.

        Args:
            config: Configuration for the store. If None, will be lazily
                   initialized from environment variables when accessed.
        """
        self._config = config
        self._client: secretmanager.SecretManagerServiceClient | None = None

    @property
    def config(self) -> CredentialConfig:
        """Get configuration, lazily initializing from environment if needed.

        Returns:
            CredentialConfig instance.

        Raises:
            ValueError: If no project ID is configured.
        """
        if self._config is None:
            self._config = CredentialConfig.from_env()
        return self._config

    @property
    def client(self) -> secretmanager.SecretManagerServiceClient:
        """Lazy-initialize Secret Manager client.

        Returns:
            Initialized SecretManagerServiceClient instance.
        """
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def _get_secret_id(self, customer_id: str, credential_type: str) -> str:
        """Get the secret ID for a credential.

        Args:
            customer_id: The customer identifier.
            credential_type: Type of credential.

        Returns:
            Secret ID in the format {prefix}-{customer_id}-{credential_type}.
        """
        return f"{self.config.secret_prefix}-{customer_id}-{credential_type}"

    def _get_parent(self) -> str:
        """Get the parent resource path for secrets.

        Returns:
            Parent path in the format projects/{project_id}.
        """
        return f"projects/{self.config.project_id}"

    def store_credential(
        self,
        customer_id: str,
        credential_type: str,
        credential_value: str,
    ) -> str:
        """Store a credential in Secret Manager.

        Creates the secret if it doesn't exist, then adds a new version.
        If the secret already exists, just adds a new version.

        Args:
            customer_id: The customer identifier.
            credential_type: Type of credential (e.g., "google_ads_refresh_token").
            credential_value: The credential value to store.

        Returns:
            The secret version name (e.g., "projects/.../secrets/.../versions/1").

        Raises:
            ValueError: If any argument is empty.
            Exception: If Secret Manager API call fails (other than AlreadyExists).
        """
        if not customer_id or not credential_type or not credential_value:
            raise ValueError("customer_id, credential_type, and credential_value are required")

        secret_id = self._get_secret_id(customer_id, credential_type)
        parent = self._get_parent()

        # Create secret if it doesn't exist
        # If secret already exists, suppress the error and continue to add new version
        with contextlib.suppress(exceptions.AlreadyExists):
            self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": {
                        "replication": {"automatic": {}},
                        "labels": {
                            "customer_id": customer_id.replace("_", "-")[:63],
                            "credential_type": credential_type.replace("_", "-")[:63],
                            "managed_by": "growthnav",
                        },
                    },
                }
            )

        # Add new version
        response = self.client.add_secret_version(
            request={
                "parent": f"{parent}/secrets/{secret_id}",
                "payload": {"data": credential_value.encode("utf-8")},
            }
        )

        return response.name

    def get_credential(
        self,
        customer_id: str,
        credential_type: str,
        version: str | None = None,
    ) -> str | None:
        """Retrieve a credential from Secret Manager.

        Retrieves the specified version of the secret, or latest if not specified.

        Args:
            customer_id: The customer identifier.
            credential_type: Type of credential to retrieve.
            version: Optional version number. Defaults to "latest".

        Returns:
            The credential value, or None if the secret doesn't exist.

        Raises:
            Exception: If Secret Manager API call fails (other than NotFound).
        """
        secret_id = self._get_secret_id(customer_id, credential_type)
        version_str = version if version else "latest"
        name = f"{self._get_parent()}/secrets/{secret_id}/versions/{version_str}"

        try:
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("utf-8")
        except exceptions.NotFound:
            return None

    def delete_credential(
        self,
        customer_id: str,
        credential_type: str,
    ) -> bool:
        """Delete a credential from Secret Manager.

        Deletes the secret entirely, including all versions.

        Args:
            customer_id: The customer identifier.
            credential_type: Type of credential to delete.

        Returns:
            True if deleted, False if the secret didn't exist.

        Raises:
            Exception: If Secret Manager API call fails (other than NotFound).
        """
        secret_id = self._get_secret_id(customer_id, credential_type)
        secret_name = f"{self._get_parent()}/secrets/{secret_id}"

        try:
            self.client.delete_secret(request={"name": secret_name})
            return True
        except exceptions.NotFound:
            return False

    def list_customer_credentials(self, customer_id: str) -> list[str]:
        """List all credential types for a customer.

        Filters secrets by the prefix {prefix}-{customer_id}- and extracts
        the credential_type portion from each matching secret name.

        Args:
            customer_id: The customer identifier.

        Returns:
            List of credential types stored for the customer.

        Raises:
            Exception: If Secret Manager API call fails.
        """
        parent = self._get_parent()
        prefix = f"{self.config.secret_prefix}-{customer_id}-"

        credential_types = []

        # List all secrets and filter by prefix
        for secret in self.client.list_secrets(request={"parent": parent}):
            secret_name = secret.name.split("/")[-1]
            if secret_name.startswith(prefix):
                # Extract credential type from secret name
                credential_type = secret_name[len(prefix) :]
                credential_types.append(credential_type)

        return credential_types

    def credential_exists(self, customer_id: str, credential_type: str) -> bool:
        """Check if a credential exists.

        Args:
            customer_id: The customer identifier.
            credential_type: Type of credential to check.

        Returns:
            True if the credential exists, False otherwise.
        """
        secret_id = self._get_secret_id(customer_id, credential_type)
        secret_name = f"{self._get_parent()}/secrets/{secret_id}"

        try:
            self.client.get_secret(request={"name": secret_name})
            return True
        except exceptions.NotFound:
            return False

    # Alias for backwards compatibility
    list_credentials = list_customer_credentials
