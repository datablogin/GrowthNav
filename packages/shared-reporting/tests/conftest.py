"""Shared test fixtures and utilities for shared-reporting tests."""

import json
import os
import time
from collections.abc import Generator

import pytest
from google.auth import default
from google.oauth2.service_account import Credentials

# Environment variable for test sharing email
TEST_SHARE_EMAIL = os.getenv(
    "GROWTHNAV_TEST_SHARE_EMAIL",
    "growthnav-ci@topgolf-460202.iam.gserviceaccount.com",
)

# Default impersonation email for domain-wide delegation
DEFAULT_IMPERSONATE_EMAIL = os.getenv(
    "GROWTHNAV_IMPERSONATE_EMAIL",
    "access@roimediapartners.com",
)

# Delay between cleanup operations to avoid rate limits
CLEANUP_DELAY = 0.5


def get_cleanup_credentials(
    scopes: list[str] | None = None,
    impersonate_email: str | None = None,
) -> Credentials | None:
    """Get credentials for test cleanup with domain-wide delegation.

    Args:
        scopes: OAuth scopes for the credentials. Defaults to drive.file scope.
        impersonate_email: Email to impersonate for domain-wide delegation.
            Defaults to GROWTHNAV_IMPERSONATE_EMAIL env var.

    Returns:
        Credentials object if successful, None if credentials unavailable.
    """
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/drive.file"]

    if impersonate_email is None:
        impersonate_email = DEFAULT_IMPERSONATE_EMAIL

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_path:
        try:
            with open(credentials_path) as f:
                cred_data = json.load(f)

            if cred_data.get("type") == "service_account":
                # Use service account credentials WITH domain-wide delegation
                # This ensures we can delete files created by the impersonated user
                return Credentials.from_service_account_file(
                    credentials_path,
                    scopes=scopes,
                    subject=impersonate_email,
                )
        except (OSError, json.JSONDecodeError):
            pass

    # Fallback to Application Default Credentials
    try:
        creds, _ = default(scopes=scopes)
        return creds
    except Exception:
        return None


def cleanup_drive_files(
    file_ids: list[str],
    credentials: Credentials | None = None,
    delay: float = CLEANUP_DELAY,
) -> None:
    """Clean up files from Google Drive with rate limiting.

    Args:
        file_ids: List of file IDs to delete.
        credentials: Credentials to use. If None, will attempt to get cleanup credentials.
        delay: Delay between deletions to avoid rate limits.
    """
    if not file_ids:
        return

    if credentials is None:
        credentials = get_cleanup_credentials()

    if credentials is None:
        print("Warning: Could not get credentials for cleanup")
        return

    from googleapiclient.discovery import build

    drive_service = build("drive", "v3", credentials=credentials)

    for file_id in file_ids:
        try:
            drive_service.files().delete(fileId=file_id).execute()
            print(f"Cleaned up file: {file_id}")
            if delay > 0 and file_id != file_ids[-1]:  # Don't delay after last file
                time.sleep(delay)
        except Exception as e:
            print(f"Failed to cleanup file {file_id}: {e}")


@pytest.fixture
def test_share_email() -> str:
    """Get the email address to use for testing sharing functionality."""
    return TEST_SHARE_EMAIL


@pytest.fixture
def cleanup_credentials() -> Credentials | None:
    """Get credentials for test cleanup."""
    return get_cleanup_credentials()


@pytest.fixture
def drive_file_ids() -> Generator[list[str], None, None]:
    """Track Drive file IDs for cleanup after tests.

    Usage:
        def test_something(drive_file_ids):
            # Create a file
            file_id = create_some_file()
            drive_file_ids.append(file_id)
            # Test assertions...
            # File will be automatically deleted after test
    """
    file_ids: list[str] = []
    yield file_ids
    cleanup_drive_files(file_ids)
