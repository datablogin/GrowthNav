"""
Shared test utilities and fixtures for shared-reporting integration tests.

This module provides common utilities for Google API integration tests,
including credential management and cleanup helpers.
"""

import json
import os
import time

from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Rate limit delay between tests (seconds)
# Google APIs have limits of 60 write requests per minute per user
RATE_LIMIT_DELAY = 5

# Delay between cleanup operations to avoid rate limits
# Using 1.0 second to stay safely within 60 requests/minute limit
CLEANUP_DELAY = 1.0

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


def get_cleanup_credentials(
    scopes: list[str] | None = None,
) -> Credentials | None:
    """
    Get credentials for test cleanup with domain-wide delegation.

    Args:
        scopes: OAuth scopes to request. Defaults to drive.file scope.

    Returns:
        Credentials object if successful, None otherwise.
    """
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/drive.file"]

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_path:
        try:
            with open(credentials_path) as f:
                cred_data = json.load(f)

            if cred_data.get("type") == "service_account":
                creds = Credentials.from_service_account_file(
                    credentials_path,
                    scopes=scopes,
                    subject=DEFAULT_IMPERSONATE_EMAIL,
                )
                return creds  # type: ignore[no-any-return]
        except (OSError, json.JSONDecodeError) as e:
            print(f"Warning: Could not load credentials from file: {e}")

    try:
        creds, _ = default(scopes=scopes)
        return creds  # type: ignore[no-any-return]
    except DefaultCredentialsError:
        return None


def cleanup_drive_files(file_ids: list[str]) -> None:
    """
    Clean up files from Google Drive with rate limiting.

    Args:
        file_ids: List of Google Drive file IDs to delete.
    """
    if not file_ids:
        return

    credentials = get_cleanup_credentials()
    if credentials is None:
        print("Warning: Could not get credentials for cleanup")
        return

    drive_service = build("drive", "v3", credentials=credentials)

    for file_id in file_ids:
        try:
            drive_service.files().delete(fileId=file_id).execute()
            print(f"Cleaned up file: {file_id}")
            if file_id != file_ids[-1]:
                time.sleep(CLEANUP_DELAY)
        except Exception as e:
            print(f"Failed to cleanup file {file_id}: {e}")
