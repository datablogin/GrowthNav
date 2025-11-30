"""Shared fixtures for onboarding package tests."""

from unittest.mock import MagicMock, patch

import pytest
from growthnav.bigquery import Industry


@pytest.fixture
def mock_bigquery_client():
    """Mock google.cloud.bigquery.Client for testing."""
    with patch("google.cloud.bigquery.Client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_secret_manager_client():
    """Mock Secret Manager client for testing."""
    with patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_customer_registry():
    """Mock CustomerRegistry for testing."""
    with patch("growthnav.onboarding.orchestrator.CustomerRegistry") as mock:
        registry = MagicMock()
        registry.get_customer.return_value = None  # Customer doesn't exist by default
        mock.return_value = registry
        yield registry


@pytest.fixture
def sample_onboarding_request():
    """Sample onboarding request for testing."""
    from growthnav.onboarding import OnboardingRequest

    return OnboardingRequest(
        customer_id="test_customer",
        customer_name="Test Customer Inc",
        industry=Industry.GOLF,
        gcp_project_id="test-project-123",
        google_ads_customer_ids=["123-456-7890"],
        meta_ad_account_ids=["act_12345"],
        tags=["test", "demo"],
    )
