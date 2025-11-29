"""Shared pytest fixtures for GrowthNav packages."""

import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_bigquery_client():
    """Mock google.cloud.bigquery.Client for testing."""
    with patch("google.cloud.bigquery.Client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_gspread_client():
    """Mock gspread client for testing."""
    with patch("gspread.authorize") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_slides_service():
    """Mock Google Slides API service for testing."""
    with patch("googleapiclient.discovery.build") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing."""
    return {
        "customer_id": "test_customer",
        "customer_name": "Test Customer Inc",
        "gcp_project_id": "test-project-123",
        "dataset": "growthnav_test_customer",
        "industry": "golf",
        "status": "active",
        "tags": ["enterprise", "demo"],
        "google_ads_customer_ids": ["123-456-7890"],
        "meta_ad_account_ids": ["act_12345"],
    }


@pytest.fixture
def sample_conversion_data():
    """Sample POS transaction data for testing."""
    return [
        {
            "order_id": "TXN-001",
            "total_amount": 150.00,
            "created_at": "2025-01-15T10:30:00Z",
            "customer_id": "CUST-001",
            "store_id": "STORE-A",
        },
        {
            "order_id": "TXN-002",
            "total_amount": 75.50,
            "created_at": "2025-01-15T11:45:00Z",
            "customer_id": "CUST-002",
            "store_id": "STORE-B",
        },
    ]
