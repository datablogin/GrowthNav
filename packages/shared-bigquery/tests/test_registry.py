"""Tests for CustomerRegistry."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from growthnav.bigquery.registry import (
    Customer,
    CustomerRegistry,
    CustomerStatus,
    Industry,
)


class TestIndustry:
    """Test suite for Industry enum."""

    def test_industry_values(self):
        """Test Industry enum values."""
        assert Industry.GOLF == "golf"
        assert Industry.MEDICAL == "medical"
        assert Industry.RESTAURANT == "restaurant"
        assert Industry.RETAIL == "retail"
        assert Industry.ECOMMERCE == "ecommerce"
        assert Industry.HOSPITALITY == "hospitality"
        assert Industry.FITNESS == "fitness"
        assert Industry.ENTERTAINMENT == "entertainment"
        assert Industry.OTHER == "other"

    def test_industry_is_string_enum(self):
        """Test Industry is a string enum."""
        assert isinstance(Industry.GOLF, str)
        assert Industry.GOLF.value == "golf"


class TestCustomerStatus:
    """Test suite for CustomerStatus enum."""

    def test_customer_status_values(self):
        """Test CustomerStatus enum values."""
        assert CustomerStatus.ACTIVE == "active"
        assert CustomerStatus.INACTIVE == "inactive"
        assert CustomerStatus.ONBOARDING == "onboarding"
        assert CustomerStatus.SUSPENDED == "suspended"

    def test_customer_status_is_string_enum(self):
        """Test CustomerStatus is a string enum."""
        assert isinstance(CustomerStatus.ACTIVE, str)
        assert CustomerStatus.ACTIVE.value == "active"


class TestCustomer:
    """Test suite for Customer dataclass."""

    def test_customer_creation(self):
        """Test Customer dataclass creation."""
        customer = Customer(
            customer_id="topgolf",
            customer_name="TopGolf",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
        )

        assert customer.customer_id == "topgolf"
        assert customer.customer_name == "TopGolf"
        assert customer.gcp_project_id == "growthnav-prod"
        assert customer.dataset == "growthnav_topgolf"
        assert customer.industry == Industry.GOLF
        assert customer.status == CustomerStatus.ACTIVE  # Default value

    def test_customer_with_all_fields(self):
        """Test Customer with all fields populated."""
        now = datetime.utcnow()
        customer = Customer(
            customer_id="topgolf",
            customer_name="TopGolf",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
            status=CustomerStatus.ONBOARDING,
            tags=["premium", "enterprise"],
            google_ads_customer_ids=["123-456-7890"],
            meta_ad_account_ids=["act_987654"],
            google_ads_token_secret="projects/growthnav/secrets/topgolf-google-ads",
            meta_access_token_secret="projects/growthnav/secrets/topgolf-meta",
            onboarded_at=now,
            updated_at=now,
        )

        assert customer.customer_id == "topgolf"
        assert customer.status == CustomerStatus.ONBOARDING
        assert customer.tags == ["premium", "enterprise"]
        assert customer.google_ads_customer_ids == ["123-456-7890"]
        assert customer.meta_ad_account_ids == ["act_987654"]
        assert customer.google_ads_token_secret == "projects/growthnav/secrets/topgolf-google-ads"
        assert customer.meta_access_token_secret == "projects/growthnav/secrets/topgolf-meta"
        assert customer.onboarded_at == now
        assert customer.updated_at == now

    def test_customer_default_values(self):
        """Test Customer default field values."""
        customer = Customer(
            customer_id="test",
            customer_name="Test Customer",
            gcp_project_id="test-project",
            dataset="growthnav_test",
            industry=Industry.OTHER,
        )

        assert customer.status == CustomerStatus.ACTIVE
        assert customer.tags == []
        assert customer.google_ads_customer_ids == []
        assert customer.meta_ad_account_ids == []
        assert customer.google_ads_token_secret is None
        assert customer.meta_access_token_secret is None
        assert customer.onboarded_at is None
        assert customer.updated_at is None

    def test_full_dataset_id_property(self):
        """Test Customer.full_dataset_id property."""
        customer = Customer(
            customer_id="topgolf",
            customer_name="TopGolf",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
        )

        assert customer.full_dataset_id == "growthnav-prod.growthnav_topgolf"


class TestCustomerRegistry:
    """Test suite for CustomerRegistry."""

    def test_registry_constants(self):
        """Test CustomerRegistry class constants."""
        assert CustomerRegistry.REGISTRY_DATASET == "growthnav_registry"
        assert CustomerRegistry.REGISTRY_TABLE == "customers"

    def test_initialization_default(self, monkeypatch):
        """Test CustomerRegistry initialization with defaults."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        registry = CustomerRegistry()

        assert registry.registry_project_id == "test-project"
        assert registry.cache_ttl == 3600

    def test_initialization_with_env_var(self, monkeypatch):
        """Test CustomerRegistry initialization with GROWTNAV_REGISTRY_PROJECT_ID."""
        monkeypatch.setenv("GROWTNAV_REGISTRY_PROJECT_ID", "registry-project")
        registry = CustomerRegistry()

        assert registry.registry_project_id == "registry-project"

    def test_initialization_custom(self):
        """Test CustomerRegistry initialization with custom values."""
        registry = CustomerRegistry(
            registry_project_id="custom-project",
            cache_ttl=7200,
        )

        assert registry.registry_project_id == "custom-project"
        assert registry.cache_ttl == 7200

    def test_table_ref_format(self):
        """Test CustomerRegistry.table_ref format."""
        registry = CustomerRegistry(registry_project_id="test-project")
        expected = "test-project.growthnav_registry.customers"
        assert registry.table_ref == expected

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_client_lazy_initialization(self, mock_bq_client):
        """Test BigQuery client is lazily initialized."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Client should not be initialized yet
        assert registry._client is None

        # Access client property
        _ = registry.client

        # Now it should be initialized
        mock_bq_client.assert_called_once_with(project="test-project")

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_get_customer_not_found(self, mock_bq_client):
        """Test get_customer returns None when not found."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock empty result
        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([])
        mock_bq_client.return_value.query.return_value.result.return_value = mock_result

        customer = registry.get_customer("nonexistent")
        assert customer is None

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_get_customer_found(self, mock_bq_client):
        """Test get_customer returns Customer when found."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock customer row
        mock_row = Mock()
        mock_row.items.return_value = [
            ("customer_id", "topgolf"),
            ("customer_name", "TopGolf"),
            ("gcp_project_id", "growthnav-prod"),
            ("dataset", "growthnav_topgolf"),
            ("industry", "golf"),
            ("status", "active"),
            ("tags", ["premium"]),
            ("google_ads_customer_ids", ["123-456-7890"]),
            ("meta_ad_account_ids", ["act_987654"]),
            ("google_ads_token_secret", "secret1"),
            ("meta_access_token_secret", "secret2"),
            ("onboarded_at", None),
            ("updated_at", None),
        ]

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([mock_row])
        mock_bq_client.return_value.query.return_value.result.return_value = mock_result

        customer = registry.get_customer("topgolf")

        assert customer is not None
        assert customer.customer_id == "topgolf"
        assert customer.customer_name == "TopGolf"
        assert customer.gcp_project_id == "growthnav-prod"
        assert customer.dataset == "growthnav_topgolf"
        assert customer.industry == Industry.GOLF
        assert customer.status == CustomerStatus.ACTIVE
        assert customer.tags == ["premium"]
        assert customer.google_ads_customer_ids == ["123-456-7890"]

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_get_customers_by_industry(self, mock_bq_client):
        """Test get_customers_by_industry returns list of customers."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock two customer rows
        mock_row1 = Mock()
        mock_row1.items.return_value = [
            ("customer_id", "topgolf"),
            ("customer_name", "TopGolf"),
            ("gcp_project_id", "growthnav-prod"),
            ("dataset", "growthnav_topgolf"),
            ("industry", "golf"),
            ("status", "active"),
            ("tags", []),
            ("google_ads_customer_ids", []),
            ("meta_ad_account_ids", []),
            ("google_ads_token_secret", None),
            ("meta_access_token_secret", None),
            ("onboarded_at", None),
            ("updated_at", None),
        ]

        mock_row2 = Mock()
        mock_row2.items.return_value = [
            ("customer_id", "pebblebeach"),
            ("customer_name", "Pebble Beach"),
            ("gcp_project_id", "growthnav-prod"),
            ("dataset", "growthnav_pebblebeach"),
            ("industry", "golf"),
            ("status", "active"),
            ("tags", []),
            ("google_ads_customer_ids", []),
            ("meta_ad_account_ids", []),
            ("google_ads_token_secret", None),
            ("meta_access_token_secret", None),
            ("onboarded_at", None),
            ("updated_at", None),
        ]

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([mock_row1, mock_row2])
        mock_bq_client.return_value.query.return_value.result.return_value = mock_result

        customers = registry.get_customers_by_industry(Industry.GOLF)

        assert len(customers) == 2
        assert customers[0].customer_id == "topgolf"
        assert customers[1].customer_id == "pebblebeach"
        assert all(c.industry == Industry.GOLF for c in customers)

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_get_customers_by_industry_empty(self, mock_bq_client):
        """Test get_customers_by_industry with no results."""
        registry = CustomerRegistry(registry_project_id="test-project")

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([])
        mock_bq_client.return_value.query.return_value.result.return_value = mock_result

        customers = registry.get_customers_by_industry(Industry.ENTERTAINMENT)
        assert customers == []

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_add_customer(self, mock_bq_client):
        """Test add_customer inserts and returns customer with timestamps."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock successful insert
        mock_bq_client.return_value.insert_rows_json.return_value = []

        customer = Customer(
            customer_id="newcustomer",
            customer_name="New Customer",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_newcustomer",
            industry=Industry.RETAIL,
        )

        result = registry.add_customer(customer)

        assert result.customer_id == "newcustomer"
        assert result.onboarded_at is not None
        assert result.updated_at is not None
        assert isinstance(result.onboarded_at, datetime)
        assert isinstance(result.updated_at, datetime)

        # Verify insert was called
        mock_bq_client.return_value.insert_rows_json.assert_called_once()

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_add_customer_error(self, mock_bq_client):
        """Test add_customer raises error on insert failure."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock insert error
        mock_bq_client.return_value.insert_rows_json.return_value = [
            {"index": 0, "errors": [{"reason": "invalid"}]}
        ]

        customer = Customer(
            customer_id="newcustomer",
            customer_name="New Customer",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_newcustomer",
            industry=Industry.RETAIL,
        )

        with pytest.raises(RuntimeError) as exc_info:
            registry.add_customer(customer)
        assert "Failed to insert customer" in str(exc_info.value)

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_update_customer(self, mock_bq_client):
        """Test update_customer updates fields and clears cache."""
        registry = CustomerRegistry(registry_project_id="test-project")

        # Mock update query
        mock_bq_client.return_value.query.return_value.result.return_value = None

        # Mock get_customer return after update
        mock_row = Mock()
        mock_row.items.return_value = [
            ("customer_id", "topgolf"),
            ("customer_name", "TopGolf Updated"),
            ("gcp_project_id", "growthnav-prod"),
            ("dataset", "growthnav_topgolf"),
            ("industry", "golf"),
            ("status", "active"),
            ("tags", ["premium", "updated"]),
            ("google_ads_customer_ids", []),
            ("meta_ad_account_ids", []),
            ("google_ads_token_secret", None),
            ("meta_access_token_secret", None),
            ("onboarded_at", None),
            ("updated_at", None),
        ]

        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter([mock_row])

        # Set up the mock to return different results for different calls
        mock_query_job = MagicMock()
        mock_query_job.result.side_effect = [None, mock_result]
        mock_bq_client.return_value.query.return_value = mock_query_job

        updates = {"customer_name": "TopGolf Updated", "tags": ["premium", "updated"]}
        result = registry.update_customer("topgolf", updates)

        assert result is not None
        assert result.customer_name == "TopGolf Updated"
        assert result.tags == ["premium", "updated"]

    @patch("growthnav.bigquery.registry.bigquery.Client")
    def test_row_to_customer(self, mock_bq_client):
        """Test _row_to_customer conversion."""
        registry = CustomerRegistry(registry_project_id="test-project")

        row = {
            "customer_id": "test",
            "customer_name": "Test Customer",
            "gcp_project_id": "test-project",
            "dataset": "growthnav_test",
            "industry": "retail",
            "status": "onboarding",
            "tags": ["tag1", "tag2"],
            "google_ads_customer_ids": ["123"],
            "meta_ad_account_ids": ["456"],
            "google_ads_token_secret": "secret1",
            "meta_access_token_secret": "secret2",
            "onboarded_at": None,
            "updated_at": None,
        }

        customer = registry._row_to_customer(row)

        assert customer.customer_id == "test"
        assert customer.customer_name == "Test Customer"
        assert customer.industry == Industry.RETAIL
        assert customer.status == CustomerStatus.ONBOARDING
        assert customer.tags == ["tag1", "tag2"]

    def test_infer_bq_type(self):
        """Test _infer_bq_type type inference."""
        registry = CustomerRegistry(registry_project_id="test-project")

        assert registry._infer_bq_type(True) == "BOOL"
        assert registry._infer_bq_type(False) == "BOOL"
        assert registry._infer_bq_type(42) == "INT64"
        assert registry._infer_bq_type(3.14) == "FLOAT64"
        assert registry._infer_bq_type("string") == "STRING"
        assert registry._infer_bq_type([1, 2, 3]) == "STRING"  # JSON serialized
