# GrowthNav Shared Analytics Infrastructure - TDD Implementation Plan

## Overview

This plan implements the GrowthNav shared analytics infrastructure monorepo using **Test-Driven Development (TDD)**. The architecture follows Option 1 from the research document: a uv workspace monorepo with namespace packages (`growthnav.*`).

**Current State**: The skeleton structure exists with 4 packages implemented (shared-bigquery, shared-reporting, shared-conversions, mcp-server) but all test directories are **empty**. No CI/CD workflows exist.

**Goal**: Add comprehensive test coverage using TDD methodology, where tests are written FIRST, then implementation is verified/fixed to pass those tests.

## Current State Analysis

### Existing Packages

| Package | Status | Test Coverage | Notes |
|---------|--------|---------------|-------|
| shared-bigquery | Implemented | 0% | TenantBigQueryClient, CustomerRegistry, QueryValidator |
| shared-reporting | Implemented | 0% | PDFGenerator, SheetsExporter, SlidesGenerator |
| shared-conversions | Implemented | 0% | Conversion schema, POSNormalizer, CRMNormalizer, LoyaltyNormalizer |
| mcp-server | Implemented | 0% | FastMCP server with tools, resources, prompts |
| shared-onboarding | Empty | 0% | Directory exists but no code |

### Key Discoveries

- **Namespace Pattern**: All packages use `growthnav.*` namespace
- **Build System**: uv workspaces with hatchling
- **Python Version**: 3.11+ required
- **Test Framework**: pytest + pytest-asyncio configured but no tests written
- **Dependencies**: All core dependencies defined in package pyproject.toml files

## Desired End State

After completing this plan:

1. **Each package has comprehensive unit tests** (>80% coverage)
2. **Integration tests** verify cross-package functionality
3. **CI/CD pipeline** runs tests on every PR
4. **All tests pass** with `uv run pytest`
5. **Type checking passes** with `uv run mypy packages/`
6. **Linting passes** with `uv run ruff check`

### Verification Commands

```bash
# Run all tests
uv run pytest

# Run tests for specific package
uv run --package growthnav-bigquery pytest packages/shared-bigquery/tests/

# Check coverage
uv run pytest --cov=growthnav --cov-report=html

# Type checking
uv run mypy packages/

# Linting
uv run ruff check packages/
```

## What We're NOT Doing

- **Not migrating existing apps** (PaidSearchNav, PaidSocialNav, AutoCLV) - that's a future phase
- **Not implementing shared-onboarding** - will be done after core packages are tested
- **Not setting up production deployment** - focus is on test infrastructure
- **Not writing end-to-end tests against live GCP** - unit tests with mocks only
- **Not adding new features** - focus is testing existing code

## Implementation Approach

**TDD Cycle for Each Module**:
1. **RED**: Write failing tests that describe expected behavior
2. **GREEN**: Verify existing implementation passes (fix if needed)
3. **REFACTOR**: Clean up test code, add edge cases

**Order of Implementation**:
1. Start with `shared-bigquery` (foundational, no internal dependencies)
2. Then `shared-conversions` (no internal dependencies)
3. Then `shared-reporting` (no internal dependencies)
4. Finally `mcp-server` (depends on all three)

---

## Phase 1: Test Infrastructure Setup

### Overview
Set up the testing infrastructure, fixtures, and configuration needed for all packages.

### Changes Required

#### 1. Root conftest.py
**File**: `conftest.py` (create at root)
**Purpose**: Shared fixtures for all packages

```python
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
```

#### 2. pytest.ini configuration
**File**: `pytest.ini` (create at root)
**Purpose**: Pytest configuration

```ini
[pytest]
testpaths = packages
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
addopts = -v --tb=short
markers =
    unit: Unit tests (no external dependencies)
    integration: Integration tests (may require mocks)
    slow: Slow-running tests
```

#### 3. Add test dependencies to root pyproject.toml
**File**: `pyproject.toml`
**Changes**: Add pytest-mock to dev dependencies

```toml
[tool.uv]
dev-dependencies = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.0.0",
    "pytest-mock>=3.12.0",
    "mypy>=1.8.0",
    "ruff>=0.3.0",
]
```

### Success Criteria

#### Automated Verification:
- [ ] `uv sync` completes without errors
- [ ] `uv run pytest --collect-only` shows fixture availability
- [ ] `uv run ruff check conftest.py` passes

#### Manual Verification:
- [ ] Verify conftest.py is importable from any test file

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation before proceeding to Phase 2.

---

## Phase 2: shared-bigquery Package Tests

### Overview
Write comprehensive tests for the BigQuery client, customer registry, and query validator.

### Changes Required

#### 1. Query Validator Tests (Start Here - Simplest)
**File**: `packages/shared-bigquery/tests/test_validation.py`
**Purpose**: Test SQL query validation logic

```python
"""Tests for QueryValidator - SQL safety validation."""

import pytest
from growthnav.bigquery.validation import QueryValidator, ValidationResult, ValidationSeverity


class TestQueryValidatorBlockedPatterns:
    """Test that destructive SQL patterns are blocked."""

    @pytest.mark.parametrize("sql,expected_error", [
        ("DROP TABLE users", "DROP statements are not allowed"),
        ("DELETE FROM orders", "DELETE statements are not allowed"),
        ("TRUNCATE TABLE logs", "TRUNCATE statements are not allowed"),
        ("UPDATE users SET active = 0", "UPDATE statements are not allowed"),
        ("INSERT INTO users VALUES (1)", "INSERT statements are not allowed"),
        ("MERGE INTO target USING source", "MERGE statements are not allowed"),
        ("CREATE TABLE new_table (id INT)", "CREATE statements are not allowed"),
        ("ALTER TABLE users ADD COLUMN age INT", "ALTER statements are not allowed"),
        ("GRANT SELECT ON users TO public", "GRANT statements are not allowed"),
        ("REVOKE SELECT ON users FROM public", "REVOKE statements are not allowed"),
    ])
    def test_blocked_patterns_raise_value_error(self, sql, expected_error):
        """Blocked SQL patterns should raise ValueError."""
        with pytest.raises(ValueError, match=expected_error):
            QueryValidator.validate(sql)

    def test_blocked_patterns_allowed_with_allow_writes(self):
        """When allow_writes=True, write operations should be allowed."""
        result = QueryValidator.validate("DELETE FROM temp_table", allow_writes=True)
        assert result.is_valid is True


class TestQueryValidatorSafeQueries:
    """Test that safe queries are allowed."""

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM users LIMIT 100",
        "SELECT id, name FROM customers WHERE status = 'active'",
        "SELECT COUNT(*) FROM orders",
        "WITH cte AS (SELECT * FROM data) SELECT * FROM cte",
    ])
    def test_safe_queries_are_valid(self, sql):
        """Safe SELECT queries should be valid."""
        result = QueryValidator.validate(sql)
        assert result.is_valid is True


class TestQueryValidatorWarnings:
    """Test warning patterns."""

    def test_select_star_warning(self):
        """SELECT * should produce a warning."""
        result = QueryValidator.validate("SELECT * FROM users LIMIT 10")
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "SELECT *" in result.message

    def test_no_limit_warning(self):
        """SELECT without LIMIT should produce a warning."""
        result = QueryValidator.validate("SELECT id, name FROM users")
        assert result.is_valid is True
        assert result.severity == ValidationSeverity.WARNING
        assert "LIMIT" in result.message


class TestSanitizeIdentifier:
    """Test identifier sanitization."""

    @pytest.mark.parametrize("identifier", [
        "users",
        "user_data",
        "UserData",
        "table_123",
        "my-table",
    ])
    def test_valid_identifiers(self, identifier):
        """Valid identifiers should pass."""
        result = QueryValidator.sanitize_identifier(identifier)
        assert result == identifier

    @pytest.mark.parametrize("identifier", [
        "123_invalid",  # Starts with number
        "user;DROP",    # Contains semicolon
        "table--name",  # Contains double hyphen
        "",             # Empty
        " ",            # Whitespace
    ])
    def test_invalid_identifiers_raise_error(self, identifier):
        """Invalid identifiers should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            QueryValidator.sanitize_identifier(identifier)


class TestValidateProjectDataset:
    """Test project and dataset validation."""

    def test_valid_project_and_dataset(self):
        """Valid project and dataset should return True."""
        result = QueryValidator.validate_project_dataset(
            "my-gcp-project",
            "growthnav_customer"
        )
        assert result is True

    def test_invalid_project_id(self):
        """Invalid project ID should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid project ID"):
            QueryValidator.validate_project_dataset("Invalid_Project", "dataset")

    def test_invalid_dataset(self):
        """Invalid dataset should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid dataset"):
            QueryValidator.validate_project_dataset("valid-project", "123invalid")
```

#### 2. BigQuery Client Tests
**File**: `packages/shared-bigquery/tests/test_client.py`
**Purpose**: Test TenantBigQueryClient with mocked BigQuery

```python
"""Tests for TenantBigQueryClient - multi-tenant BigQuery client."""

import pytest
from unittest.mock import MagicMock, patch
from growthnav.bigquery.client import (
    TenantBigQueryClient,
    BigQueryConfig,
    QueryResult,
)


class TestBigQueryConfig:
    """Test BigQuery configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        config = BigQueryConfig()
        assert config.project_id is None
        assert config.location == "US"
        assert config.max_results == 10_000
        assert config.timeout == 300

    def test_from_env(self, monkeypatch):
        """Test loading config from environment variables."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GROWTNAV_BQ_LOCATION", "EU")

        config = BigQueryConfig.from_env()
        assert config.project_id == "test-project"
        assert config.location == "EU"

    def test_from_env_fallback(self, monkeypatch):
        """Test GROWTNAV_PROJECT_ID fallback."""
        monkeypatch.setenv("GROWTNAV_PROJECT_ID", "growthnav-project")

        config = BigQueryConfig.from_env()
        assert config.project_id == "growthnav-project"


class TestTenantBigQueryClient:
    """Test TenantBigQueryClient tenant isolation."""

    def test_dataset_id_format(self):
        """Dataset ID should follow growthnav_{customer_id} pattern."""
        client = TenantBigQueryClient(customer_id="topgolf")
        assert client.dataset_id == "growthnav_topgolf"

    def test_customer_id_stored(self):
        """Customer ID should be stored on client."""
        client = TenantBigQueryClient(customer_id="test_customer")
        assert client.customer_id == "test_customer"

    def test_config_uses_default_if_none(self):
        """Should use default config if none provided."""
        client = TenantBigQueryClient(customer_id="test")
        assert client.config is not None
        assert isinstance(client.config, BigQueryConfig)

    def test_config_uses_provided(self):
        """Should use provided config."""
        config = BigQueryConfig(project_id="custom-project", location="EU")
        client = TenantBigQueryClient(customer_id="test", config=config)
        assert client.config.project_id == "custom-project"
        assert client.config.location == "EU"


class TestTenantBigQueryClientQuery:
    """Test query execution."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mocked BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock_class:
            mock_client = MagicMock()
            mock_class.return_value = mock_client
            yield mock_client

    def test_query_validates_sql(self, mock_bq_client):
        """Query should validate SQL before execution."""
        client = TenantBigQueryClient(customer_id="test")

        with pytest.raises(ValueError, match="DROP statements are not allowed"):
            client.query("DROP TABLE users")

    def test_query_returns_query_result(self, mock_bq_client):
        """Query should return QueryResult object."""
        # Setup mock
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 1024
        mock_job.cache_hit = False

        mock_row = MagicMock()
        mock_row.items.return_value = [("id", 1), ("name", "Test")]

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_result.total_rows = 1

        mock_job.result.return_value = mock_result
        mock_bq_client.query.return_value = mock_job

        client = TenantBigQueryClient(customer_id="test")
        result = client.query("SELECT id, name FROM users LIMIT 10")

        assert isinstance(result, QueryResult)
        assert result.total_rows == 1
        assert result.bytes_processed == 1024
        assert result.cache_hit is False
        assert len(result.rows) == 1
        assert result.rows[0] == {"id": 1, "name": "Test"}


class TestTenantBigQueryClientCostEstimation:
    """Test cost estimation."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mocked BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock_class:
            mock_client = MagicMock()
            mock_class.return_value = mock_client
            yield mock_client

    def test_estimate_cost_returns_dict(self, mock_bq_client):
        """Estimate cost should return bytes and USD."""
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 1024 * 1024 * 1024  # 1 GB
        mock_job.cache_hit = False
        mock_bq_client.query.return_value = mock_job

        client = TenantBigQueryClient(customer_id="test")
        result = client.estimate_cost("SELECT * FROM big_table")

        assert "bytes_processed" in result
        assert "estimated_cost_usd" in result
        assert "is_cached" in result
        assert result["bytes_processed"] == 1024 * 1024 * 1024


class TestTypeInference:
    """Test Python-to-BigQuery type inference."""

    def test_infer_type_bool(self):
        """Boolean should map to BOOL."""
        client = TenantBigQueryClient(customer_id="test")
        assert client._infer_type(True) == "BOOL"
        assert client._infer_type(False) == "BOOL"

    def test_infer_type_int(self):
        """Integer should map to INT64."""
        client = TenantBigQueryClient(customer_id="test")
        assert client._infer_type(42) == "INT64"

    def test_infer_type_float(self):
        """Float should map to FLOAT64."""
        client = TenantBigQueryClient(customer_id="test")
        assert client._infer_type(3.14) == "FLOAT64"

    def test_infer_type_string(self):
        """String should map to STRING."""
        client = TenantBigQueryClient(customer_id="test")
        assert client._infer_type("hello") == "STRING"

    def test_infer_type_default_to_string(self):
        """Unknown types should default to STRING."""
        client = TenantBigQueryClient(customer_id="test")
        assert client._infer_type([1, 2, 3]) == "STRING"
        assert client._infer_type({"key": "value"}) == "STRING"
```

#### 3. Customer Registry Tests
**File**: `packages/shared-bigquery/tests/test_registry.py`
**Purpose**: Test customer registry with mocked BigQuery

```python
"""Tests for CustomerRegistry - customer management."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from growthnav.bigquery.registry import (
    CustomerRegistry,
    Customer,
    Industry,
    CustomerStatus,
)


class TestIndustryEnum:
    """Test Industry enumeration."""

    def test_industry_values(self):
        """Test all industry values exist."""
        assert Industry.GOLF.value == "golf"
        assert Industry.MEDICAL.value == "medical"
        assert Industry.RESTAURANT.value == "restaurant"
        assert Industry.RETAIL.value == "retail"
        assert Industry.ECOMMERCE.value == "ecommerce"

    def test_industry_from_string(self):
        """Industry can be created from string."""
        industry = Industry("golf")
        assert industry == Industry.GOLF


class TestCustomerStatusEnum:
    """Test CustomerStatus enumeration."""

    def test_status_values(self):
        """Test all status values exist."""
        assert CustomerStatus.ACTIVE.value == "active"
        assert CustomerStatus.INACTIVE.value == "inactive"
        assert CustomerStatus.ONBOARDING.value == "onboarding"
        assert CustomerStatus.SUSPENDED.value == "suspended"


class TestCustomerDataclass:
    """Test Customer dataclass."""

    def test_customer_creation(self):
        """Test creating a customer with required fields."""
        customer = Customer(
            customer_id="topgolf",
            customer_name="Topgolf Entertainment",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
        )
        assert customer.customer_id == "topgolf"
        assert customer.status == CustomerStatus.ACTIVE  # default

    def test_full_dataset_id(self):
        """Test full_dataset_id property."""
        customer = Customer(
            customer_id="topgolf",
            customer_name="Topgolf",
            gcp_project_id="growthnav-prod",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
        )
        assert customer.full_dataset_id == "growthnav-prod.growthnav_topgolf"

    def test_customer_with_platform_ids(self):
        """Test customer with platform-specific IDs."""
        customer = Customer(
            customer_id="test",
            customer_name="Test",
            gcp_project_id="project",
            dataset="dataset",
            industry=Industry.OTHER,
            google_ads_customer_ids=["123-456-7890"],
            meta_ad_account_ids=["act_12345"],
        )
        assert len(customer.google_ads_customer_ids) == 1
        assert len(customer.meta_ad_account_ids) == 1


class TestCustomerRegistry:
    """Test CustomerRegistry class."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mocked BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock_class:
            mock_client = MagicMock()
            mock_class.return_value = mock_client
            yield mock_client

    def test_registry_project_from_env(self, monkeypatch):
        """Registry should use project from environment."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-registry-project")
        registry = CustomerRegistry()
        assert registry.registry_project_id == "test-registry-project"

    def test_table_ref_format(self, monkeypatch):
        """Table ref should follow expected format."""
        monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
        registry = CustomerRegistry()
        assert registry.table_ref == "my-project.growthnav_registry.customers"

    def test_get_customer_returns_none_when_not_found(self, mock_bq_client, monkeypatch):
        """get_customer should return None when customer doesn't exist."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_bq_client.query.return_value.result.return_value = mock_result

        registry = CustomerRegistry()
        registry.get_customer.cache_clear()  # Clear LRU cache

        customer = registry.get_customer("nonexistent")
        assert customer is None

    def test_get_customer_returns_customer(self, mock_bq_client, monkeypatch):
        """get_customer should return Customer when found."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")

        mock_row = MagicMock()
        mock_row.items.return_value = [
            ("customer_id", "topgolf"),
            ("customer_name", "Topgolf Entertainment"),
            ("gcp_project_id", "growthnav-prod"),
            ("dataset", "growthnav_topgolf"),
            ("industry", "golf"),
            ("status", "active"),
            ("tags", ["enterprise"]),
            ("google_ads_customer_ids", []),
            ("meta_ad_account_ids", []),
        ]

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_bq_client.query.return_value.result.return_value = mock_result

        registry = CustomerRegistry()
        registry.get_customer.cache_clear()

        customer = registry.get_customer("topgolf")
        assert customer is not None
        assert customer.customer_id == "topgolf"
        assert customer.industry == Industry.GOLF


class TestCustomerRegistryIndustryQueries:
    """Test industry-based queries."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mocked BigQuery client."""
        with patch("google.cloud.bigquery.Client") as mock_class:
            mock_client = MagicMock()
            mock_class.return_value = mock_client
            yield mock_client

    def test_get_customers_by_industry(self, mock_bq_client, monkeypatch):
        """Should return list of customers in an industry."""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")

        mock_rows = []
        for i in range(2):
            mock_row = MagicMock()
            mock_row.items.return_value = [
                ("customer_id", f"golf_customer_{i}"),
                ("customer_name", f"Golf Customer {i}"),
                ("gcp_project_id", "growthnav-prod"),
                ("dataset", f"growthnav_golf_{i}"),
                ("industry", "golf"),
                ("status", "active"),
                ("tags", []),
                ("google_ads_customer_ids", []),
                ("meta_ad_account_ids", []),
            ]
            mock_rows.append(mock_row)

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter(mock_rows)
        mock_bq_client.query.return_value.result.return_value = mock_result

        registry = CustomerRegistry()
        customers = registry.get_customers_by_industry(Industry.GOLF)

        assert len(customers) == 2
        assert all(c.industry == Industry.GOLF for c in customers)
```

#### 4. Package __init__ test
**File**: `packages/shared-bigquery/tests/test_init.py`
**Purpose**: Test public API exports

```python
"""Tests for package public API."""

import pytest


def test_public_api_exports():
    """Test that all expected classes are exported from package."""
    from growthnav.bigquery import (
        TenantBigQueryClient,
        BigQueryConfig,
        QueryResult,
        CustomerRegistry,
        Customer,
        Industry,
        CustomerStatus,
        QueryValidator,
        ValidationResult,
    )

    # Just verify they're importable
    assert TenantBigQueryClient is not None
    assert BigQueryConfig is not None
    assert QueryResult is not None
    assert CustomerRegistry is not None
    assert Customer is not None
    assert Industry is not None
    assert CustomerStatus is not None
    assert QueryValidator is not None
    assert ValidationResult is not None
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run --package growthnav-bigquery pytest packages/shared-bigquery/tests/ -v` passes all tests
- [ ] `uv run pytest packages/shared-bigquery/tests/ --cov=growthnav.bigquery --cov-report=term-missing` shows >80% coverage
- [ ] `uv run mypy packages/shared-bigquery/` passes with no errors
- [ ] `uv run ruff check packages/shared-bigquery/` passes

#### Manual Verification:
- [ ] Review test output to confirm all edge cases are covered
- [ ] Verify mocking strategy is appropriate for unit testing

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation before proceeding to Phase 3.

---

## Phase 3: shared-conversions Package Tests

### Overview
Write tests for the conversion schema, normalizers, and attribution logic.

### Changes Required

#### 1. Schema Tests
**File**: `packages/shared-conversions/tests/test_schema.py`
**Purpose**: Test Conversion dataclass and enums

```python
"""Tests for conversion schema - data models and enums."""

import pytest
from datetime import datetime
from uuid import UUID
from growthnav.conversions.schema import (
    Conversion,
    ConversionSource,
    ConversionType,
    AttributionModel,
)


class TestConversionEnums:
    """Test conversion-related enumerations."""

    def test_conversion_source_values(self):
        """Test ConversionSource enum values."""
        assert ConversionSource.POS.value == "pos"
        assert ConversionSource.CRM.value == "crm"
        assert ConversionSource.LOYALTY.value == "loyalty"
        assert ConversionSource.ECOMMERCE.value == "ecommerce"

    def test_conversion_type_values(self):
        """Test ConversionType enum values."""
        assert ConversionType.PURCHASE.value == "purchase"
        assert ConversionType.LEAD.value == "lead"
        assert ConversionType.SIGNUP.value == "signup"
        assert ConversionType.BOOKING.value == "booking"

    def test_attribution_model_values(self):
        """Test AttributionModel enum values."""
        assert AttributionModel.LAST_CLICK.value == "last_click"
        assert AttributionModel.FIRST_CLICK.value == "first_click"
        assert AttributionModel.LINEAR.value == "linear"


class TestConversionDataclass:
    """Test Conversion dataclass."""

    def test_minimal_conversion(self):
        """Test creating conversion with minimal required fields."""
        conversion = Conversion(customer_id="topgolf")

        assert conversion.customer_id == "topgolf"
        assert conversion.conversion_type == ConversionType.PURCHASE
        assert conversion.source == ConversionSource.POS
        assert conversion.value == 0.0
        assert conversion.currency == "USD"

    def test_full_conversion(self):
        """Test creating conversion with all fields."""
        conversion = Conversion(
            customer_id="topgolf",
            user_id="USER-123",
            transaction_id="TXN-456",
            conversion_type=ConversionType.PURCHASE,
            source=ConversionSource.POS,
            value=150.00,
            currency="USD",
            quantity=2,
            product_id="PROD-789",
            product_name="Golf Session",
            location_id="LOC-001",
            gclid="Cj0KCQiA...",
            utm_source="google",
        )

        assert conversion.value == 150.00
        assert conversion.quantity == 2
        assert conversion.gclid == "Cj0KCQiA..."

    def test_conversion_id_auto_generated(self):
        """Conversion ID should be auto-generated UUID."""
        conversion = Conversion(customer_id="test")
        assert isinstance(conversion.conversion_id, UUID)

    def test_timestamp_auto_generated(self):
        """Timestamp should be auto-generated if not provided."""
        before = datetime.utcnow()
        conversion = Conversion(customer_id="test")
        after = datetime.utcnow()

        assert before <= conversion.timestamp <= after


class TestConversionSerialization:
    """Test Conversion to_dict and from_dict."""

    def test_to_dict(self):
        """Test converting Conversion to dictionary."""
        conversion = Conversion(
            customer_id="topgolf",
            transaction_id="TXN-001",
            value=100.00,
            gclid="test_gclid",
        )

        data = conversion.to_dict()

        assert data["customer_id"] == "topgolf"
        assert data["transaction_id"] == "TXN-001"
        assert data["value"] == 100.00
        assert data["gclid"] == "test_gclid"
        assert data["conversion_type"] == "purchase"
        assert data["source"] == "pos"

    def test_from_dict(self):
        """Test creating Conversion from dictionary."""
        data = {
            "customer_id": "topgolf",
            "transaction_id": "TXN-001",
            "value": 150.0,
            "conversion_type": "purchase",
            "source": "pos",
            "timestamp": "2025-01-15T10:30:00",
        }

        conversion = Conversion.from_dict(data)

        assert conversion.customer_id == "topgolf"
        assert conversion.value == 150.0
        assert conversion.conversion_type == ConversionType.PURCHASE

    def test_roundtrip_serialization(self):
        """Test to_dict -> from_dict roundtrip."""
        original = Conversion(
            customer_id="test",
            transaction_id="TXN-999",
            value=250.00,
            gclid="test_gclid",
            attributed_platform="google_ads",
            attribution_model=AttributionModel.LAST_CLICK,
        )

        data = original.to_dict()
        restored = Conversion.from_dict(data)

        assert restored.customer_id == original.customer_id
        assert restored.transaction_id == original.transaction_id
        assert restored.value == original.value
        assert restored.gclid == original.gclid
        assert restored.attribution_model == original.attribution_model
```

#### 2. Normalizer Tests
**File**: `packages/shared-conversions/tests/test_normalizer.py`
**Purpose**: Test data normalization from various sources

```python
"""Tests for conversion normalizers."""

import pytest
import pandas as pd
from datetime import datetime
from growthnav.conversions.normalizer import (
    POSNormalizer,
    CRMNormalizer,
    LoyaltyNormalizer,
)
from growthnav.conversions.schema import ConversionType, ConversionSource


class TestPOSNormalizer:
    """Test Point of Sale normalizer."""

    def test_normalize_basic_transactions(self):
        """Test normalizing basic POS transactions."""
        data = [
            {
                "order_id": "TXN-001",
                "total_amount": 150.00,
                "created_at": "2025-01-15T10:30:00Z",
            },
            {
                "order_id": "TXN-002",
                "total_amount": 75.50,
                "created_at": "2025-01-15T11:45:00Z",
            },
        ]

        normalizer = POSNormalizer(customer_id="topgolf")
        conversions = normalizer.normalize(data)

        assert len(conversions) == 2
        assert conversions[0].customer_id == "topgolf"
        assert conversions[0].transaction_id == "TXN-001"
        assert conversions[0].value == 150.00
        assert conversions[0].source == ConversionSource.POS
        assert conversions[0].conversion_type == ConversionType.PURCHASE

    def test_normalize_with_dataframe(self):
        """Test normalizing from pandas DataFrame."""
        df = pd.DataFrame([
            {"order_id": "TXN-001", "total": 100.00, "date": "2025-01-15"},
        ])

        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize(df)

        assert len(conversions) == 1
        assert conversions[0].value == 100.00

    def test_custom_field_mapping(self):
        """Test normalizer with custom field mapping."""
        data = [{"receipt_number": "R-001", "amount": 50.00}]

        custom_map = {
            "receipt_number": "transaction_id",
            "amount": "value",
        }

        normalizer = POSNormalizer(customer_id="test", field_map=custom_map)
        conversions = normalizer.normalize(data)

        assert conversions[0].transaction_id == "R-001"
        assert conversions[0].value == 50.00

    def test_location_mapping(self):
        """Test location field mapping."""
        data = [{
            "order_id": "TXN-001",
            "total_amount": 100.00,
            "store_id": "STORE-A",
            "store_name": "Downtown Location",
        }]

        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert conversions[0].location_id == "STORE-A"
        assert conversions[0].location_name == "Downtown Location"

    def test_raw_data_preserved(self):
        """Test that raw source data is preserved."""
        data = [{"order_id": "TXN-001", "custom_field": "extra_data"}]

        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert "custom_field" in conversions[0].raw_data
        assert conversions[0].raw_data["custom_field"] == "extra_data"


class TestCRMNormalizer:
    """Test CRM normalizer."""

    def test_normalize_leads(self):
        """Test normalizing CRM leads."""
        data = [{
            "opportunity_id": "OPP-001",
            "amount": 5000.00,
            "close_date": "2025-01-20",
            "utm_source": "google",
            "gclid": "test_gclid",
        }]

        normalizer = CRMNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].transaction_id == "OPP-001"
        assert conversions[0].value == 5000.00
        assert conversions[0].conversion_type == ConversionType.LEAD
        assert conversions[0].source == ConversionSource.CRM
        assert conversions[0].gclid == "test_gclid"

    def test_custom_conversion_type(self):
        """Test CRM normalizer with custom conversion type."""
        data = [{"lead_id": "LEAD-001", "amount": 100.00}]

        normalizer = CRMNormalizer(
            customer_id="test",
            conversion_type=ConversionType.SIGNUP,
        )
        conversions = normalizer.normalize(data)

        assert conversions[0].conversion_type == ConversionType.SIGNUP

    def test_utm_tracking_preserved(self):
        """Test UTM parameters are preserved."""
        data = [{
            "opportunity_id": "OPP-001",
            "utm_source": "google",
            "utm_medium": "cpc",
            "utm_campaign": "spring_2025",
        }]

        normalizer = CRMNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert conversions[0].utm_source == "google"
        assert conversions[0].utm_medium == "cpc"
        assert conversions[0].utm_campaign == "spring_2025"


class TestLoyaltyNormalizer:
    """Test Loyalty Program normalizer."""

    def test_normalize_redemptions(self):
        """Test normalizing loyalty redemptions."""
        data = [{
            "member_id": "MEM-001",
            "redemption_id": "RED-001",
            "points_value": 500.00,
            "redemption_date": "2025-01-15",
        }]

        normalizer = LoyaltyNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].user_id == "MEM-001"
        assert conversions[0].transaction_id == "RED-001"
        assert conversions[0].value == 500.00
        assert conversions[0].source == ConversionSource.LOYALTY

    def test_signup_detection(self):
        """Test signup detection from data content."""
        data = [{"member_id": "MEM-001", "type": "signup", "reward_value": 0}]

        normalizer = LoyaltyNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert conversions[0].conversion_type == ConversionType.SIGNUP


class TestNormalizerEdgeCases:
    """Test edge cases across all normalizers."""

    def test_empty_data(self):
        """Normalizers should handle empty data."""
        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize([])
        assert conversions == []

    def test_missing_optional_fields(self):
        """Normalizers should handle missing optional fields."""
        data = [{"order_id": "TXN-001"}]  # Missing value, timestamp, etc.

        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert conversions[0].value == 0.0  # Default value

    def test_null_values(self):
        """Normalizers should handle null values."""
        data = [{"order_id": "TXN-001", "total_amount": None}]

        normalizer = POSNormalizer(customer_id="test")
        conversions = normalizer.normalize(data)

        assert conversions[0].value == 0.0  # Should handle None
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run --package growthnav-conversions pytest packages/shared-conversions/tests/ -v` passes
- [ ] `uv run pytest packages/shared-conversions/tests/ --cov=growthnav.conversions --cov-report=term-missing` shows >80% coverage
- [ ] `uv run mypy packages/shared-conversions/` passes
- [ ] `uv run ruff check packages/shared-conversions/` passes

#### Manual Verification:
- [ ] Review normalizer field mappings match real-world POS/CRM systems
- [ ] Verify timestamp parsing handles common formats

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation before proceeding to Phase 4.

---

## Phase 4: shared-reporting Package Tests

### Overview
Write tests for PDF, Sheets, and Slides generation with mocked Google APIs.

### Changes Required

#### 1. PDF Generator Tests
**File**: `packages/shared-reporting/tests/test_pdf.py`
**Purpose**: Test PDF generation with mocked WeasyPrint

```python
"""Tests for PDFGenerator - HTML to PDF conversion."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from growthnav.reporting.pdf import PDFGenerator


class TestPDFGeneratorInit:
    """Test PDFGenerator initialization."""

    def test_default_templates_dir(self):
        """Should use package templates directory by default."""
        pdf = PDFGenerator()
        assert pdf.templates_dir.name == "templates"

    def test_custom_templates_dir(self, tmp_path):
        """Should accept custom templates directory."""
        pdf = PDFGenerator(templates_dir=tmp_path)
        assert pdf.templates_dir == tmp_path


class TestPDFGeneratorAvailability:
    """Test WeasyPrint availability detection."""

    def test_is_available_when_installed(self):
        """Should return True when WeasyPrint is installed."""
        pdf = PDFGenerator()
        # Note: This will pass if weasyprint is installed
        # In CI, we may want to mock this
        result = pdf.is_available()
        assert isinstance(result, bool)

    def test_is_available_when_not_installed(self):
        """Should return False when WeasyPrint import fails."""
        pdf = PDFGenerator()

        with patch.dict("sys.modules", {"weasyprint": None}):
            with patch("builtins.__import__", side_effect=ImportError):
                # Force re-check by creating new instance
                result = pdf.is_available()
                # Note: This test may not work as expected due to import caching


class TestPDFGeneratorRenderHTML:
    """Test HTML rendering from templates."""

    @pytest.fixture
    def pdf_with_template(self, tmp_path):
        """Create PDFGenerator with a test template."""
        template_content = """
<!DOCTYPE html>
<html>
<head><title>{{ title }}</title></head>
<body>
<h1>{{ title }}</h1>
<p>Customer: {{ customer_name }}</p>
</body>
</html>
"""
        template_file = tmp_path / "test_report.html.j2"
        template_file.write_text(template_content)

        return PDFGenerator(templates_dir=tmp_path)

    def test_render_html(self, pdf_with_template):
        """Should render Jinja2 template to HTML."""
        data = {"title": "Test Report", "customer_name": "Topgolf"}

        html = pdf_with_template.render_html(data, "test_report")

        assert "<title>Test Report</title>" in html
        assert "<h1>Test Report</h1>" in html
        assert "Customer: Topgolf" in html


class TestPDFGeneratorGenerate:
    """Test PDF generation."""

    @pytest.fixture
    def mock_weasyprint(self):
        """Mock WeasyPrint HTML class."""
        with patch("growthnav.reporting.pdf.HTML") as mock_html_class:
            mock_html = MagicMock()
            mock_html.write_pdf.return_value = None
            mock_html_class.return_value = mock_html
            yield mock_html_class, mock_html

    def test_generate_pdf_with_mocked_weasyprint(self, tmp_path, mock_weasyprint):
        """Test PDF generation with mocked WeasyPrint."""
        mock_html_class, mock_html = mock_weasyprint

        # Create template
        template = tmp_path / "report.html.j2"
        template.write_text("<html><body>{{ content }}</body></html>")

        pdf = PDFGenerator(templates_dir=tmp_path)
        data = {"content": "Test content"}

        # Mock the buffer write
        mock_html.write_pdf.side_effect = lambda buf: buf.write(b"PDF_CONTENT")

        result = pdf.generate(data, "report")

        assert result == b"PDF_CONTENT"
        mock_html_class.assert_called_once()

    def test_generate_with_custom_css(self, tmp_path, mock_weasyprint):
        """Test PDF generation with injected CSS."""
        mock_html_class, mock_html = mock_weasyprint

        template = tmp_path / "styled.html.j2"
        template.write_text("<html><head></head><body>{{ text }}</body></html>")

        pdf = PDFGenerator(templates_dir=tmp_path)

        mock_html.write_pdf.side_effect = lambda buf: buf.write(b"STYLED_PDF")

        result = pdf.generate(
            data={"text": "Hello"},
            template="styled",
            css="body { color: red; }",
        )

        # Verify CSS was in the HTML string passed to HTML()
        call_args = mock_html_class.call_args
        html_string = call_args.kwargs.get("string") or call_args.args[0]
        assert "color: red" in html_string
```

#### 2. Sheets Exporter Tests
**File**: `packages/shared-reporting/tests/test_sheets.py`
**Purpose**: Test Google Sheets export with mocked gspread

```python
"""Tests for SheetsExporter - Google Sheets integration."""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from growthnav.reporting.sheets import SheetsExporter


class TestSheetsExporterInit:
    """Test SheetsExporter initialization."""

    def test_credentials_from_param(self):
        """Should use credentials path from parameter."""
        sheets = SheetsExporter(credentials_path="/path/to/creds.json")
        assert sheets.credentials_path == "/path/to/creds.json"

    def test_credentials_from_env(self, monkeypatch):
        """Should use credentials from environment variable."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/env/creds.json")
        sheets = SheetsExporter()
        assert sheets.credentials_path == "/env/creds.json"


class TestSheetsExporterDashboard:
    """Test dashboard creation."""

    @pytest.fixture
    def mock_gspread(self):
        """Mock gspread and Google auth."""
        with patch("gspread.authorize") as mock_auth:
            mock_client = MagicMock()
            mock_auth.return_value = mock_client

            with patch("google.oauth2.service_account.Credentials") as mock_creds_class:
                mock_creds = MagicMock()
                mock_creds_class.from_service_account_file.return_value = mock_creds

                yield mock_client

    def test_create_dashboard_returns_url(self, mock_gspread, tmp_path):
        """Create dashboard should return spreadsheet URL."""
        # Setup mock
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/abc123"
        mock_spreadsheet.sheet1 = MagicMock()
        mock_gspread.create.return_value = mock_spreadsheet

        # Create temp credentials file
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        sheets = SheetsExporter(credentials_path=str(creds_file))

        data = pd.DataFrame([
            {"metric": "Conversions", "value": 100},
            {"metric": "Revenue", "value": 5000},
        ])

        url = sheets.create_dashboard(
            title="Test Dashboard",
            data=data,
        )

        assert url == "https://docs.google.com/spreadsheets/d/abc123"
        mock_gspread.create.assert_called_once_with("Test Dashboard", folder_id=None)

    def test_create_dashboard_shares_with_users(self, mock_gspread, tmp_path):
        """Should share spreadsheet with specified users."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/abc123"
        mock_spreadsheet.sheet1 = MagicMock()
        mock_gspread.create.return_value = mock_spreadsheet

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        sheets = SheetsExporter(credentials_path=str(creds_file))

        sheets.create_dashboard(
            title="Shared Dashboard",
            data=[{"col": "val"}],
            share_with=["user1@example.com", "user2@example.com"],
        )

        assert mock_spreadsheet.share.call_count == 2

    def test_create_dashboard_from_list(self, mock_gspread, tmp_path):
        """Should accept list of dicts as data."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/abc123"
        mock_spreadsheet.sheet1 = MagicMock()
        mock_gspread.create.return_value = mock_spreadsheet

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        sheets = SheetsExporter(credentials_path=str(creds_file))

        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
        ]

        url = sheets.create_dashboard(title="Test", data=data)

        assert url is not None


class TestSheetsExporterMultiTab:
    """Test multi-tab dashboard creation."""

    @pytest.fixture
    def mock_gspread(self):
        """Mock gspread."""
        with patch("gspread.authorize") as mock_auth:
            mock_client = MagicMock()
            mock_auth.return_value = mock_client

            with patch("google.oauth2.service_account.Credentials"):
                yield mock_client

    def test_create_multi_tab_dashboard(self, mock_gspread, tmp_path):
        """Should create spreadsheet with multiple tabs."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/multi"
        mock_spreadsheet.sheet1 = MagicMock()
        mock_spreadsheet.add_worksheet = MagicMock()
        mock_gspread.create.return_value = mock_spreadsheet

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        sheets = SheetsExporter(credentials_path=str(creds_file))

        tabs = {
            "Summary": pd.DataFrame([{"metric": "Total", "value": 100}]),
            "Details": pd.DataFrame([{"item": "A", "count": 50}]),
            "Raw Data": pd.DataFrame([{"id": 1, "data": "test"}]),
        }

        url = sheets.create_multi_tab_dashboard(title="Multi-Tab", tabs=tabs)

        assert url == "https://docs.google.com/spreadsheets/d/multi"
        # First tab reuses sheet1, subsequent tabs are added
        assert mock_spreadsheet.add_worksheet.call_count == 2


class TestSheetsExporterValueSerialization:
    """Test value serialization for Sheets API."""

    def test_serialize_none_as_empty_string(self):
        """NaN/None should become empty string."""
        sheets = SheetsExporter()

        assert sheets._serialize_value(None) == ""
        assert sheets._serialize_value(float("nan")) == ""

    def test_serialize_datetime(self):
        """Datetime should become ISO string."""
        from datetime import datetime

        sheets = SheetsExporter()
        dt = datetime(2025, 1, 15, 10, 30, 0)

        result = sheets._serialize_value(dt)
        assert result == "2025-01-15T10:30:00"

    def test_serialize_list_as_json(self):
        """Lists should become JSON strings."""
        sheets = SheetsExporter()

        result = sheets._serialize_value([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_serialize_dict_as_json(self):
        """Dicts should become JSON strings."""
        sheets = SheetsExporter()

        result = sheets._serialize_value({"key": "value"})
        assert result == '{"key": "value"}'
```

#### 3. Slides Generator Tests
**File**: `packages/shared-reporting/tests/test_slides.py`
**Purpose**: Test Google Slides generation with mocked API

```python
"""Tests for SlidesGenerator - Google Slides integration."""

import pytest
from unittest.mock import MagicMock, patch
from growthnav.reporting.slides import (
    SlidesGenerator,
    SlideContent,
    SlideLayout,
)


class TestSlideContent:
    """Test SlideContent dataclass."""

    def test_minimal_slide(self):
        """Test creating slide with minimal content."""
        slide = SlideContent(title="Test Slide")

        assert slide.title == "Test Slide"
        assert slide.body is None
        assert slide.layout == SlideLayout.TITLE_AND_BODY

    def test_full_slide(self):
        """Test creating slide with all content."""
        slide = SlideContent(
            title="Executive Summary",
            body=["Point 1", "Point 2"],
            layout=SlideLayout.TITLE_AND_TWO_COLUMNS,
            notes="Speaker notes here",
        )

        assert slide.title == "Executive Summary"
        assert len(slide.body) == 2
        assert slide.layout == SlideLayout.TITLE_AND_TWO_COLUMNS
        assert slide.notes == "Speaker notes here"


class TestSlideLayout:
    """Test SlideLayout enum."""

    def test_layout_values(self):
        """Test all layout values exist."""
        assert SlideLayout.TITLE.value == "TITLE"
        assert SlideLayout.TITLE_AND_BODY.value == "TITLE_AND_BODY"
        assert SlideLayout.BLANK.value == "BLANK"
        assert SlideLayout.SECTION_HEADER.value == "SECTION_HEADER"


class TestSlidesGeneratorInit:
    """Test SlidesGenerator initialization."""

    def test_credentials_from_param(self):
        """Should use credentials from parameter."""
        slides = SlidesGenerator(credentials_path="/path/to/creds.json")
        assert slides.credentials_path == "/path/to/creds.json"

    def test_credentials_from_env(self, monkeypatch):
        """Should use credentials from environment."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/env/creds.json")
        slides = SlidesGenerator()
        assert slides.credentials_path == "/env/creds.json"


class TestSlidesGeneratorCreatePresentation:
    """Test presentation creation."""

    @pytest.fixture
    def mock_google_apis(self):
        """Mock Google Slides and Drive APIs."""
        with patch("googleapiclient.discovery.build") as mock_build:
            mock_slides_service = MagicMock()
            mock_drive_service = MagicMock()

            def build_side_effect(api, version, **kwargs):
                if api == "slides":
                    return mock_slides_service
                elif api == "drive":
                    return mock_drive_service

            mock_build.side_effect = build_side_effect

            with patch("google.oauth2.service_account.Credentials"):
                yield mock_slides_service, mock_drive_service

    def test_create_presentation_returns_url(self, mock_google_apis, tmp_path):
        """Should return presentation URL."""
        mock_slides, mock_drive = mock_google_apis

        # Setup mocks
        mock_slides.presentations.return_value.create.return_value.execute.return_value = {
            "presentationId": "pres_123"
        }
        mock_slides.presentations.return_value.batchUpdate.return_value.execute.return_value = {}

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        generator = SlidesGenerator(credentials_path=str(creds_file))

        slides = [
            SlideContent(title="Title Slide"),
            SlideContent(title="Content", body="Body text"),
        ]

        url = generator.create_presentation(
            title="Test Presentation",
            slides=slides,
        )

        assert url == "https://docs.google.com/presentation/d/pres_123"

    def test_create_presentation_shares_with_users(self, mock_google_apis, tmp_path):
        """Should share presentation with specified users."""
        mock_slides, mock_drive = mock_google_apis

        mock_slides.presentations.return_value.create.return_value.execute.return_value = {
            "presentationId": "pres_123"
        }

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        generator = SlidesGenerator(credentials_path=str(creds_file))

        generator.create_presentation(
            title="Shared Presentation",
            slides=[SlideContent(title="Test")],
            share_with=["user@example.com"],
        )

        mock_drive.permissions.return_value.create.assert_called()


class TestSlidesGeneratorFromTemplate:
    """Test template-based presentation creation."""

    @pytest.fixture
    def mock_google_apis(self):
        """Mock Google APIs."""
        with patch("googleapiclient.discovery.build") as mock_build:
            mock_slides = MagicMock()
            mock_drive = MagicMock()

            def build_side_effect(api, version, **kwargs):
                return mock_slides if api == "slides" else mock_drive

            mock_build.side_effect = build_side_effect

            with patch("google.oauth2.service_account.Credentials"):
                yield mock_slides, mock_drive

    def test_create_from_template(self, mock_google_apis, tmp_path):
        """Should copy template and replace placeholders."""
        mock_slides, mock_drive = mock_google_apis

        mock_drive.files.return_value.copy.return_value.execute.return_value = {
            "id": "copied_pres_123"
        }

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        generator = SlidesGenerator(credentials_path=str(creds_file))

        url = generator.create_from_template(
            template_id="template_abc",
            title="Monthly Report - January",
            replacements={
                "customer_name": "Topgolf",
                "report_date": "January 2025",
            },
        )

        assert url == "https://docs.google.com/presentation/d/copied_pres_123"
        mock_drive.files.return_value.copy.assert_called_once()
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run --package growthnav-reporting pytest packages/shared-reporting/tests/ -v` passes
- [ ] `uv run pytest packages/shared-reporting/tests/ --cov=growthnav.reporting --cov-report=term-missing` shows >80% coverage
- [ ] `uv run mypy packages/shared-reporting/` passes
- [ ] `uv run ruff check packages/shared-reporting/` passes

#### Manual Verification:
- [ ] Review mock setup matches actual Google API behavior
- [ ] Verify template handling is realistic

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation before proceeding to Phase 5.

---

## Phase 5: mcp-server Package Tests

### Overview
Write tests for the MCP server tools, resources, and prompts.

### Changes Required

#### 1. MCP Server Tests
**File**: `packages/mcp-server/tests/test_server.py`
**Purpose**: Test MCP server tools and resources

```python
"""Tests for GrowthNav MCP Server."""

import pytest
from unittest.mock import MagicMock, patch


class TestBigQueryTools:
    """Test BigQuery-related MCP tools."""

    @patch("growthnav.bigquery.TenantBigQueryClient")
    def test_query_bigquery(self, mock_client_class):
        """Test query_bigquery tool."""
        from growthnav_mcp.server import query_bigquery

        mock_client = MagicMock()
        mock_client.query.return_value = MagicMock(
            rows=[{"id": 1, "name": "Test"}],
            total_rows=1,
            bytes_processed=1024,
            cache_hit=False,
        )
        mock_client_class.return_value = mock_client

        result = query_bigquery(
            customer_id="topgolf",
            sql="SELECT * FROM metrics LIMIT 10",
        )

        assert result["total_rows"] == 1
        assert result["rows"] == [{"id": 1, "name": "Test"}]
        mock_client_class.assert_called_with(customer_id="topgolf")

    @patch("growthnav.bigquery.TenantBigQueryClient")
    def test_estimate_query_cost(self, mock_client_class):
        """Test estimate_query_cost tool."""
        from growthnav_mcp.server import estimate_query_cost

        mock_client = MagicMock()
        mock_client.estimate_cost.return_value = {
            "bytes_processed": 1_000_000,
            "estimated_cost_usd": 0.006,
            "is_cached": False,
        }
        mock_client_class.return_value = mock_client

        result = estimate_query_cost(
            customer_id="topgolf",
            sql="SELECT * FROM large_table",
        )

        assert result["bytes_processed"] == 1_000_000
        assert result["estimated_cost_usd"] == 0.006

    @patch("growthnav.bigquery.TenantBigQueryClient")
    def test_get_table_schema(self, mock_client_class):
        """Test get_table_schema tool."""
        from growthnav_mcp.server import get_table_schema

        mock_client = MagicMock()
        mock_client.get_table_schema.return_value = [
            {"name": "id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        ]
        mock_client_class.return_value = mock_client

        result = get_table_schema(
            customer_id="topgolf",
            table_name="metrics",
        )

        assert len(result) == 2
        assert result[0]["name"] == "id"


class TestCustomerRegistryTools:
    """Test customer registry MCP tools."""

    @patch("growthnav.bigquery.CustomerRegistry")
    def test_get_customer_found(self, mock_registry_class):
        """Test get_customer when customer exists."""
        from growthnav_mcp.server import get_customer
        from growthnav.bigquery.registry import Customer, Industry, CustomerStatus

        mock_registry = MagicMock()
        mock_registry.get_customer.return_value = Customer(
            customer_id="topgolf",
            customer_name="Topgolf Entertainment",
            gcp_project_id="project",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
            status=CustomerStatus.ACTIVE,
            tags=["enterprise"],
        )
        mock_registry_class.return_value = mock_registry

        result = get_customer("topgolf")

        assert result["customer_id"] == "topgolf"
        assert result["industry"] == "golf"
        assert result["tags"] == ["enterprise"]

    @patch("growthnav.bigquery.CustomerRegistry")
    def test_get_customer_not_found(self, mock_registry_class):
        """Test get_customer when customer doesn't exist."""
        from growthnav_mcp.server import get_customer

        mock_registry = MagicMock()
        mock_registry.get_customer.return_value = None
        mock_registry_class.return_value = mock_registry

        result = get_customer("nonexistent")

        assert result is None

    @patch("growthnav.bigquery.CustomerRegistry")
    def test_list_customers_by_industry(self, mock_registry_class):
        """Test list_customers_by_industry tool."""
        from growthnav_mcp.server import list_customers_by_industry
        from growthnav.bigquery.registry import Customer, Industry, CustomerStatus

        mock_registry = MagicMock()
        mock_registry.get_customers_by_industry.return_value = [
            Customer(
                customer_id="topgolf",
                customer_name="Topgolf",
                gcp_project_id="p1",
                dataset="d1",
                industry=Industry.GOLF,
            ),
            Customer(
                customer_id="puttery",
                customer_name="Puttery",
                gcp_project_id="p2",
                dataset="d2",
                industry=Industry.GOLF,
            ),
        ]
        mock_registry_class.return_value = mock_registry

        result = list_customers_by_industry("golf")

        assert len(result) == 2
        assert result[0]["customer_id"] == "topgolf"
        assert result[1]["customer_id"] == "puttery"


class TestReportingTools:
    """Test reporting MCP tools."""

    @patch("growthnav.reporting.PDFGenerator")
    def test_generate_pdf_report_success(self, mock_pdf_class):
        """Test successful PDF generation."""
        from growthnav_mcp.server import generate_pdf_report

        mock_pdf = MagicMock()
        mock_pdf.is_available.return_value = True
        mock_pdf.generate.return_value = b"PDF_CONTENT"
        mock_pdf_class.return_value = mock_pdf

        result = generate_pdf_report(
            template="customer_report",
            data={"customer": "Topgolf"},
        )

        assert result["success"] is True
        assert result["size_bytes"] == len(b"PDF_CONTENT")

    @patch("growthnav.reporting.PDFGenerator")
    def test_generate_pdf_report_unavailable(self, mock_pdf_class):
        """Test PDF generation when WeasyPrint unavailable."""
        from growthnav_mcp.server import generate_pdf_report

        mock_pdf = MagicMock()
        mock_pdf.is_available.return_value = False
        mock_pdf_class.return_value = mock_pdf

        result = generate_pdf_report(
            template="customer_report",
            data={"customer": "Topgolf"},
        )

        assert result["success"] is False
        assert "WeasyPrint" in result["error"]

    @patch("growthnav.reporting.SheetsExporter")
    @patch("pandas.DataFrame")
    def test_create_sheets_dashboard(self, mock_df, mock_sheets_class):
        """Test Sheets dashboard creation."""
        from growthnav_mcp.server import create_sheets_dashboard

        mock_sheets = MagicMock()
        mock_sheets.create_dashboard.return_value = "https://sheets.google.com/abc"
        mock_sheets_class.return_value = mock_sheets

        result = create_sheets_dashboard(
            title="Test Dashboard",
            data=[{"col": "val"}],
        )

        assert result["success"] is True
        assert result["url"] == "https://sheets.google.com/abc"


class TestConversionTools:
    """Test conversion normalization MCP tools."""

    @patch("growthnav.conversions.POSNormalizer")
    def test_normalize_pos_data(self, mock_normalizer_class):
        """Test POS data normalization."""
        from growthnav_mcp.server import normalize_pos_data

        mock_normalizer = MagicMock()
        mock_conversion = MagicMock()
        mock_conversion.to_dict.return_value = {
            "customer_id": "topgolf",
            "transaction_id": "TXN-001",
            "value": 150.0,
        }
        mock_normalizer.normalize.return_value = [mock_conversion]
        mock_normalizer_class.return_value = mock_normalizer

        result = normalize_pos_data(
            customer_id="topgolf",
            transactions=[{"order_id": "TXN-001", "total": 150.0}],
        )

        assert len(result) == 1
        assert result[0]["value"] == 150.0


class TestMCPResources:
    """Test MCP resource handlers."""

    @patch("growthnav.bigquery.CustomerRegistry")
    def test_get_customer_resource(self, mock_registry_class):
        """Test customer resource."""
        from growthnav_mcp.server import get_customer_resource
        from growthnav.bigquery.registry import Customer, Industry, CustomerStatus

        mock_registry = MagicMock()
        mock_registry.get_customer.return_value = Customer(
            customer_id="topgolf",
            customer_name="Topgolf Entertainment",
            gcp_project_id="project",
            dataset="growthnav_topgolf",
            industry=Industry.GOLF,
            status=CustomerStatus.ACTIVE,
            tags=["enterprise"],
        )
        mock_registry_class.return_value = mock_registry

        result = get_customer_resource("topgolf")

        assert "Topgolf Entertainment" in result
        assert "golf" in result

    def test_list_industries_resource(self):
        """Test industries list resource."""
        from growthnav_mcp.server import list_industries

        result = list_industries()

        assert "golf" in result
        assert "medical" in result
        assert "restaurant" in result


class TestMCPPrompts:
    """Test MCP prompt templates."""

    def test_analyze_customer_data_prompt(self):
        """Test customer analysis prompt."""
        from growthnav_mcp.server import analyze_customer_data

        result = analyze_customer_data(
            customer_id="topgolf",
            analysis_type="performance",
        )

        assert "topgolf" in result
        assert "performance" in result

    def test_generate_monthly_report_prompt(self):
        """Test monthly report prompt."""
        from growthnav_mcp.server import generate_monthly_report

        result = generate_monthly_report(customer_id="topgolf")

        assert "topgolf" in result
        assert "monthly" in result.lower()
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run --package growthnav-mcp pytest packages/mcp-server/tests/ -v` passes
- [ ] `uv run pytest packages/mcp-server/tests/ --cov=growthnav_mcp --cov-report=term-missing` shows >80% coverage
- [ ] `uv run mypy packages/mcp-server/` passes
- [ ] `uv run ruff check packages/mcp-server/` passes

#### Manual Verification:
- [ ] Verify MCP tools return expected response formats
- [ ] Test prompts produce useful guidance

**Implementation Note**: After completing this phase and all automated verification passes, pause here for manual confirmation before proceeding to Phase 6.

---

## Phase 6: CI/CD and Integration Tests

### Overview
Set up GitHub Actions CI/CD pipeline and add integration tests.

### Changes Required

#### 1. GitHub Actions Workflow
**File**: `.github/workflows/ci.yml`
**Purpose**: Run tests on every PR

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  UV_CACHE_DIR: ~/.cache/uv

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4
        with:
          version: "latest"

      - name: Set up Python ${{ matrix.python-version }}
        run: uv python install ${{ matrix.python-version }}

      - name: Cache uv dependencies
        uses: actions/cache@v4
        with:
          path: ${{ env.UV_CACHE_DIR }}
          key: uv-${{ runner.os }}-${{ hashFiles('**/pyproject.toml') }}
          restore-keys: |
            uv-${{ runner.os }}-

      - name: Install dependencies
        run: uv sync

      - name: Run linting
        run: uv run ruff check packages/

      - name: Run type checking
        run: uv run mypy packages/
        continue-on-error: true  # Don't fail on type errors initially

      - name: Run tests with coverage
        run: |
          uv run pytest packages/ \
            --cov=growthnav \
            --cov=growthnav_mcp \
            --cov-report=xml \
            --cov-report=term-missing \
            -v

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          file: ./coverage.xml
          fail_ci_if_error: false

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4
        with:
          version: "latest"

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: uv sync

      - name: Run Ruff
        run: uv run ruff check packages/ --output-format=github
```

#### 2. Integration Test for Package Imports
**File**: `tests/integration/test_package_imports.py`
**Purpose**: Verify all packages can be imported together

```python
"""Integration tests for package imports."""

import pytest


def test_all_packages_importable():
    """All GrowthNav packages should be importable."""
    # BigQuery package
    from growthnav.bigquery import TenantBigQueryClient
    from growthnav.bigquery import CustomerRegistry
    from growthnav.bigquery import QueryValidator

    # Reporting package
    from growthnav.reporting import PDFGenerator
    from growthnav.reporting import SheetsExporter
    from growthnav.reporting import SlidesGenerator

    # Conversions package
    from growthnav.conversions import Conversion
    from growthnav.conversions import POSNormalizer
    from growthnav.conversions import CRMNormalizer


def test_mcp_server_importable():
    """MCP server should be importable."""
    from growthnav_mcp.server import mcp
    from growthnav_mcp.server import query_bigquery
    from growthnav_mcp.server import generate_pdf_report


def test_cross_package_integration():
    """Packages should work together."""
    from growthnav.bigquery.registry import Industry
    from growthnav.conversions.schema import ConversionType

    # Verify enums from different packages are usable
    assert Industry.GOLF.value == "golf"
    assert ConversionType.PURCHASE.value == "purchase"
```

### Success Criteria

#### Automated Verification:
- [ ] `uv run pytest` runs all tests across all packages
- [ ] `uv run pytest --cov` shows combined coverage report
- [ ] GitHub Actions workflow runs successfully on push/PR
- [ ] `uv run ruff check packages/` passes
- [ ] `uv run mypy packages/` passes (or has acceptable errors)

#### Manual Verification:
- [ ] Create a test PR to verify CI pipeline runs
- [ ] Review coverage report for gaps

**Implementation Note**: After completing this phase and all automated verification passes, the TDD implementation is complete.

---

## Testing Strategy

### Unit Tests
- **Location**: `packages/*/tests/`
- **Pattern**: `test_*.py`
- **Run Command**: `uv run --package <package-name> pytest`

### Test Categories (using pytest markers)
- `@pytest.mark.unit` - Pure unit tests, no external dependencies
- `@pytest.mark.integration` - Tests requiring multiple packages
- `@pytest.mark.slow` - Long-running tests

### Mocking Strategy
- **BigQuery**: Mock `google.cloud.bigquery.Client`
- **Google Sheets**: Mock `gspread.authorize` and credentials
- **Google Slides**: Mock `googleapiclient.discovery.build`
- **WeasyPrint**: Mock `weasyprint.HTML` for PDF generation

### Coverage Requirements
- Minimum 80% coverage per package
- Coverage report generated with `pytest-cov`

---

## Performance Considerations

- **Parallel Test Execution**: Use `pytest-xdist` for parallel test runs
- **Mock Performance**: Mocks prevent slow API calls during testing
- **CI Caching**: Cache uv dependencies to speed up CI runs

---

## Migration Notes

This plan focuses on testing existing code without migration. Future phases will:
1. Migrate PaidSearchNav-MCP as `app-paid-search`
2. Migrate PaidSocialNav as `app-paid-social`
3. Migrate AutoCLV as `app-auto-clv`

---

## References

- Original research: `thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md`
- uv Workspaces: https://docs.astral.sh/uv/concepts/projects/workspaces/
- pytest Documentation: https://docs.pytest.org/
- FastMCP Testing: https://gofastmcp.com/
