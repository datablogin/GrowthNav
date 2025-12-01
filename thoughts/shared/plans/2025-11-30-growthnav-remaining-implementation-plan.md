# GrowthNav Remaining Implementation Plan

## Overview

This plan addresses the remaining components needed to fulfill the vision from the original research document (`thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md`). The TDD implementation plan (`thoughts/shared/plans/2025-11-28-growthnav-tdd-implementation-plan.md`) has been largely completed, and this plan covers what remains.

## Current State Analysis

### Completed Components

| Component | Status | Notes |
|-----------|--------|-------|
| shared-bigquery | **Implemented + Tested** | TenantBigQueryClient, CustomerRegistry, QueryValidator with 80%+ test coverage |
| shared-reporting | **Implemented + Tested** | PDFGenerator, SheetsExporter, SlidesGenerator, HTMLRenderer with integration tests |
| shared-conversions | **Implemented + Tested** | Conversion schema, POSNormalizer, CRMNormalizer, LoyaltyNormalizer, AttributionEngine |
| mcp-server | **Implemented + Tested** | FastMCP server with tools, resources, prompts |
| CI/CD Pipeline | **Implemented** | GitHub Actions with linting, type checking, tests, coverage, GCP auth |
| Root conftest.py | **Implemented** | Shared test fixtures |
| Integration tests | **Implemented** | BigQuery, Sheets, Slides integration tests |

### Test Status Summary
- **375 tests collected**
- **334 passed** (unit tests all pass)
- **18 failed** (BigQuery integration tests - require GCP credentials)
- **23 skipped** (Sheets/Slides integration tests - require env vars)

### Missing Components from Research Vision

| Component | Priority | Status | Notes |
|-----------|----------|--------|-------|
| shared-onboarding package | P1 | **Empty directories only** | Was in original vision as `growthnav.onboarding` |
| Claude Skills content | P2 | **Empty directories only** | 3 skill directories exist but no SKILL.md files |
| App migrations | P3 | **Not started** | PaidSearchNav, PaidSocialNav, AutoCLV not migrated |
| Report templates | P2 | **Not verified** | Jinja2 templates for reporting |

## What We're NOT Doing

- **Not migrating existing apps** - PaidSearchNav-MCP, PaidSocialNav, AutoCLV migration is out of scope
- **Not adding new features** to existing packages
- **Not fixing integration test failures** - These require GCP credentials configuration, not code changes

## Implementation Approach

Focus on the two remaining gaps that provide the most value:
1. **shared-onboarding package** - Critical for customer lifecycle management
2. **Claude Skills** - Enable AI-assisted workflows

---

## Phase 1: shared-onboarding Package

### Overview
Create the customer onboarding orchestration package as specified in the research document.

### Changes Required

#### 1. Package Structure
**Directory**: `packages/shared-onboarding/`

```
packages/shared-onboarding/
├── pyproject.toml
├── growthnav/
│   └── onboarding/
│       ├── __init__.py
│       ├── orchestrator.py      # OnboardingOrchestrator
│       ├── provisioning.py      # DatasetProvisioner
│       └── secrets.py           # SecretManager integration
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_orchestrator.py
    ├── test_provisioning.py
    └── test_secrets.py
```

#### 2. pyproject.toml
**File**: `packages/shared-onboarding/pyproject.toml`

```toml
[project]
name = "growthnav-onboarding"
version = "0.1.0"
description = "Customer onboarding orchestration for GrowthNav"
requires-python = ">=3.11"
dependencies = [
    "growthnav-bigquery",
    "google-cloud-secret-manager>=2.16.0",
    "pydantic>=2.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["growthnav"]
```

#### 3. OnboardingOrchestrator
**File**: `packages/shared-onboarding/growthnav/onboarding/orchestrator.py`

Key functionality:
- Validate customer input data
- Create BigQuery dataset via TenantBigQueryClient
- Store customer in CustomerRegistry
- Store credentials in Secret Manager
- Return onboarding status

```python
"""Customer onboarding orchestrator."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from growthnav.bigquery import CustomerRegistry, Customer, Industry


class OnboardingStatus(str, Enum):
    """Status of customer onboarding."""
    PENDING = "pending"
    PROVISIONING = "provisioning"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class OnboardingRequest:
    """Request to onboard a new customer."""
    customer_id: str
    customer_name: str
    industry: Industry
    gcp_project_id: Optional[str] = None
    google_ads_customer_ids: Optional[list[str]] = None
    meta_ad_account_ids: Optional[list[str]] = None
    tags: Optional[list[str]] = None


@dataclass
class OnboardingResult:
    """Result of customer onboarding."""
    status: OnboardingStatus
    customer: Optional[Customer] = None
    dataset_id: Optional[str] = None
    error: Optional[str] = None


class OnboardingOrchestrator:
    """Orchestrates customer onboarding workflow."""

    def __init__(
        self,
        registry: Optional[CustomerRegistry] = None,
        provisioner: Optional["DatasetProvisioner"] = None,
    ):
        self.registry = registry or CustomerRegistry()
        self.provisioner = provisioner

    def onboard(self, request: OnboardingRequest) -> OnboardingResult:
        """
        Onboard a new customer.

        Steps:
        1. Validate request
        2. Check customer doesn't already exist
        3. Create BigQuery dataset
        4. Register customer in registry
        5. Store any credentials
        """
        # Implementation details...
        pass

    def validate_request(self, request: OnboardingRequest) -> list[str]:
        """Validate onboarding request, return list of errors."""
        errors = []
        if not request.customer_id:
            errors.append("customer_id is required")
        if not request.customer_name:
            errors.append("customer_name is required")
        # Additional validation...
        return errors
```

#### 4. DatasetProvisioner
**File**: `packages/shared-onboarding/growthnav/onboarding/provisioning.py`

```python
"""BigQuery dataset provisioning for new customers."""

from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery


@dataclass
class ProvisioningConfig:
    """Configuration for dataset provisioning."""
    project_id: str
    location: str = "US"
    default_table_expiration_ms: Optional[int] = None


class DatasetProvisioner:
    """Provisions BigQuery datasets for new customers."""

    def __init__(self, config: Optional[ProvisioningConfig] = None):
        self.config = config or ProvisioningConfig.from_env()
        self._client: Optional[bigquery.Client] = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy-initialize BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.config.project_id)
        return self._client

    def create_dataset(self, customer_id: str) -> str:
        """
        Create a new dataset for a customer.

        Returns the full dataset ID.
        """
        dataset_id = f"growthnav_{customer_id}"
        dataset = bigquery.Dataset(f"{self.config.project_id}.{dataset_id}")
        dataset.location = self.config.location

        self.client.create_dataset(dataset, exists_ok=True)
        return f"{self.config.project_id}.{dataset_id}"

    def create_standard_tables(self, dataset_id: str) -> list[str]:
        """Create standard tables for a customer dataset."""
        # Tables like conversions, metrics, etc.
        pass
```

#### 5. SecretManager Integration
**File**: `packages/shared-onboarding/growthnav/onboarding/secrets.py`

```python
"""Secret Manager integration for credential storage."""

from dataclasses import dataclass
from typing import Optional

from google.cloud import secretmanager


@dataclass
class CredentialStore:
    """Stores and retrieves customer credentials."""

    project_id: str
    _client: Optional[secretmanager.SecretManagerServiceClient] = None

    @property
    def client(self) -> secretmanager.SecretManagerServiceClient:
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def store_credential(
        self,
        customer_id: str,
        credential_type: str,
        credential_value: str,
    ) -> str:
        """
        Store a credential in Secret Manager.

        Returns the secret version name.
        """
        secret_id = f"growthnav-{customer_id}-{credential_type}"
        parent = f"projects/{self.project_id}"

        # Create secret if it doesn't exist
        try:
            self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )
        except Exception:
            pass  # Secret already exists

        # Add new version
        response = self.client.add_secret_version(
            request={
                "parent": f"{parent}/secrets/{secret_id}",
                "payload": {"data": credential_value.encode()},
            }
        )
        return response.name

    def get_credential(
        self,
        customer_id: str,
        credential_type: str,
    ) -> Optional[str]:
        """Retrieve a credential from Secret Manager."""
        secret_id = f"growthnav-{customer_id}-{credential_type}"
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"

        try:
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode()
        except Exception:
            return None
```

#### 6. Package __init__.py
**File**: `packages/shared-onboarding/growthnav/onboarding/__init__.py`

```python
"""GrowthNav Customer Onboarding Package."""

from growthnav.onboarding.orchestrator import (
    OnboardingOrchestrator,
    OnboardingRequest,
    OnboardingResult,
    OnboardingStatus,
)
from growthnav.onboarding.provisioning import (
    DatasetProvisioner,
    ProvisioningConfig,
)
from growthnav.onboarding.secrets import CredentialStore

__all__ = [
    "OnboardingOrchestrator",
    "OnboardingRequest",
    "OnboardingResult",
    "OnboardingStatus",
    "DatasetProvisioner",
    "ProvisioningConfig",
    "CredentialStore",
]
```

### Success Criteria

#### Automated Verification:
- [x] `uv sync` includes the new package
- [x] `uv run --package growthnav-onboarding pytest packages/shared-onboarding/tests/ -v` passes
- [x] `uv run pytest packages/shared-onboarding/tests/ --cov=growthnav.onboarding` shows >80% coverage (98%)
- [x] `uv run mypy packages/shared-onboarding/` passes
- [x] `uv run ruff check packages/shared-onboarding/` passes

#### Manual Verification:
- [x] Onboarding workflow can be triggered manually with test data
- [x] Dataset is created in BigQuery (requires GCP access)

---

## Phase 2: Claude Skills Content

### Overview
Create the SKILL.md files for the three skill directories that were created but left empty.

### Changes Required

#### 1. Analytics Reporting Skill
**File**: `.claude/skills/analytics-reporting/SKILL.md`

```markdown
# Analytics Reporting Skill

## Overview
This skill guides you through creating analytics reports for GrowthNav customers using the shared reporting infrastructure.

## When to Use This Skill
- Generating PDF reports for customer presentations
- Creating Google Sheets dashboards for data analysis
- Building Google Slides presentations for executive summaries
- Exporting data in various formats (HTML, PDF, Sheets, Slides)

## Available Tools

### PDF Generation
Use the `generate_pdf_report` MCP tool:
```
generate_pdf_report(
    template="customer_report",
    data={"customer": "...", "metrics": {...}},
    output_path="/path/to/output.pdf"  # optional
)
```

### Google Sheets Dashboard
Use the `create_sheets_dashboard` MCP tool:
```
create_sheets_dashboard(
    title="Monthly Performance Dashboard",
    data=[...],  # list of dicts or DataFrame
    share_with=["analyst@company.com"]
)
```

### Google Slides Presentation
Use the `create_slides_presentation` MCP tool:
```
create_slides_presentation(
    title="Q4 Executive Summary",
    slides=[
        {"title": "Overview", "body": "Key highlights..."},
        {"title": "Metrics", "body": ["Point 1", "Point 2"]}
    ],
    share_with=["exec@company.com"]
)
```

## Best Practices

### Report Templates
1. Always use existing templates from `growthnav.reporting.templates/`
2. Keep consistent branding across reports
3. Include data freshness timestamps

### Google Workspace Integration
1. Share reports with appropriate permission levels (viewer vs editor)
2. Use folder_id parameter to organize reports in Drive
3. Consider rate limits when creating multiple reports

### Data Preparation
1. Query BigQuery for fresh data before generating reports
2. Use the `query_bigquery` tool with appropriate LIMIT clauses
3. Validate data before rendering to avoid empty reports

## Example Workflow

### Monthly Customer Report
1. Query customer metrics from BigQuery
2. Normalize data for report template
3. Generate PDF report
4. Create Sheets dashboard with raw data
5. Share both with customer stakeholders

```python
# Step 1: Query data
metrics = query_bigquery(
    customer_id="topgolf",
    sql="SELECT * FROM monthly_metrics WHERE month = '2025-01'"
)

# Step 2: Generate PDF
pdf_result = generate_pdf_report(
    template="monthly_summary",
    data={"customer": "Topgolf", "metrics": metrics}
)

# Step 3: Create dashboard
sheets_url = create_sheets_dashboard(
    title="Topgolf - January 2025 Data",
    data=metrics,
    share_with=["analyst@topgolf.com"]
)
```

## Troubleshooting

### PDF Generation Fails
- Check if WeasyPrint is installed (required for PDF)
- Verify template exists in templates directory
- Check data matches template variables

### Sheets/Slides Permission Errors
- Verify service account has appropriate permissions
- Check if credentials are configured in environment

### Empty Reports
- Verify BigQuery query returns data
- Check date ranges in queries
- Confirm customer_id is correct
```

#### 2. Customer Onboarding Skill
**File**: `.claude/skills/customer-onboarding/SKILL.md`

```markdown
# Customer Onboarding Skill

## Overview
This skill guides you through the customer onboarding process for GrowthNav, including BigQuery dataset setup, customer registry, and credential management.

## When to Use This Skill
- Setting up a new customer in GrowthNav
- Creating BigQuery datasets for customer data
- Registering customers in the customer registry
- Storing customer credentials securely

## Prerequisites

Before onboarding a customer, gather:
1. **Customer ID**: Unique identifier (e.g., "topgolf", "medcorp_a")
2. **Customer Name**: Display name (e.g., "Topgolf Entertainment")
3. **Industry**: One of: golf, medical, restaurant, retail, ecommerce, other
4. **Platform IDs** (optional):
   - Google Ads Customer IDs (format: "123-456-7890")
   - Meta Ad Account IDs (format: "act_12345")

## Onboarding Workflow

### Step 1: Validate Customer Doesn't Exist
```python
customer = get_customer(customer_id="new_customer")
if customer:
    print("Customer already exists!")
```

### Step 2: Create BigQuery Dataset
The dataset follows the pattern: `growthnav_{customer_id}`
```python
# Dataset will be created at: project.growthnav_new_customer
```

### Step 3: Register Customer
```python
from growthnav.bigquery import CustomerRegistry, Customer, Industry

registry = CustomerRegistry()
customer = Customer(
    customer_id="new_customer",
    customer_name="New Customer Inc",
    gcp_project_id="growthnav-prod",
    dataset="growthnav_new_customer",
    industry=Industry.GOLF,
    google_ads_customer_ids=["123-456-7890"],
    meta_ad_account_ids=["act_12345"],
    tags=["enterprise", "q1_2025"],
)
registry.add_customer(customer)
```

### Step 4: Store Credentials (if needed)
For customers with platform integrations:
```python
from growthnav.onboarding import CredentialStore

store = CredentialStore(project_id="growthnav-prod")
store.store_credential(
    customer_id="new_customer",
    credential_type="google_ads_refresh_token",
    credential_value="1//0x..."
)
```

## Industry Classification

Choose the appropriate industry for cross-customer insights:

| Industry | Description | Examples |
|----------|-------------|----------|
| golf | Golf and entertainment venues | Topgolf, Puttery |
| medical | Healthcare providers | Hospitals, clinics |
| restaurant | Food service | QSR, fine dining |
| retail | Brick-and-mortar retail | Department stores |
| ecommerce | Online retail | D2C brands |
| other | Doesn't fit categories | Custom industries |

## Dataset Structure

Each customer dataset includes standard tables:
- `conversions` - Unified conversion data
- `metrics` - Aggregated performance metrics
- `campaigns` - Campaign-level data (if applicable)

## Verification Checklist

After onboarding, verify:
- [ ] Customer appears in registry: `get_customer("customer_id")`
- [ ] Dataset exists in BigQuery console
- [ ] Customer can be queried by industry: `list_customers_by_industry("industry")`
- [ ] Credentials are accessible (if stored)

## Troubleshooting

### "Customer already exists"
- Customer ID must be unique
- Check if customer was previously onboarded

### "Permission denied" on dataset creation
- Verify service account has BigQuery Admin role
- Check project_id is correct

### Registry update fails
- Ensure registry table exists
- Verify service account has write access
```

#### 3. BigQuery Best Practices Skill
**File**: `.claude/skills/bigquery-best-practices/SKILL.md`

```markdown
# BigQuery Best Practices Skill

## Overview
This skill provides guidance on using BigQuery effectively within GrowthNav, including query optimization, cost management, and tenant isolation.

## When to Use This Skill
- Writing BigQuery queries for customer data
- Optimizing query performance and cost
- Understanding tenant isolation patterns
- Troubleshooting query issues

## Tenant Isolation Pattern

GrowthNav uses dataset-per-customer isolation:
```
project: growthnav-prod
├── growthnav_topgolf/      # Topgolf's data
├── growthnav_puttery/      # Puttery's data
├── growthnav_medcorp_a/    # MedCorp A's data
└── growthnav_registry/     # Customer registry (shared)
```

### Always Use TenantBigQueryClient
```python
from growthnav.bigquery import TenantBigQueryClient

# Client automatically scopes to customer's dataset
client = TenantBigQueryClient(customer_id="topgolf")

# Queries run against growthnav_topgolf dataset
result = client.query("SELECT * FROM conversions LIMIT 10")
```

## Query Safety

### Automatic Validation
The QueryValidator blocks destructive operations:
- DROP, DELETE, TRUNCATE - **Blocked**
- INSERT, UPDATE, MERGE - **Blocked** (unless `allow_writes=True`)
- CREATE, ALTER - **Blocked**

### Warnings
You'll receive warnings for:
- `SELECT *` - Prefer specific columns
- Missing `LIMIT` - Always limit result sets

## Cost Optimization

### Check Cost Before Querying
```python
estimate = client.estimate_cost(sql)
print(f"This query will scan {estimate['bytes_processed']:,} bytes")
print(f"Estimated cost: ${estimate['estimated_cost_usd']:.4f}")
```

### Cost-Saving Patterns

1. **Use column projection**
   ```sql
   -- Good: Only select needed columns
   SELECT customer_id, value, timestamp FROM conversions

   -- Bad: Scans all columns
   SELECT * FROM conversions
   ```

2. **Partition pruning**
   ```sql
   -- Good: Filter on partition column
   SELECT * FROM conversions
   WHERE DATE(timestamp) = '2025-01-15'

   -- Bad: Function on partition column prevents pruning
   SELECT * FROM conversions
   WHERE EXTRACT(MONTH FROM timestamp) = 1
   ```

3. **Use LIMIT for exploration**
   ```sql
   -- Always limit during development
   SELECT * FROM large_table LIMIT 100
   ```

## Common Patterns

### Cross-Customer Industry Analysis
```python
from growthnav.bigquery import CustomerRegistry, Industry

registry = CustomerRegistry()
golf_customers = registry.get_customers_by_industry(Industry.GOLF)

# Query each customer's dataset for industry benchmarks
for customer in golf_customers:
    client = TenantBigQueryClient(customer_id=customer.customer_id)
    # Aggregate metrics...
```

### Parameterized Queries
```python
# Safe: Uses parameterized queries
result = client.query(
    "SELECT * FROM conversions WHERE value > @min_value",
    params={"min_value": 100.0}
)

# Unsafe: String interpolation (SQL injection risk)
result = client.query(f"SELECT * FROM conversions WHERE value > {min_value}")
```

### Async Queries for Large Results
```python
import asyncio

async def get_large_dataset():
    result = await client.query_async(
        "SELECT * FROM large_table",
        max_results=100000
    )
    return result
```

## Troubleshooting

### "Access Denied" Errors
- Verify customer_id is correct
- Check service account has access to dataset
- Confirm dataset exists

### Slow Queries
- Check if query uses partition pruning
- Reduce columns in SELECT
- Add appropriate WHERE filters
- Consider materialized views for common queries

### High Costs
- Always use estimate_cost() before large queries
- Use LIMIT during development
- Prefer column projection over SELECT *
- Filter on partitioned/clustered columns

## Performance Tips

1. **Clustering**: Tables are clustered by common filter columns
2. **Partitioning**: Time-based tables partition by date
3. **Caching**: Queries cache for 24 hours (check `cache_hit` in results)
4. **Slots**: On-demand pricing, no slot reservations needed
```

### Success Criteria

#### Automated Verification:
- [x] All three SKILL.md files exist and are non-empty
- [x] Files follow markdown formatting correctly
- [x] No syntax errors in code examples

#### Manual Verification:
- [ ] Skills appear in Claude Desktop/Code skill list
- [ ] Code examples in skills are accurate and runnable
- [ ] Workflows described match actual tool capabilities

---

## Phase 3: Update Workspace Configuration

### Overview
Add the shared-onboarding package to the workspace.

### Changes Required

#### 1. Update root pyproject.toml
The workspace members pattern `"packages/shared-*"` will automatically include `shared-onboarding`.

Verify with:
```bash
uv sync
uv pip list | grep growthnav
```

### Success Criteria

#### Automated Verification:
- [x] `uv sync` completes without errors
- [x] `uv run python -c "from growthnav.onboarding import OnboardingOrchestrator"` works
- [x] All 5 packages appear in `uv pip list` (bigquery, conversions, mcp, onboarding, reporting)

---

## Testing Strategy

### Unit Tests for Onboarding Package
- Mock BigQuery client for dataset creation
- Mock Secret Manager client for credential storage
- Test validation logic thoroughly
- Test error handling for each step

### Integration Tests
- Skip by default (require GCP credentials)
- Enable via `RUN_ONBOARDING_INTEGRATION_TESTS=1`

---

## References

- Original research: `thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md`
- TDD Implementation plan: `thoughts/shared/plans/2025-11-28-growthnav-tdd-implementation-plan.md`
- uv Workspaces: https://docs.astral.sh/uv/concepts/projects/workspaces/
- Google Cloud Secret Manager: https://cloud.google.com/secret-manager/docs
