# Customer Onboarding Skill

## Overview

This skill guides you through the customer onboarding process for GrowthNav, including BigQuery dataset setup, customer registry registration, and credential management.

## When to Use This Skill

- Setting up a new customer in GrowthNav
- Creating BigQuery datasets for customer data
- Registering customers in the customer registry
- Storing customer credentials securely
- Offboarding inactive customers

## Prerequisites

Before onboarding a customer, gather the following information:

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| customer_id | Yes | Unique identifier | `topgolf`, `medcorp_a` |
| customer_name | Yes | Display name | `Topgolf Entertainment` |
| industry | Yes | Industry classification | `golf`, `medical`, `restaurant` |
| gcp_project_id | No* | GCP project for data | `growthnav-prod` |
| google_ads_customer_ids | No | Google Ads accounts | `["123-456-7890"]` |
| meta_ad_account_ids | No | Meta ad accounts | `["act_12345"]` |
| tags | No | Classification tags | `["enterprise", "q1_2025"]` |

*Required if no default project is configured

### Customer ID Format Rules

Customer IDs must follow these validation rules:

- **Allowed characters**: Lowercase letters, numbers, underscores only
- **Must start with**: A lowercase letter
- **Length**: 3-32 characters
- **Must be unique**: No duplicates in registry

**Valid examples**: `topgolf`, `medcorp_a`, `acme_corp_2025`
**Invalid examples**: `TopGolf` (uppercase), `123acme` (starts with number), `ab` (too short)

## Industry Classification

Choose the appropriate industry for cross-customer insights and benchmarking:

| Industry | Value | Description | Examples |
|----------|-------|-------------|----------|
| Golf | `golf` | Golf and entertainment venues | Topgolf, Puttery, Drive Shack |
| Medical | `medical` | Healthcare providers | Hospitals, clinics, dental |
| Restaurant | `restaurant` | Food service | QSR, casual dining, fine dining |
| Retail | `retail` | Brick-and-mortar retail | Department stores, specialty |
| E-commerce | `ecommerce` | Online retail | D2C brands, marketplaces |
| Other | `other` | Doesn't fit categories | Custom industries |

## MCP Tools for Onboarding

### Check if Customer Exists

```
get_customer(customer_id="new_customer")
```

Returns customer details or `null` if not found.

### List Existing Customers by Industry

```
list_customers_by_industry(industry="golf")
```

Returns list of all customers in that industry.

## Onboarding Workflow

### Step 1: Validate Customer Doesn't Exist

```python
# Check if customer already exists
existing = get_customer(customer_id="acme_corp")
if existing:
    print(f"Customer already exists: {existing['customer_name']}")
    # Decide whether to update or abort
```

### Step 2: Prepare Onboarding Request

```python
from growthnav.onboarding import OnboardingOrchestrator, OnboardingRequest
from growthnav.bigquery import Industry

request = OnboardingRequest(
    customer_id="acme_corp",
    customer_name="Acme Corporation",
    industry=Industry.ECOMMERCE,
    gcp_project_id="growthnav-prod",  # Optional if default is set
    google_ads_customer_ids=["123-456-7890"],
    meta_ad_account_ids=["act_12345678"],
    tags=["enterprise", "q1_2025"],
)
```

### Step 3: Execute Onboarding

```python
orchestrator = OnboardingOrchestrator(
    default_project_id="growthnav-prod"  # Fallback project
)

result = orchestrator.onboard(request)

if result.is_success:
    print(f"Customer onboarded successfully!")
    print(f"Dataset: {result.dataset_id}")
    print(f"Duration: {result.duration_seconds:.2f}s")
else:
    print(f"Onboarding failed: {result.errors}")
```

### Step 4: Store Credentials (Optional)

For customers with platform integrations:

```python
from growthnav.onboarding import CredentialStore

store = CredentialStore()

# Store Google Ads refresh token
store.store_credential(
    customer_id="acme_corp",
    credential_type="google_ads_refresh_token",
    credential_value="1//0x..."
)

# Store Meta access token
store.store_credential(
    customer_id="acme_corp",
    credential_type="meta_access_token",
    credential_value="EAAG..."
)
```

## Credential Security Best Practices

**Always validate credentials before storing them:**

### Validating Google Ads Credentials

```python
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def validate_google_ads_token(refresh_token: str, config: dict) -> bool:
    """Validate a Google Ads refresh token before storing."""
    try:
        client = GoogleAdsClient.load_from_dict({
            "refresh_token": refresh_token,
            "developer_token": config["developer_token"],
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        })
        customer_service = client.get_service("CustomerService")
        customer_service.list_accessible_customers()
        return True
    except GoogleAdsException as e:
        print(f"❌ Invalid Google Ads credential: {e.failure.errors[0].message}")
        return False

# Validate before storing
if validate_google_ads_token(refresh_token, ads_config):
    store.store_credential("acme_corp", "google_ads_refresh_token", refresh_token)
else:
    print("Credential validation failed - not storing")
```

### Validating Meta Credentials

```python
import requests

def validate_meta_token(access_token: str) -> bool:
    """Validate a Meta access token before storing."""
    try:
        response = requests.get(
            "https://graph.facebook.com/v18.0/me",
            params={"access_token": access_token}
        )
        if response.status_code == 200:
            return True
        print(f"❌ Invalid Meta credential: {response.json().get('error', {}).get('message')}")
        return False
    except Exception as e:
        print(f"❌ Meta validation error: {e}")
        return False
```

### Security Guidelines

| Guideline | Description |
|-----------|-------------|
| **Validate First** | Always test credentials work before saving |
| **Rotation Policy** | Rotate credentials every 90 days |
| **Least Privilege** | Request only minimum required OAuth scopes |
| **Audit Logging** | All Secret Manager access is logged to Cloud Audit Logs |
| **No Logging Values** | Never log credential values, only success/failure |

## Error Recovery

The `OnboardingOrchestrator` includes automatic rollback on failure. If onboarding fails partway through:

### Automatic Rollback Behavior

| Step Failed | Rollback Actions |
|-------------|------------------|
| Dataset creation | None needed |
| Registry update | Dataset deleted if created |
| Credential storage | Customer marked inactive, dataset kept for investigation |

### Manual Recovery

If automatic rollback fails or you need to clean up manually:

```python
# Check what was created
customer = get_customer("acme_corp")
if customer:
    print(f"Registry entry exists: {customer['status']}")

# Clean up orphaned resources
orchestrator = OnboardingOrchestrator()
result = orchestrator.offboard("acme_corp", delete_data=True)

# Retry with same or different customer_id
# If same ID: ensure full cleanup first
# If different ID: proceed with new onboarding
```

### Investigating Failures

```python
result = orchestrator.onboard(request)

if not result.is_success:
    print(f"Failed at step: {result.failed_step}")
    print(f"Error: {result.errors}")
    print(f"Completed steps: {result.completed_steps}")
    print(f"Resources created: {result.created_resources}")
```

## What Gets Created

When a customer is onboarded:

### 1. BigQuery Dataset

```
project: growthnav-prod
├── growthnav_acme_corp/           # New customer dataset
│   ├── conversions                # Unified conversion data
│   └── daily_metrics              # Aggregated daily metrics
```

### 2. Customer Registry Entry

```sql
SELECT * FROM growthnav_registry.customers
WHERE customer_id = 'acme_corp'
```

### 3. Secret Manager Entries (if credentials provided)

```
growthnav-acme_corp-google_ads_refresh_token
growthnav-acme_corp-meta_access_token
```

## Verification Checklist

After onboarding, verify the following:

### Automated Checks

```python
# 1. Customer appears in registry
customer = get_customer("acme_corp")
assert customer is not None
assert customer["status"] == "active"

# 2. Customer appears in industry list
golf_customers = list_customers_by_industry("ecommerce")
assert any(c["customer_id"] == "acme_corp" for c in golf_customers)

# 3. Can query the dataset
result = query_bigquery(
    customer_id="acme_corp",
    sql="SELECT 1 as test"
)
assert result["rows"]
```

### Manual Checks

- [ ] Dataset visible in BigQuery console
- [ ] Tables have correct schema
- [ ] Credentials accessible (if stored)
- [ ] Customer can be used in reporting tools

## Offboarding Customers

To offboard a customer (mark as inactive):

```python
orchestrator = OnboardingOrchestrator()

# Mark as inactive but keep data
result = orchestrator.offboard("acme_corp")

# Mark as inactive AND delete data (destructive!)
result = orchestrator.offboard("acme_corp", delete_data=True)
```

## Troubleshooting

### "Customer already exists"

The customer_id must be unique. Options:
1. Use a different customer_id
2. Update the existing customer instead
3. Offboard the existing customer first

### "Permission denied" on dataset creation

1. Verify service account has BigQuery Admin role
2. Check project_id is correct
3. Ensure quotas haven't been exceeded

### "Invalid Google Ads customer ID format"

Google Ads IDs must be in format: `XXX-XXX-XXXX`
- Valid: `123-456-7890`
- Invalid: `1234567890`, `123-4567-890`

### "Invalid Meta ad account ID format"

Meta account IDs must start with `act_`:
- Valid: `act_12345678`
- Invalid: `12345678`, `account_12345`

### Registry update fails

1. Ensure registry table exists in BigQuery
2. Verify service account has write access
3. Check customer data passes validation

## Customer Status Lifecycle

```
┌──────────┐     ┌────────────┐     ┌─────────┐
│ onboard  │────▶│  ACTIVE    │────▶│ offboard│
└──────────┘     └────────────┘     └─────────┘
                       │                  │
                       │                  ▼
                       │           ┌──────────┐
                       └──────────▶│ INACTIVE │
                                   └──────────┘
```

## Related Skills

- **analytics-reporting**: Generate reports for customers
- **bigquery-best-practices**: Query customer data efficiently

## IAM Requirements

The service account running onboarding operations requires specific permissions:

### Required Roles

| Resource | Role | Purpose |
|----------|------|---------|
| BigQuery | `roles/bigquery.admin` | Create datasets and tables |
| Secret Manager | `roles/secretmanager.admin` | Store customer credentials |
| Registry Dataset | `roles/bigquery.dataEditor` | Update customer registry |

### Least Privilege Alternative

For production, use these granular permissions instead of admin roles:

**BigQuery Permissions**:
- `bigquery.datasets.create` - Create customer datasets
- `bigquery.datasets.get` - Read dataset metadata
- `bigquery.datasets.delete` - Rollback on failure
- `bigquery.tables.create` - Create standard tables
- `bigquery.tables.updateData` - Update registry table

**Secret Manager Permissions**:
- `secretmanager.secrets.create` - Create new secrets
- `secretmanager.versions.add` - Add secret versions
- `secretmanager.versions.access` - Read secrets (for validation)
- `secretmanager.secrets.delete` - Clean up on offboarding

### Setting Up Permissions

```bash
# Create service account
gcloud iam service-accounts create growthnav-onboarding \
  --display-name="GrowthNav Onboarding Service"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding growthnav-prod \
  --member="serviceAccount:growthnav-onboarding@growthnav-prod.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"

# Grant Secret Manager permissions
gcloud projects add-iam-policy-binding growthnav-prod \
  --member="serviceAccount:growthnav-onboarding@growthnav-prod.iam.gserviceaccount.com" \
  --role="roles/secretmanager.admin"
```

### Verifying Permissions

```python
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden

def check_bigquery_permissions(project_id: str) -> bool:
    """Verify service account has required BigQuery permissions."""
    try:
        client = bigquery.Client(project=project_id)
        # Try to list datasets (requires bigquery.datasets.list)
        list(client.list_datasets(max_results=1))
        return True
    except Forbidden as e:
        print(f"❌ Missing BigQuery permissions: {e}")
        return False
```

## API Rate Limits

Be aware of these rate limits when onboarding multiple customers:

| Service | Limit | Notes |
|---------|-------|-------|
| BigQuery Dataset Creation | 50/minute | Per project |
| Secret Manager | 1,800 requests/minute | Per project |
| Customer Registry (BigQuery) | 100 concurrent queries | Per project |

**Recommendation**: When bulk onboarding, add 1-2 second delays between customers.

## Related MCP Resources

- `customer://{customer_id}/config` - Get customer configuration
- `industries://list` - List all available industries
