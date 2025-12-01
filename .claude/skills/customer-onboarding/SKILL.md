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

## Related MCP Resources

- `customer://{customer_id}/config` - Get customer configuration
- `industries://list` - List all available industries
