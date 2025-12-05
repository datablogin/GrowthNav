---
date: 2025-12-05T10:30:00-06:00
researcher: Robert Welborn
git_commit: 92959e93261205c38f43519f510da3c02ddf0958
branch: main
repository: GrowthNav
topic: "Client Onboarding and External Data Integration Architecture for CLV Analytics"
tags: [research, onboarding, integration, connectors, clv, pos, crm, data-lakes]
status: complete
last_updated: 2025-12-05
last_updated_by: Robert Welborn
---

# Research: Client Onboarding and External Data Integration Architecture

**Date**: 2025-12-05T10:30:00-06:00
**Researcher**: Robert Welborn
**Git Commit**: 92959e93261205c38f43519f510da3c02ddf0958
**Branch**: main
**Repository**: GrowthNav

## Research Question

Review the deep research on automating CLV data integration across data lakes (`thoughts/Automating CLV Data Integration Across Data Lakes.md`) and the current shared analytics infrastructure architecture (`thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md`). Determine:

1. Should we consider a new path for building client onboarding?
2. What should we build?
3. What should we import?
4. How can we make external connections (POS, CRM, online ordering, marketing platforms) easier for CLV and conversion tracking?

---

## Summary

After comprehensive analysis of both the CLV integration research document and the current GrowthNav codebase, **the existing architecture provides a solid foundation** but requires **strategic extensions** to address the sophisticated data integration challenges described in the CLV research. The key gap is not in the onboarding workflow itself, but in **automated semantic discovery and field mapping** for heterogeneous data sources.

### Key Recommendation

**Hybrid Approach**: Enhance the existing `shared-onboarding` and `shared-conversions` packages with a new **`shared-connectors`** package that provides:

1. **Connector Adapters** for common platforms (Square, Toast, Salesforce, HubSpot, Shopify)
2. **Semantic Field Mapping** using LLM-assisted or heuristic-based schema matching
3. **Identity Graph Foundation** for probabilistic customer resolution across systems

---

## Current State: What Already Exists

### Implemented Onboarding Infrastructure

The GrowthNav codebase already has a comprehensive onboarding system:

| Component | Location | Capabilities |
|-----------|----------|--------------|
| **OnboardingOrchestrator** | [orchestrator.py](packages/shared-onboarding/growthnav/onboarding/orchestrator.py) | Multi-step workflow with rollback, validation, status tracking |
| **DatasetProvisioner** | [provisioning.py](packages/shared-onboarding/growthnav/onboarding/provisioning.py) | BigQuery dataset creation, standard table schemas |
| **CredentialStore** | [secrets.py](packages/shared-onboarding/growthnav/onboarding/secrets.py) | Secret Manager integration for API tokens |
| **CustomerRegistry** | [registry.py](packages/shared-bigquery/growthnav/bigquery/registry.py) | Customer metadata, industry classification, platform IDs |

### Implemented Conversion Normalization

The `shared-conversions` package provides platform-agnostic data normalization:

| Component | Location | Capabilities |
|-----------|----------|--------------|
| **Conversion Schema** | [schema.py](packages/shared-conversions/growthnav/conversions/schema.py) | Unified conversion model with attribution fields |
| **POSNormalizer** | [normalizer.py:64-171](packages/shared-conversions/growthnav/conversions/normalizer.py) | Field mapping for Square, Toast, Lightspeed, Clover |
| **CRMNormalizer** | [normalizer.py:174-263](packages/shared-conversions/growthnav/conversions/normalizer.py) | Field mapping for Salesforce, HubSpot |
| **LoyaltyNormalizer** | [normalizer.py:266-344](packages/shared-conversions/growthnav/conversions/normalizer.py) | Field mapping for loyalty program data |
| **AttributionEngine** | [attribution.py](packages/shared-conversions/growthnav/conversions/attribution.py) | Multi-touch attribution models |

### What's Working Well

1. **Tenant Isolation**: Dataset-per-customer pattern (`growthnav_{customer_id}`) is robust
2. **Onboarding Workflow**: Multi-step orchestration with rollback is production-ready
3. **Field Mapping Pattern**: Default field maps handle common POS/CRM systems
4. **Unified Schema**: `Conversion` dataclass captures all CLV-required fields (RFM components)
5. **Attribution**: Multiple attribution models implemented (last-click, first-click, linear, time-decay, position-based)

---

## Gap Analysis: CLV Research vs. Current Implementation

### The CLV Research Vision

The deep research document (`thoughts/Automating CLV Data Integration Across Data Lakes.md`) describes a sophisticated system with:

| Capability | Research Vision | Current State | Gap |
|------------|-----------------|---------------|-----|
| **Semantic Type Detection** | Sherlock/Sato deep learning models | Manual field mapping dictionaries | No automated discovery |
| **LLM Schema Matching** | RAG-based column-to-schema mapping | N/A | Not implemented |
| **Probabilistic Identity Resolution** | Splink/Zingg entity matching | N/A | Not implemented |
| **Identity Graph** | Graph database for cross-system linkage | N/A | Not implemented |
| **Walled Garden Integration** | Google Customer Match, Meta CAPI | Click IDs captured but not sent | Partial |
| **Media Mix Modeling** | Robyn/Meridian integration | N/A | Not implemented |

### Critical Gaps

1. **No Automated Schema Discovery**: Current normalizers require explicit field mappings. The research envisions automatic detection of "what columns mean" based on data profiling.

2. **No Identity Resolution**: Current system assumes a single `user_id` per conversion. Real-world data has fragmented identities (hashed CC, email, phone, loyalty ID).

3. **No API Connectors**: Current system expects pre-transformed data. No direct API integration with POS/CRM systems.

4. **No Feedback Loop**: No mechanism to send matched customer segments back to Google/Meta for Customer Match.

---

## Detailed Findings

### 1. Existing Onboarding Flow

The current [OnboardingOrchestrator](packages/shared-onboarding/growthnav/onboarding/orchestrator.py:149-351) implements:

```
Request Validation → Dataset Provisioning → Customer Registration → Credential Storage
       ↓                    ↓                       ↓                    ↓
   (format checks)    (BigQuery dataset)    (CustomerRegistry)    (Secret Manager)
```

**Strengths**:
- Automatic rollback on failure ([orchestrator.py:306-351](packages/shared-onboarding/growthnav/onboarding/orchestrator.py))
- Platform ID validation (Google Ads: `123-456-7890`, Meta: `act_xxxxx`)
- Status lifecycle tracking (PENDING → VALIDATING → PROVISIONING → REGISTERING → COMPLETED)

**Gap**: No data source configuration step. After onboarding, there's no workflow to connect the customer's actual data sources.

### 2. Existing Field Mapping Pattern

The [POSNormalizer](packages/shared-conversions/growthnav/conversions/normalizer.py:64-171) uses a dictionary-based mapping:

```python
def _default_field_map(self) -> dict[str, str]:
    return {
        # Transaction ID variants
        "order_id": "transaction_id",
        "transaction_id": "transaction_id",
        "receipt_number": "transaction_id",
        "check_number": "transaction_id",
        # Value variants
        "total": "value",
        "total_amount": "value",
        "amount": "value",
        "subtotal": "value",
        # ...
    }
```

**Strengths**:
- Covers common field name variations
- Custom mappings can override defaults
- Preserves raw data for debugging

**Gap**: This is essentially hardcoded. The CLV research describes using deep learning (Sherlock/Sato) to automatically detect semantic types from data distributions.

### 3. Conversion Schema Design

The [Conversion](packages/shared-conversions/growthnav/conversions/schema.py:62-196) dataclass captures:

**CLV-Required Fields (RFM)**:
- `timestamp` → Recency calculation
- Transaction count → Frequency calculation
- `value` → Monetary calculation

**Attribution Fields**:
- `gclid`, `fbclid`, `ttclid`, `msclkid` → Click ID tracking
- `utm_source`, `utm_medium`, `utm_campaign` → UTM parameters
- `attributed_platform`, `attribution_model`, `attribution_weight` → Attribution results

**Identity Fields**:
- `customer_id` → GrowthNav tenant ID
- `user_id` → End-user identifier (single field)

**Gap**: Single `user_id` field doesn't support the Identity Graph concept. Real data has multiple identity fragments that need probabilistic linking.

### 4. MCP Server Integration

The [MCP server](packages/mcp-server/growthnav_mcp/server.py) exposes:

**Tools**:
- `query_bigquery` - Execute queries against customer datasets
- `normalize_pos_data` - Normalize POS transactions
- `normalize_crm_data` - Normalize CRM leads
- `generate_pdf_report`, `create_sheets_dashboard`, `create_slides_presentation` - Reporting

**Resources**:
- `customer://{customer_id}` - Customer configuration
- `industries://list` - Industry enumeration

**Gap**: No tools for data source configuration, connector management, or schema discovery.

---

## Architecture Documentation

### Current Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           GrowthNav Platform                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────┐│
│  │  shared-onboarding  │    │  shared-conversions │    │ shared-bigquery ││
│  │  ┌───────────────┐  │    │  ┌───────────────┐  │    │ ┌─────────────┐ ││
│  │  │ Orchestrator  │  │    │  │ POSNormalizer │  │    │ │TenantClient │ ││
│  │  │ Provisioner   │  │    │  │ CRMNormalizer │  │    │ │  Registry   │ ││
│  │  │ CredStore     │  │    │  │ Attribution   │  │    │ │  Validator  │ ││
│  │  └───────────────┘  │    │  └───────────────┘  │    │ └─────────────┘ ││
│  └─────────────────────┘    └─────────────────────┘    └─────────────────┘│
│                                                                             │
│                         ┌─────────────────────────┐                         │
│                         │      mcp-server         │                         │
│                         │   (FastMCP tools)       │                         │
│                         └─────────────────────────┘                         │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │         BigQuery              │
                    │  ┌─────────────────────────┐  │
                    │  │ growthnav_{customer_id} │  │
                    │  │     (per-tenant data)   │  │
                    │  └─────────────────────────┘  │
                    └───────────────────────────────┘
```

**Data Flow (Current)**:
1. Client calls MCP tool with pre-formatted data
2. Normalizer maps fields using dictionary lookups
3. Data inserted into customer's BigQuery dataset
4. Attribution engine matches click IDs

**Missing Layer**: No connection between external systems and GrowthNav

### Proposed Architecture Enhancement

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           GrowthNav Platform (Enhanced)                     │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    NEW: shared-connectors                             │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐  │  │
│  │  │ Connector      │  │ Schema         │  │ Identity               │  │  │
│  │  │ Adapters       │  │ Discovery      │  │ Resolution             │  │  │
│  │  │ ┌──────────┐   │  │ ┌──────────┐   │  │ ┌──────────────────┐   │  │  │
│  │  │ │Square    │   │  │ │Profiler  │   │  │ │IdentityGraph     │   │  │  │
│  │  │ │Toast     │   │  │ │LLMMapper │   │  │ │SparkMatcher      │   │  │  │
│  │  │ │Salesforce│   │  │ │Valentine │   │  │ │CrossSystemLinker │   │  │  │
│  │  │ │Shopify   │   │  │ └──────────┘   │  │ └──────────────────┘   │  │  │
│  │  │ └──────────┘   │  └────────────────┘  └────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│              ┌─────────────────────┼─────────────────────┐                  │
│              ▼                     ▼                     ▼                  │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────┐│
│  │  shared-onboarding  │    │  shared-conversions │    │ shared-bigquery ││
│  │  (+ DataSourceConfig)│   │  (+ IdentityFields) │    │    (unchanged)  ││
│  └─────────────────────┘    └─────────────────────┘    └─────────────────┘│
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
            │   Square     │ │  Salesforce  │ │   Shopify    │
            │   Toast      │ │   HubSpot    │ │   WooCommerce│
            │   Clover     │ │   Zoho CRM   │ │   BigCommerce│
            └──────────────┘ └──────────────┘ └──────────────┘
                 POS              CRM             Ecommerce
```

---

## What Should We Build?

### Tier 1: Foundation (Build Now)

#### 1. **shared-connectors Package**

A new package to handle external system integration:

```
packages/shared-connectors/
├── pyproject.toml
├── growthnav/
│   └── connectors/
│       ├── __init__.py
│       ├── base.py            # Abstract Connector class
│       ├── registry.py        # Connector type registry
│       ├── adapters/
│       │   ├── __init__.py
│       │   ├── pos/
│       │   │   ├── square.py
│       │   │   ├── toast.py
│       │   │   └── clover.py
│       │   ├── crm/
│       │   │   ├── salesforce.py
│       │   │   └── hubspot.py
│       │   └── ecommerce/
│       │       ├── shopify.py
│       │       └── woocommerce.py
│       ├── discovery/
│       │   ├── __init__.py
│       │   ├── profiler.py    # Column statistics and sampling
│       │   └── matcher.py     # Field-to-schema matching
│       └── identity/
│           ├── __init__.py
│           └── linker.py      # Cross-system identity linking
└── tests/
```

**Base Connector Pattern**:

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generator
from growthnav.conversions import Conversion

@dataclass
class ConnectorConfig:
    """Configuration for a data source connector."""
    connector_type: str  # "square", "toast", "salesforce", etc.
    customer_id: str
    credentials_secret_path: str
    field_overrides: dict[str, str] | None = None
    sync_mode: str = "incremental"  # or "full"
    last_sync: datetime | None = None

class BaseConnector(ABC):
    """Abstract base class for data source connectors."""

    connector_type: str

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._client = None

    @abstractmethod
    def authenticate(self) -> None:
        """Authenticate with the external system."""
        pass

    @abstractmethod
    def fetch_transactions(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> Generator[dict, None, None]:
        """Yield raw transaction records from the source."""
        pass

    @abstractmethod
    def get_schema(self) -> dict[str, str]:
        """Return the source system's schema (column → type)."""
        pass

    def normalize(self, raw_records: list[dict]) -> list[Conversion]:
        """Normalize raw records to Conversion objects."""
        # Uses the appropriate normalizer from shared-conversions
        pass
```

#### 2. **Data Source Configuration in Onboarding**

Extend [OnboardingRequest](packages/shared-onboarding/growthnav/onboarding/orchestrator.py) to include data source configuration:

```python
@dataclass
class DataSourceConfig:
    """Configuration for a customer data source."""
    source_type: str  # "pos", "crm", "ecommerce", "loyalty"
    connector_type: str  # "square", "toast", "salesforce", etc.
    credentials: dict[str, str]  # API keys, OAuth tokens, etc.
    field_overrides: dict[str, str] | None = None
    sync_schedule: str = "daily"  # or "hourly", "realtime"

@dataclass
class OnboardingRequest:
    # Existing fields...
    customer_id: str
    customer_name: str
    industry: Industry
    # New field
    data_sources: list[DataSourceConfig] = field(default_factory=list)
```

#### 3. **Schema Discovery Service**

A service to analyze customer data and suggest field mappings:

```python
class SchemaDiscovery:
    """Analyzes source data to discover schema mappings."""

    def profile_columns(
        self,
        sample_data: list[dict],
    ) -> dict[str, ColumnProfile]:
        """
        Profile each column in the sample data.

        Returns statistics like:
        - Data type distribution
        - Null percentage
        - Unique value count
        - Sample values
        - Patterns detected (email, phone, currency, date)
        """
        pass

    def suggest_mappings(
        self,
        source_schema: dict[str, ColumnProfile],
        target_schema: type[Conversion],
    ) -> dict[str, MappingSuggestion]:
        """
        Suggest field mappings from source to target.

        Uses:
        1. Column name similarity
        2. Data type compatibility
        3. Pattern matching (email regex → user_id, currency → value)
        4. Value distribution analysis
        """
        pass
```

### Tier 2: Enhancement (Build Later)

#### 4. **Identity Resolution Foundation**

Extend the `Conversion` schema to support multiple identity fragments:

```python
@dataclass
class IdentityFragment:
    """A single identity identifier."""
    fragment_type: str  # "email", "phone", "hashed_cc", "loyalty_id", etc.
    fragment_value: str
    confidence: float = 1.0
    source_system: str | None = None

@dataclass
class Conversion:
    # Existing fields...
    user_id: str | None = None  # Primary resolved ID

    # New field
    identity_fragments: list[IdentityFragment] = field(default_factory=list)
```

#### 5. **LLM-Assisted Schema Mapping**

Optional integration with Claude for complex schema matching:

```python
class LLMSchemaMapper:
    """Uses LLM to map complex schemas."""

    def map_with_context(
        self,
        source_schema: dict[str, ColumnProfile],
        target_schema: dict[str, str],
        sample_rows: list[dict],
    ) -> dict[str, str]:
        """
        Use LLM to understand semantic meaning of columns.

        Prompt includes:
        - Source column names and sample values
        - Target schema with descriptions
        - Industry context (golf, medical, restaurant)
        """
        pass
```

### Tier 3: Advanced (Future Vision)

#### 6. **Probabilistic Identity Graph**

Following the CLV research vision using Splink-style matching:

```python
class IdentityGraph:
    """Graph-based identity resolution across systems."""

    def link_records(
        self,
        records: list[Conversion],
        blocking_rules: list[str],
        comparison_columns: list[str],
    ) -> dict[str, str]:
        """
        Probabilistically link records to unified identities.

        Uses Fellegi-Sunter model (like Splink):
        - m-probability: P(match | records are same person)
        - u-probability: P(match | records are different people)

        Returns mapping: conversion_id → global_person_id
        """
        pass
```

#### 7. **Customer Match Export**

Export matched audiences to ad platforms:

```python
class CustomerMatchExporter:
    """Export customer segments to ad platforms."""

    def export_to_google(
        self,
        customer_id: str,
        segment_name: str,
        customers: list[dict],
    ) -> str:
        """
        Export to Google Customer Match.

        Normalizes and hashes PII:
        - Email: lowercase, SHA-256
        - Phone: E.164 format, SHA-256
        - Name + Zip: normalized, SHA-256
        """
        pass

    def export_to_meta(
        self,
        customer_id: str,
        segment_name: str,
        customers: list[dict],
    ) -> str:
        """Export to Meta Custom Audiences via CAPI."""
        pass
```

---

## What Should We Import?

### Immediate Additions (pip install)

| Package | Purpose | Priority |
|---------|---------|----------|
| `httpx` | Async HTTP client for API connectors | P0 |
| `tenacity` | Already in deps, use for retry logic | P0 |
| `python-dateutil` | Date parsing for diverse formats | P1 |
| `phonenumbers` | Phone number normalization | P2 |
| `email-validator` | Email format validation | P2 |

### Consider for Tier 2/3

| Package | Purpose | When |
|---------|---------|------|
| `splink` | Probabilistic record linkage | Identity resolution phase |
| `polars` | Fast DataFrame operations | Large-scale processing |
| `textdistance` | String similarity for name matching | Identity resolution |
| `langchain` or `anthropic` | LLM schema mapping | If automated discovery needed |

### Platform SDKs (As Needed)

| Platform | SDK | When |
|----------|-----|------|
| Square | `squareup` | When Square connector built |
| Toast | `toasttab-api` (unofficial) | When Toast connector built |
| Salesforce | `simple-salesforce` | When Salesforce connector built |
| HubSpot | `hubspot-api-client` | When HubSpot connector built |
| Shopify | `ShopifyAPI` | When Shopify connector built |

---

## How to Make External Connections Easier

### Pattern 1: Connector Configuration via MCP

Add MCP tools for data source management:

```python
@mcp.tool()
def configure_data_source(
    customer_id: str,
    source_type: str,  # "pos", "crm", "ecommerce"
    connector_type: str,  # "square", "salesforce", "shopify"
    credentials: dict[str, str],
    field_overrides: dict[str, str] | None = None,
) -> dict:
    """
    Configure a new data source for a customer.

    Validates credentials, tests connection, stores config.
    """
    pass

@mcp.tool()
def discover_schema(
    customer_id: str,
    source_name: str,
    sample_size: int = 100,
) -> dict:
    """
    Analyze a data source and suggest field mappings.

    Returns suggested mappings with confidence scores.
    """
    pass

@mcp.tool()
def sync_data_source(
    customer_id: str,
    source_name: str,
    since: str | None = None,  # ISO datetime
) -> dict:
    """
    Sync data from a configured source.

    Returns sync status, records processed, errors.
    """
    pass
```

### Pattern 2: Onboarding Wizard Workflow

A Claude Skill for guided data source setup:

```markdown
# Data Source Configuration Skill

## Workflow

### Step 1: Identify Data Sources
Ask the customer:
- What POS system do you use? (Square, Toast, Clover, Lightspeed, other)
- What CRM do you use? (Salesforce, HubSpot, Zoho, none)
- Do you have an ecommerce platform? (Shopify, WooCommerce, BigCommerce, none)
- Do you have a loyalty program? (Platform name or custom)

### Step 2: Configure Connectors
For each data source:
1. Collect API credentials
2. Test connection
3. Sample data and analyze schema
4. Review suggested field mappings
5. Customize if needed
6. Store configuration

### Step 3: Initial Sync
- Run full historical sync
- Verify data quality
- Set up incremental sync schedule
```

### Pattern 3: Field Mapping UI Feedback Loop

For complex cases where auto-mapping fails:

```python
@dataclass
class MappingSuggestion:
    """A suggested field mapping."""
    source_field: str
    target_field: str
    confidence: float  # 0.0 - 1.0
    reason: str  # "name_match", "type_match", "pattern_match"
    sample_values: list[Any]

class SchemaReviewWorkflow:
    """Interactive schema mapping review."""

    def generate_review(
        self,
        source_schema: dict,
        suggested_mappings: list[MappingSuggestion],
    ) -> str:
        """
        Generate a human-readable review document.

        Format:
        | Source Field | Suggested Target | Confidence | Sample Values | Action |
        |--------------|------------------|------------|---------------|--------|
        | order_total  | value            | 95%        | 42.50, 15.00  | Accept |
        | cust_email   | user_id          | 80%        | j@ex.com      | Review |
        | field_x      | ???              | 0%         | abc123        | Manual |
        """
        pass
```

---

## Implementation Priorities

### Phase 1: Foundation (P0)

1. **Create `shared-connectors` package structure**
   - Base connector abstract class
   - Connector registry pattern
   - ConnectorConfig dataclass

2. **Implement Square connector** (most common POS)
   - Authentication via Square Connect API
   - Transaction fetch with pagination
   - Field mapping to Conversion schema

3. **Extend OnboardingRequest with data sources**
   - Add DataSourceConfig support
   - Store connector configs in registry

### Phase 2: Schema Discovery (P1)

4. **Implement SchemaDiscovery service**
   - Column profiling (stats, patterns)
   - Heuristic-based mapping suggestions
   - Confidence scoring

5. **Add MCP tools for connector management**
   - configure_data_source
   - discover_schema
   - sync_data_source

### Phase 3: Additional Connectors (P2)

6. **Build common connectors**
   - Toast (POS)
   - Salesforce (CRM)
   - Shopify (Ecommerce)

7. **Add sync orchestration**
   - Scheduled syncs
   - Incremental sync tracking
   - Error handling and retry

### Phase 4: Identity Resolution (P3)

8. **Extend Conversion schema for identity fragments**
9. **Implement basic identity linking** (deterministic first)
10. **Consider probabilistic matching** (Splink integration)

---

## Historical Context (from thoughts/)

### From Original Research (2025-11-27)

The [original architecture research](thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md) established:

- **Namespace pattern**: `growthnav.*` for all packages
- **Tenant isolation**: `growthnav_{customer_id}` datasets
- **Industry classification**: For cross-customer benchmarking
- **MCP server**: Unified tool exposure

### From CLV Integration Research

The [CLV research document](thoughts/Automating CLV Data Integration Across Data Lakes.md) describes:

- **Schema-on-Read philosophy**: Don't force data format changes at source
- **Semantic type detection**: Sherlock/Sato models for automatic column classification
- **Common Information Model**: Customer, Transaction, Product, Touchpoint entities
- **Identity Graph**: Transitive linking across identifiers
- **RFM Vector**: Core CLV input (Recency, Frequency, Monetary)

### From Implementation Plan (2025-11-30)

The [remaining implementation plan](thoughts/shared/plans/2025-11-30-growthnav-remaining-implementation-plan.md) shows:

- All core packages implemented and tested
- Claude Skills created with workflows
- Integration tests passing (with GCP credentials)
- **Gap**: No data connector layer

---

## Open Questions

1. **LLM vs. Heuristic Schema Mapping**: Should we start with simple heuristics (pattern matching, type inference) or jump to LLM-assisted mapping?

2. **Connector Priority**: Which connectors are most needed first? (Square seems likely for POS, Salesforce for CRM)

3. **Identity Resolution Scope**: Do we need probabilistic matching initially, or can deterministic linking (exact email/phone matches) suffice?

4. **Sync Architecture**: Should syncs be pull-based (scheduled polling) or push-based (webhooks)?

5. **Customer Match Integration**: Is exporting to Google/Meta Customer Match a near-term requirement or future enhancement?

---

## Code References

### Current Onboarding Implementation
- [orchestrator.py](packages/shared-onboarding/growthnav/onboarding/orchestrator.py) - OnboardingOrchestrator class
- [provisioning.py](packages/shared-onboarding/growthnav/onboarding/provisioning.py) - DatasetProvisioner class
- [secrets.py](packages/shared-onboarding/growthnav/onboarding/secrets.py) - CredentialStore class

### Current Conversion Normalization
- [schema.py](packages/shared-conversions/growthnav/conversions/schema.py) - Conversion dataclass
- [normalizer.py](packages/shared-conversions/growthnav/conversions/normalizer.py) - POSNormalizer, CRMNormalizer, LoyaltyNormalizer
- [attribution.py](packages/shared-conversions/growthnav/conversions/attribution.py) - Attribution engine

### Customer Registry
- [registry.py](packages/shared-bigquery/growthnav/bigquery/registry.py) - CustomerRegistry, Customer, Industry

### MCP Server
- [server.py](packages/mcp-server/growthnav_mcp/server.py) - FastMCP tools and resources

### Research Documents
- [thoughts/Automating CLV Data Integration Across Data Lakes.md](thoughts/Automating%20CLV%20Data%20Integration%20Across%20Data%20Lakes.md) - Deep CLV integration research
- [thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md](thoughts/shared/research/2025-11-27-shared-analytics-infrastructure-architecture.md) - Original architecture research

---

## Conclusion

**Should we consider a new path?** No fundamental change is needed. The existing architecture is sound. What's needed is an **extension layer** (`shared-connectors`) that bridges external systems to the existing normalization pipeline.

**What should we build?**
1. Connector adapter framework with base class and registry
2. Schema discovery service for field mapping assistance
3. MCP tools for connector configuration and sync management
4. Initial connectors for Square (POS) and Salesforce (CRM)

**What should we import?**
- `httpx` for async API calls
- Platform SDKs as connectors are built
- Consider `splink` later for identity resolution

**How to make connections easier?**
1. Connector configuration via MCP tools
2. Schema discovery with confidence-scored suggestions
3. Claude Skill for guided data source setup workflow
4. Human-in-the-loop review for low-confidence mappings
