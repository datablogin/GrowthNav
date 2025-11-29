---
date: 2025-11-27T10:00:00-06:00
researcher: Robert Welborn
topic: "Shared Analytics Infrastructure Architecture: Consolidating PaidSearchNav-MCP, PaidSocialNav, and AutoCLV"
tags: [research, architecture, monorepo, mcp, bigquery, reporting, infrastructure]
status: complete
last_updated: 2025-11-27
last_updated_by: Robert Welborn
---

# Research: Shared Analytics Infrastructure Architecture

**Date**: 2025-11-27
**Researcher**: Robert Welborn

## Research Question

How should three converging analytics repositories (PaidSearchNav-MCP, PaidSocialNav, AutoCLV) be consolidated to share common infrastructure? Specifically:
- Customer onboarding modules
- BigQuery project creation and data storage
- Promotional data storage
- Reporting framework (HTML, Google Slides, Google Sheets)
- Conversion data integration
- Support for 6-7 additional planned analytics tools

What is the right architecture: monorepo, multi-repo, or Claude Skills?

---

## Summary

After analyzing all three repositories, **significant overlap exists** in BigQuery integration, customer registry patterns, reporting capabilities, and MCP server architecture. The recommended approach is a **uv workspace monorepo** with namespace packages, a unified MCP server, and shared Claude Skills.

### Key Findings

| Capability | PaidSearchNav-MCP | PaidSocialNav | AutoCLV |
|------------|-------------------|---------------|---------|
| BigQuery Client | ✅ Custom wrapper | ✅ BQClient class | ✅ Connector factory |
| Customer Registry | ✅ BigQuery table | ✅ BigQuery + YAML fallback | ❌ None |
| HTML Reports | ❌ None | ✅ Jinja2 templates | ✅ Export templates |
| PDF Reports | ❌ None | ✅ WeasyPrint | ❌ None |
| Google Sheets | ❌ None | ✅ gspread API | ❌ None |
| MCP Server | ✅ FastMCP | ✅ FastMCP | ✅ FastMCP |
| Conversion Tracking | ✅ SearchTermMetrics | ✅ fct_ad_insights_daily | ❌ Different focus |

---

## Detailed Findings

### 1. BigQuery Integration (High Overlap)

All three repositories implement BigQuery clients with similar patterns:

#### PaidSearchNav-MCP
**File**: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/clients/bigquery/client.py`
- `BigQueryClient` class with async query execution
- Cost estimation via dry-run queries
- Schema inspection for table metadata
- Max results: 10,000, timeout: 300 seconds

#### PaidSocialNav
**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/storage/bq.py`
- `BQClient` class with parameterized queries
- Dataset/table creation with `exists_ok=True`
- MERGE-based deduplication for data loading
- Staging table pattern for atomic operations

#### AutoCLV
**File**: `/Users/robertwelborn/PycharmProjects/AutoCLV/analytics/libs/data_warehouse/connectors/bigquery.py`
- `BigQueryConnector` with `BigQueryConfig` Pydantic model
- Async query execution with job status tracking
- Schema discovery and table sampling
- Factory pattern for multi-warehouse support (Snowflake, Redshift)

**Consolidation Opportunity**: Create a shared `TenantBigQueryClient` with:
- Multi-tenant isolation (dataset-per-tenant or project-per-tenant)
- Unified configuration via Pydantic settings
- Common query validation and cost estimation
- Connection pooling and circuit breakers

---

### 2. Customer Registry/Onboarding (Medium Overlap)

#### PaidSearchNav-MCP
**File**: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/clients/bigquery/customer_registry.py`
- `CustomerRegistry` class with LRU cache (TTL: 3600s)
- BigQuery table: `{registry_project}.paidsearchnav_production.customer_registry`
- `CustomerConfig` dataclass: project_id, dataset, account_name, status
- Manual onboarding documented in `/docs/CUSTOMER_ONBOARDING.md`

#### PaidSocialNav
**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/core/customer_registry.py`
- `CustomerRegistry` class with BigQuery storage + YAML fallback
- `Customer` dataclass: 15 fields including meta_ad_account_ids, tags, notes
- Script-based onboarding: `/scripts/onboard_customer.py`
- Secret Manager integration for credential storage

#### AutoCLV
- No dedicated customer registry
- Transaction-based data ingestion via CLI
- Focus on analytics rather than SaaS multi-tenancy

**Consolidation Opportunity**: Create a shared `CustomerOnboardingOrchestrator` with:
- Unified `Customer` model supporting multiple platforms
- Tier-based provisioning (basic/premium/enterprise)
- Infrastructure-as-code dataset creation
- Secret Manager integration for all credentials

---

### 3. Reporting Framework (Low-Medium Overlap)

#### PaidSearchNav-MCP
- **No direct reporting** - delegates to Claude Skills
- MCP server returns JSON data for Claude Desktop to format
- Jinja2 used only for Google Ads script templates

#### PaidSocialNav
**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/render/renderer.py`
- `ReportRenderer` class with Jinja2 templates
- HTML template: `/paid_social_nav/render/templates/audit_report.html.j2`
- Markdown template: `/paid_social_nav/render/templates/audit_report.md.j2`
- Chart generation: matplotlib via `/paid_social_nav/visuals/charts.py`

**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/render/pdf.py`
- WeasyPrint integration for HTML-to-PDF conversion

**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/sheets/exporter.py`
- `GoogleSheetsExporter` class with Google Sheets API v4
- 3-tab format: Executive Summary, Rule Details, Raw Data
- Conditional formatting for score visualization

#### AutoCLV
**File**: `/Users/robertwelborn/PycharmProjects/AutoCLV/customer_base_audit/monitoring/exports.py`
- Markdown export: `export_drift_report_markdown()`
- JSON export: `export_drift_report_json()`
- CSV export: `export_drift_report_csv()`

**File**: `/Users/robertwelborn/PycharmProjects/AutoCLV/analytics/services/reporting_engine/routes/exports.py`
- Template-based exports with multiple formats (HTML, PDF, Excel, CSV, JSON)
- Predefined report templates (user_analytics, data_quality_summary)

**Consolidation Opportunity**: Create a shared `ReportingEngine` with:
- Template registry for reusable report formats
- Unified export API supporting HTML, PDF, Sheets, Slides
- Chart generation library with consistent styling
- Async export for large reports

---

### 4. MCP Server Architecture (High Overlap)

All three repositories use FastMCP for Model Context Protocol servers:

#### PaidSearchNav-MCP
**File**: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/server.py`
- 8+ tools: get_search_terms, get_keywords, get_campaigns, query_bigquery, etc.
- Resources: bigquery/datasets, bigquery/config
- Redis caching with circuit breakers
- ~1,728 lines of MCP implementation

#### PaidSocialNav
**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/mcp_server/server.py`
- Tools, resources, and prompts for social media audit
- Authentication via `/mcp_server/auth.py`

#### AutoCLV
**File**: `/Users/robertwelborn/PycharmProjects/AutoCLV/customer_base_audit/mcp/`
- MCP server for CLV analytics
- Serializers for Five Lenses data

**Consolidation Opportunity**: Create a unified `SharedAnalyticsMCP` server with:
- Composable sub-servers (mount pattern)
- Shared authentication/authorization
- Common tool patterns for BigQuery, reporting, etc.
- Single entry point for Claude Desktop/Claude Code

---

### 5. Promotional/Conversion Data (Medium Overlap)

#### PaidSearchNav-MCP
**File**: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/models/search_term.py`
- `SearchTermMetrics`: conversions, conversion_value, cpa, conversion_rate, roas
- Stored in BigQuery views: keyword_stats_with_keyword_info_view, search_term_stats_view

#### PaidSocialNav
**File**: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/adapters/meta/adapter.py`
- Conversions derived from Meta `actions` array
- Stored in `fct_ad_insights_daily.conversions`
- Global ID format: `meta:account:{id}`, `meta:campaign:{id}`

#### AutoCLV
**File**: `/Users/robertwelborn/PycharmProjects/AutoCLV/customer_base_audit/synthetic/generator.py`
- Promotional data via `ScenarioConfig.promo_month` and `promo_uplift`
- Focus on transaction-level data, not ad platform metrics

**Consolidation Opportunity**: Create a unified conversion data model with:
- Platform-agnostic conversion schema
- Global ID convention across platforms
- Time-series aggregation patterns
- Cross-platform attribution support

---

### 6. Dependencies Analysis

#### Common Dependencies

| Package | PaidSearchNav-MCP | PaidSocialNav | AutoCLV |
|---------|-------------------|---------------|---------|
| google-cloud-bigquery | >=3.13.0 | >=3.11 | >=3.25.0 |
| pydantic | >=2.0.0 | implicit | >=2.5.0 |
| fastmcp | >=0.3.0 | >=2.13.1 | >=2.12.0 |
| pandas | >=2.0.0 | implicit | >=2.1.0 |
| jinja2 | >=3.1.0 | >=3.1.0 | - |
| redis | >=5.0.0 | - | >=5.0.0 |
| tenacity | >=8.0.0 | - | >=9.0.0 |

#### Unique Dependencies

- **PaidSearchNav-MCP**: google-ads>=22.0.0, circuitbreaker>=2.0.0
- **PaidSocialNav**: weasyprint>=60.0, google-api-python-client>=2.0.0, anthropic>=0.34.0
- **AutoCLV**: pymc-marketing>=0.3.0, langgraph>=0.6.0, opentelemetry-sdk>=1.37.0

---

## Architecture Documentation

### Current State: Separate Repositories

```
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│  PaidSearchNav-MCP  │  │    PaidSocialNav    │  │      AutoCLV        │
├─────────────────────┤  ├─────────────────────┤  ├─────────────────────┤
│ - Google Ads API    │  │ - Meta/Reddit/etc   │  │ - CLV Analytics     │
│ - BigQuery client   │  │ - BigQuery client   │  │ - BigQuery client   │
│ - Customer registry │  │ - Customer registry │  │ - Data warehouse    │
│ - MCP server        │  │ - MCP server        │  │ - MCP server        │
│                     │  │ - PDF/Sheets export │  │ - Export engine     │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                    Duplicated Infrastructure
```

### Proposed State: Shared Infrastructure Monorepo

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    analytics-platform (uv workspace)                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                     Shared Libraries (namespace packages)            ││
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐           ││
│  │  │ mycompany/     │ │ mycompany/     │ │ mycompany/     │           ││
│  │  │   bigquery/    │ │   reporting/   │ │   onboarding/  │           ││
│  │  │ - client.py    │ │ - pdf.py       │ │ - orchestrator │           ││
│  │  │ - registry.py  │ │ - sheets.py    │ │ - provisioning │           ││
│  │  │ - validation.py│ │ - html.py      │ │ - secrets.py   │           ││
│  │  └────────────────┘ └────────────────┘ └────────────────┘           ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                     Unified MCP Server                               ││
│  │  - BigQuery tools (query, schema, cost estimation)                   ││
│  │  - Reporting tools (PDF, Sheets, Slides generation)                  ││
│  │  - Customer tools (onboarding, lookup, update)                       ││
│  │  - Platform-specific tools (mounted sub-servers)                     ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐            │
│  │ PaidSearchNav   │ │ PaidSocialNav   │ │ AutoCLV         │  + 6 more  │
│  │ (app)           │ │ (app)           │ │ (app)           │  apps      │
│  │ - Google Ads    │ │ - Meta/Social   │ │ - CLV models    │            │
│  │ - Keywords      │ │ - Audit rules   │ │ - Five Lenses   │            │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘            │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Recommended Architecture: uv Workspace Monorepo

### Why uv Workspaces?

1. **Speed**: 10-100x faster than Poetry/pip for dependency resolution
2. **Shared lockfile**: Single `uv.lock` ensures consistency across all packages
3. **Namespace packages**: Clean imports like `from mycompany.bigquery import TenantClient`
4. **Editable installs**: Automatic for workspace members
5. **Mature enough**: Used by LlamaIndex, modern Python standard

### Proposed Directory Structure

```text
GrowthNav/                              # Root workspace
├── pyproject.toml                     # Workspace configuration
├── uv.lock                            # Shared lockfile
├── .mcp.json                          # Project-scope MCP config
├── .claude/
│   └── skills/                        # Shared Claude Skills
│       ├── analytics-reporting/
│       │   └── SKILL.md
│       ├── customer-onboarding/
│       │   └── SKILL.md
│       └── bigquery-best-practices/
│           └── SKILL.md
│
├── packages/
│   ├── shared-bigquery/               # Shared BigQuery client
│   │   ├── pyproject.toml
│   │   └── growthnav/
│   │       └── bigquery/
│   │           ├── client.py          # TenantBigQueryClient
│   │           ├── registry.py        # CustomerRegistry
│   │           └── validation.py      # Query validation
│   │
│   ├── shared-reporting/              # Shared reporting framework
│   │   ├── pyproject.toml
│   │   └── growthnav/
│   │       └── reporting/
│   │           ├── pdf.py             # WeasyPrint wrapper
│   │           ├── sheets.py          # Google Sheets API
│   │           ├── slides.py          # Google Slides API (P1)
│   │           └── templates/         # Jinja2 templates
│   │
│   ├── shared-onboarding/             # Customer onboarding
│   │   ├── pyproject.toml
│   │   └── growthnav/
│   │       └── onboarding/
│   │           ├── orchestrator.py    # Onboarding workflow
│   │           ├── provisioning.py    # Infrastructure setup
│   │           └── secrets.py         # Secret Manager
│   │
│   ├── shared-conversions/            # Unified conversion tracking
│   │   ├── pyproject.toml
│   │   └── growthnav/
│   │       └── conversions/
│   │           ├── schema.py          # Platform-agnostic schema
│   │           ├── normalizer.py      # POS/CRM/Loyalty ingestion
│   │           └── attribution.py     # Cross-platform attribution
│   │
│   ├── mcp-server/                    # Unified MCP server
│   │   ├── pyproject.toml
│   │   └── growthnav_mcp/
│   │       ├── server.py              # FastMCP entry point
│   │       ├── tools/                 # Tool implementations
│   │       └── resources/             # Resource handlers
│   │
│   ├── app-paid-search/               # PaidSearchNav (migrated)
│   ├── app-paid-social/               # PaidSocialNav (migrated)
│   ├── app-auto-clv/                  # AutoCLV (migrated)
│   └── app-*/                         # Future 6-7 tools
│
├── thoughts/                          # Research and documentation
│   └── shared/
│       └── research/
│
└── .github/
    └── workflows/
        └── ci.yml                     # Test only changed packages
```

### Root pyproject.toml

```toml
[project]
name = "growthnav"
version = "0.1.0"
requires-python = ">=3.11"
description = "Shared analytics infrastructure platform for growth marketing tools"

[tool.uv.workspace]
members = [
    "packages/shared-*",
    "packages/mcp-server",
    "packages/app-*",
]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "mypy>=1.8.0",
    "ruff>=0.3.0",
]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["packages/*/tests"]
```

---

## Alternative Architectures Considered

### Option 2: Multi-Repo with Internal PyPI

**Structure**: Separate repos publishing to internal PyPI (devpi)

**Pros**:
- Independent versioning per library
- Familiar multi-repo workflow
- Clear ownership boundaries

**Cons**:
- Higher coordination overhead
- Difficult cross-package refactoring
- Version compatibility challenges
- More complex CI/CD

**Verdict**: Not recommended for team of 1-3 developers. Overhead exceeds benefits.

### Option 3: Claude Skills Only (No Shared Code)

**Structure**: Each app remains independent, share knowledge via Claude Skills

**Pros**:
- No code changes required
- Immediate implementation
- Skills already exist

**Cons**:
- Duplicated code remains
- No shared testing
- Inconsistent implementations
- Higher maintenance long-term

**Verdict**: Good for quick wins, but doesn't solve infrastructure duplication.

### Option 4: Pants Build System

**Structure**: Monorepo with Pants for fine-grained builds

**Pros**:
- Excellent caching
- Fine-grained dependency tracking
- Mature enterprise tooling

**Cons**:
- Steep learning curve
- Overkill for <20 packages
- Complex configuration

**Verdict**: Consider only if workspace grows to 50+ packages.

---

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)

1. **Create OrchestrationNav monorepo** with uv workspace
2. **Extract shared-bigquery** from PaidSocialNav (most complete implementation)
3. **Create namespace package** structure: `orchestrationnav.*`
4. **Write unit tests** for BigQuery client

### Phase 2: Reporting & Onboarding (Weeks 3-4)

1. **Extract shared-reporting** from PaidSocialNav
2. **Extract shared-onboarding** from PaidSocialNav
3. **Add Google Slides support** (new capability)
4. **Create unified templates** directory

### Phase 3: MCP Server (Weeks 5-6)

1. **Create unified MCP server** exposing all shared capabilities
2. **Mount sub-servers** for app-specific tools
3. **Configure project-scope MCP** for team sharing
4. **Document MCP tools and resources**

### Phase 4: Application Migration (Weeks 7-10)

1. **Migrate PaidSearchNav-MCP** as `app-paid-search`
2. **Migrate PaidSocialNav** as `app-paid-social`
3. **Migrate AutoCLV** as `app-auto-clv`
4. **Update imports** to use `orchestrationnav.*` namespace

### Phase 5: Claude Skills (Week 11)

1. **Create analytics-reporting skill** with workflow documentation
2. **Create customer-onboarding skill** with step-by-step guide
3. **Create bigquery-best-practices skill** with patterns and anti-patterns

### Phase 6: CI/CD & Documentation (Week 12)

1. **Set up GitHub Actions** with changed-package detection
2. **Create Docker multi-stage builds** for each app
3. **Write architecture documentation**
4. **Create contributor guide**

---

## Open Source Tools for Quick Wins

### Immediate Adoption

| Tool | Purpose | Benefit |
|------|---------|---------|
| **uv** | Package management | 10x faster installs, native workspaces |
| **FastMCP** | MCP server framework | Production-ready, zero-config OAuth |
| **WeasyPrint** | PDF generation | Already in PaidSocialNav |
| **gspread** | Google Sheets | Already in PaidSocialNav |
| **Ruff** | Linting/formatting | 100x faster than flake8+black |

### Consider for Future

| Tool | Purpose | When to Add |
|------|---------|-------------|
| **Polylith** | Component architecture | When apps share >50% code |
| **devpi** | Internal PyPI | When publishing to external consumers |
| **Pants** | Build system | When packages exceed 50 |
| **Great Expectations** | Data validation | When data quality becomes critical |

---

## Claude Skills vs MCP Server vs Shared Libraries

### When to Use Each

| Approach | Use When | Examples |
|----------|----------|----------|
| **Shared Libraries** | Code needs reuse across apps | BigQuery client, PDF generator |
| **MCP Server** | Claude needs tool access | query_bigquery(), generate_report() |
| **Claude Skills** | Workflow documentation | "How to onboard a customer" |

### Combined Pattern

```
┌─────────────────────────────────────────────────────────────┐
│                     Claude Desktop/Code                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Claude Skill: "analytics-reporting"                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ SKILL.md describes:                                  │   │
│  │ 1. When to use each reporting format                 │   │
│  │ 2. How to query BigQuery for customer data           │   │
│  │ 3. Best practices for PDF layout                     │   │
│  │ 4. Google Sheets sharing settings                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│                          ▼                                  │
│  MCP Server: unified-analytics                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Tools:                                               │   │
│  │ - query_bigquery(sql, tenant_id)                    │   │
│  │ - generate_pdf_report(data, template)               │   │
│  │ - create_sheets_dashboard(data, share_with)         │   │
│  │                                                      │   │
│  │ Resources:                                           │   │
│  │ - customer://{id}/config                            │   │
│  │ - template://pdf/{name}                             │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│                          ▼                                  │
│  Shared Libraries: orchestrationnav.*                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ orchestrationnav.bigquery.TenantBigQueryClient      │   │
│  │ orchestrationnav.reporting.PDFGenerator             │   │
│  │ orchestrationnav.reporting.SheetsExporter           │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Decision Matrix: Monorepo vs Multi-Repo vs Skills

| Factor | Monorepo (uv) | Multi-Repo + PyPI | Skills Only |
|--------|---------------|-------------------|-------------|
| **Initial Effort** | Medium | High | Low |
| **Maintenance** | Low | High | Medium |
| **Code Sharing** | Excellent | Good | Poor |
| **Atomic Refactoring** | Yes | No | N/A |
| **Independent Versioning** | Optional | Yes | N/A |
| **Learning Curve** | Low (uv is simple) | Low | None |
| **Scalability** | 2-50 packages | Unlimited | N/A |
| **CI/CD Complexity** | Medium | High | Low |

**Recommendation**: Start with **monorepo (uv workspaces)** for the 3 existing repos + 6-7 planned tools. Add internal PyPI only if external consumers need versioned releases.

---

## Code References

### PaidSearchNav-MCP
- BigQuery client: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/clients/bigquery/client.py`
- Customer registry: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/clients/bigquery/customer_registry.py`
- MCP server: `/Users/robertwelborn/PycharmProjects/PaidSearchNav-MCP/src/paidsearchnav_mcp/server.py`

### PaidSocialNav
- BigQuery client: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/storage/bq.py`
- Customer registry: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/core/customer_registry.py`
- PDF export: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/render/pdf.py`
- Sheets export: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/paid_social_nav/sheets/exporter.py`
- Onboarding script: `/Users/robertwelborn/PycharmProjects/PaidSocialNav/scripts/onboard_customer.py`

### AutoCLV
- BigQuery connector: `/Users/robertwelborn/PycharmProjects/AutoCLV/analytics/libs/data_warehouse/connectors/bigquery.py`
- Export engine: `/Users/robertwelborn/PycharmProjects/AutoCLV/analytics/services/reporting_engine/routes/exports.py`
- MCP serializers: `/Users/robertwelborn/PycharmProjects/AutoCLV/customer_base_audit/mcp/serializers.py`

---

## Decisions Made

1. **Namespace**: `growthnav.*` - Short, memorable, growth-focused

2. **Google Slides API**: **P1 Priority** - Build, buy, or borrow (modify open source) to implement. This is next after core infrastructure.

3. **Cross-platform attribution**: Conversion data should be **unified by customer** across search and social since conversions depend on the same transaction sources (POS, CRM, Loyalty Program). Design a platform-agnostic conversion schema that normalizes data from any ad platform to the same customer transaction.

4. **Tenant isolation model**: **Dataset-per-customer with industry tagging**
   - Each customer gets their own dataset: `growthnav_{customer_id}`
   - Customer metadata includes `industry` field for cross-learning
   - Industry groups: golf (2), medical (4), restaurants (3), etc.
   - Insights learned from one customer can be applied to industry peers
   - Enables benchmarking within industry verticals

### Industry-Based Learning Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Customer Registry                             │
├─────────────────────────────────────────────────────────────────┤
│ customer_id │ industry  │ dataset              │ tags           │
├─────────────┼───────────┼──────────────────────┼────────────────┤
│ topgolf     │ golf      │ growthnav_topgolf     │ [enterprise]   │
│ puttery     │ golf      │ growthnav_puttery     │ [smb]          │
│ medcorp_a   │ medical   │ growthnav_medcorp_a   │ [hospital]     │
│ medcorp_b   │ medical   │ growthnav_medcorp_b   │ [clinic]       │
│ restaurant1 │ restaurant│ growthnav_restaurant1 │ [qsr]          │
└─────────────────────────────────────────────────────────────────┘

Industry Insights Flow:
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Topgolf    │     │   Puttery    │     │  Golf Avg    │
│   Dataset    │────▶│   Dataset    │────▶│  Benchmarks  │
│  (learnings) │     │  (apply)     │     │  (aggregate) │
└──────────────┘     └──────────────┘     └──────────────┘
```

## Remaining Open Questions

1. **MCP authentication**: Should the unified MCP server use OAuth (Google/GitHub) or service account credentials?

2. **Benchmark storage**: Should industry benchmarks live in a shared `growthnav_benchmarks` dataset or within each customer's dataset?

3. **Cross-customer insights**: What governance is needed before applying learnings from Customer A to Customer B in the same industry?

---

## Sources

- [uv Workspaces Documentation](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- [LlamaIndex Monorepo Migration](https://www.llamaindex.ai/blog/python-tooling-at-scale-llamaindex-s-monorepo-overhaul)
- [FastMCP Documentation](https://gofastmcp.com/)
- [Python Namespace Packages](https://realpython.com/python-namespace-package/)
- [BigQuery Multi-Tenant Best Practices](https://cloud.google.com/bigquery/docs/best-practices-for-multi-tenant-workloads-on-bigquery)
- [AWS SaaS Tenant Onboarding](https://aws.amazon.com/blogs/apn/tenant-onboarding-best-practices-in-saas-with-the-aws-well-architected-saas-lens/)
- [Claude Skills Deep Dive](https://leehanchung.github.io/blogs/2025/10/26/claude-skills-deep-dive/)
