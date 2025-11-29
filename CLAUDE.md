# GrowthNav - Shared Analytics Infrastructure

## Overview

GrowthNav is a uv workspace monorepo providing shared infrastructure for growth marketing analytics tools.

## Architecture

```
packages/
├── shared-bigquery/      # growthnav.bigquery - Multi-tenant BigQuery client
├── shared-reporting/     # growthnav.reporting - PDF, Sheets, Slides generation
├── shared-conversions/   # growthnav.conversions - Unified conversion tracking
├── mcp-server/           # growthnav_mcp - Unified MCP server
└── app-*/                # Application packages
```

## Key Concepts

### Customer Isolation
- Each customer gets isolated dataset: `growthnav_{customer_id}`
- Customer registry tracks industry for cross-learning
- Industries: golf, medical, restaurant, retail, ecommerce, etc.

### Namespace Packages
- All shared libraries use `growthnav.*` namespace
- Import pattern: `from growthnav.bigquery import TenantBigQueryClient`

### MCP Server
- Single unified server exposing all capabilities
- Tools for BigQuery, reporting, conversions
- Resources for customer configs, industry lists
- Prompts for common workflows

## Development Commands

```bash
# Install all dependencies
uv sync

# Run tests for a package
uv run --package growthnav-bigquery pytest

# Run the MCP server
uv run --package growthnav-mcp growthnav-mcp

# Add a dependency to a package
cd packages/shared-bigquery && uv add pandas
```

## Code Style

- Python 3.11+
- Type hints required
- Pydantic for data models
- Async-first where appropriate
- Line length: 100 characters

## Priority Roadmap

| Priority | Feature | Package |
|----------|---------|---------|
| P0 | BigQuery client | shared-bigquery |
| P0 | Customer registry | shared-bigquery |
| P1 | Google Slides output | shared-reporting |
| P1 | Unified conversions | shared-conversions |
| P2 | Industry benchmarks | shared-bigquery |

## Related Repositories

- PaidSearchNav-MCP: Google Ads keyword audit
- PaidSocialNav: Social media advertising audit
- AutoCLV: Customer Lifetime Value analytics
