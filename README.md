# GrowthNav

Shared analytics infrastructure platform for growth marketing tools.

## Overview

GrowthNav is a uv workspace monorepo that provides shared infrastructure for multiple analytics applications:

- **PaidSearchNav** - Google Ads keyword audit and optimization
- **PaidSocialNav** - Social media advertising audit (Meta, Reddit, Pinterest, TikTok, X)
- **AutoCLV** - Customer Lifetime Value analytics
- **+ 6-7 more planned tools**

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                        GrowthNav Platform                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Shared Libraries (growthnav.*)                                         │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
│  │  bigquery    │ │  reporting   │ │  onboarding  │ │ conversions  │   │
│  │  - client    │ │  - pdf       │ │  - orchestr. │ │  - schema    │   │
│  │  - registry  │ │  - sheets    │ │  - provision │ │  - normalize │   │
│  │  - validate  │ │  - slides    │ │  - secrets   │ │  - attribute │   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
│                                                                         │
│  Unified MCP Server (growthnav_mcp)                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Tools: query_bigquery, generate_report, onboard_customer, etc.  │   │
│  │ Resources: customer://{id}, template://{name}, benchmark://...  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Applications                                                           │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐          │
│  │ paid-search│ │ paid-social│ │  auto-clv  │ │  + more    │          │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/growthnav.git
cd growthnav

# Install all dependencies
uv sync

# Run tests
uv run pytest
```

### Using Shared Libraries

```python
# BigQuery client with tenant isolation
from growthnav.bigquery import TenantBigQueryClient

client = TenantBigQueryClient(customer_id="topgolf")
results = client.query("SELECT * FROM metrics LIMIT 10")

# Reporting with multiple formats
from growthnav.reporting import PDFGenerator, SheetsExporter

pdf = PDFGenerator()
pdf.generate(data, template="customer_report")

# Unified conversion tracking
from growthnav.conversions import ConversionNormalizer

normalizer = ConversionNormalizer()
unified = normalizer.from_pos(pos_transactions)
```

## Project Structure

```text
GrowthNav/
├── packages/
│   ├── shared-bigquery/      # BigQuery client + customer registry
│   ├── shared-reporting/     # PDF, Sheets, Slides generation
│   ├── shared-onboarding/    # Customer provisioning
│   ├── shared-conversions/   # Unified conversion tracking
│   ├── mcp-server/           # Unified MCP server
│   ├── app-paid-search/      # PaidSearchNav application
│   ├── app-paid-social/      # PaidSocialNav application
│   └── app-auto-clv/         # AutoCLV application
├── .claude/skills/           # Shared Claude Skills
├── thoughts/                 # Research and documentation
└── .github/workflows/        # CI/CD
```

## Customer Data Model

Each customer gets an isolated dataset with industry tagging for cross-learning:

| customer_id | industry   | dataset            | tags         |
|-------------|------------|--------------------|--------------|
| topgolf     | golf       | growthnav_topgolf   | [enterprise] |
| puttery     | golf       | growthnav_puttery   | [smb]        |
| medcorp_a   | medical    | growthnav_medcorp_a | [hospital]   |

Insights learned from one customer can be applied to others in the same industry.

## Priority Roadmap

| Priority | Feature                  | Status      |
|----------|--------------------------|-------------|
| P0       | Shared BigQuery client   | In Progress |
| P0       | Customer registry        | In Progress |
| P1       | Google Slides output     | Planned     |
| P1       | Unified conversions      | Planned     |
| P2       | Industry benchmarks      | Planned     |

## Contributing

1. Create a feature branch
2. Make changes in the appropriate package
3. Run tests: `uv run pytest packages/<package>/tests`
4. Submit a pull request

## License

MIT
