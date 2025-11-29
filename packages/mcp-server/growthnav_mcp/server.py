"""
GrowthNav MCP Server - Main entry point.

Unified MCP server exposing all GrowthNav capabilities:
- BigQuery queries and schema inspection
- PDF, Sheets, and Slides report generation
- Customer registry and onboarding
- Conversion normalization and attribution
"""

from __future__ import annotations

from fastmcp import FastMCP

# Initialize server
mcp = FastMCP(
    "GrowthNav Analytics Platform",
    dependencies=["growthnav-bigquery", "growthnav-reporting", "growthnav-conversions"],
)


# =============================================================================
# BigQuery Tools
# =============================================================================


@mcp.tool()
def query_bigquery(
    customer_id: str,
    sql: str,
    max_results: int = 10000,
) -> dict:
    """
    Execute a BigQuery query for a customer.

    The query runs against the customer's isolated dataset (growthnav_{customer_id}).
    Queries are validated to prevent destructive operations.

    Args:
        customer_id: Customer identifier (e.g., "topgolf")
        sql: SQL query to execute
        max_results: Maximum rows to return (default 10,000)

    Returns:
        Query results with rows and metadata
    """
    from growthnav.bigquery import TenantBigQueryClient

    client = TenantBigQueryClient(customer_id=customer_id)
    result = client.query(sql, max_results=max_results)

    return {
        "rows": result.rows,
        "total_rows": result.total_rows,
        "bytes_processed": result.bytes_processed,
        "cache_hit": result.cache_hit,
    }


@mcp.tool()
def estimate_query_cost(
    customer_id: str,
    sql: str,
) -> dict:
    """
    Estimate the cost of a BigQuery query before running it.

    Args:
        customer_id: Customer identifier
        sql: SQL query to estimate

    Returns:
        Cost estimation with bytes processed and USD cost
    """
    from growthnav.bigquery import TenantBigQueryClient

    client = TenantBigQueryClient(customer_id=customer_id)
    return client.estimate_cost(sql)


@mcp.tool()
def get_table_schema(
    customer_id: str,
    table_name: str,
) -> list[dict]:
    """
    Get the schema of a table in the customer's dataset.

    Args:
        customer_id: Customer identifier
        table_name: Name of the table

    Returns:
        List of field definitions (name, type, mode, description)
    """
    from growthnav.bigquery import TenantBigQueryClient

    client = TenantBigQueryClient(customer_id=customer_id)
    return client.get_table_schema(table_name)


# =============================================================================
# Customer Registry Tools
# =============================================================================


@mcp.tool()
def get_customer(customer_id: str) -> dict | None:
    """
    Get customer configuration from the registry.

    Args:
        customer_id: Customer identifier

    Returns:
        Customer configuration or None if not found
    """
    from growthnav.bigquery import CustomerRegistry

    registry = CustomerRegistry()
    customer = registry.get_customer(customer_id)

    if customer:
        return {
            "customer_id": customer.customer_id,
            "customer_name": customer.customer_name,
            "industry": customer.industry.value,
            "dataset": customer.dataset,
            "status": customer.status.value,
            "tags": customer.tags,
        }
    return None


@mcp.tool()
def list_customers_by_industry(industry: str) -> list[dict]:
    """
    List all active customers in an industry.

    Useful for benchmarking and cross-customer learning.

    Args:
        industry: Industry name (golf, medical, restaurant, etc.)

    Returns:
        List of customers in the industry
    """
    from growthnav.bigquery import CustomerRegistry
    from growthnav.bigquery.registry import Industry

    registry = CustomerRegistry()
    customers = registry.get_customers_by_industry(Industry(industry))

    return [
        {
            "customer_id": c.customer_id,
            "customer_name": c.customer_name,
            "dataset": c.dataset,
            "tags": c.tags,
        }
        for c in customers
    ]


# =============================================================================
# Reporting Tools
# =============================================================================


@mcp.tool()
def generate_pdf_report(
    template: str,
    data: dict,
    output_path: str | None = None,
) -> dict:
    """
    Generate a PDF report from a template.

    Args:
        template: Template name (e.g., "customer_report", "audit_summary")
        data: Data to populate the template
        output_path: Optional path to save the PDF

    Returns:
        Status and path/bytes of generated PDF
    """
    from growthnav.reporting import PDFGenerator

    pdf = PDFGenerator()

    if not pdf.is_available():
        return {
            "success": False,
            "error": "WeasyPrint not available. Install with: pip install weasyprint",
        }

    try:
        pdf_bytes = pdf.generate(data, template)

        if output_path:
            with open(output_path, "wb") as f:
                f.write(pdf_bytes)
            return {"success": True, "path": output_path, "size_bytes": len(pdf_bytes)}

        return {"success": True, "size_bytes": len(pdf_bytes)}

    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_sheets_dashboard(
    title: str,
    data: list[dict],
    share_with: list[str] | None = None,
) -> dict:
    """
    Create a Google Sheets dashboard.

    Args:
        title: Spreadsheet title
        data: Data rows as list of dictionaries
        share_with: Email addresses to share with

    Returns:
        URL of created spreadsheet
    """
    import pandas as pd
    from growthnav.reporting import SheetsExporter

    try:
        sheets = SheetsExporter()
        df = pd.DataFrame(data)
        url = sheets.create_dashboard(title, df, share_with=share_with)
        return {"success": True, "url": url}

    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_slides_presentation(
    title: str,
    slides: list[dict],
    share_with: list[str] | None = None,
) -> dict:
    """
    Create a Google Slides presentation.

    Args:
        title: Presentation title
        slides: List of slide definitions with title, body, and optional layout
        share_with: Email addresses to share with

    Returns:
        URL of created presentation
    """
    from growthnav.reporting import SlidesGenerator
    from growthnav.reporting.slides import SlideContent, SlideLayout

    try:
        generator = SlidesGenerator()

        slide_content = [
            SlideContent(
                title=s.get("title", ""),
                body=s.get("body"),
                layout=SlideLayout(s.get("layout", "TITLE_AND_BODY")),
                notes=s.get("notes"),
            )
            for s in slides
        ]

        url = generator.create_presentation(title, slide_content, share_with=share_with)
        return {"success": True, "url": url}

    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# Conversion Tools
# =============================================================================


@mcp.tool()
def normalize_pos_data(
    customer_id: str,
    transactions: list[dict],
) -> list[dict]:
    """
    Normalize Point of Sale transactions to unified conversion format.

    Args:
        customer_id: Customer identifier
        transactions: Raw POS transaction data

    Returns:
        List of normalized conversions
    """
    from growthnav.conversions import POSNormalizer

    normalizer = POSNormalizer(customer_id=customer_id)
    conversions = normalizer.normalize(transactions)

    return [c.to_dict() for c in conversions]


@mcp.tool()
def normalize_crm_data(
    customer_id: str,
    leads: list[dict],
    conversion_type: str = "lead",
) -> list[dict]:
    """
    Normalize CRM leads/opportunities to unified conversion format.

    Args:
        customer_id: Customer identifier
        leads: Raw CRM lead data
        conversion_type: Type of conversion (lead, signup, etc.)

    Returns:
        List of normalized conversions
    """
    from growthnav.conversions import CRMNormalizer
    from growthnav.conversions.schema import ConversionType

    normalizer = CRMNormalizer(
        customer_id=customer_id,
        conversion_type=ConversionType(conversion_type),
    )
    conversions = normalizer.normalize(leads)

    return [c.to_dict() for c in conversions]


# =============================================================================
# Resources
# =============================================================================


@mcp.resource("customer://{customer_id}")
def get_customer_resource(customer_id: str) -> str:
    """Get customer configuration as a resource."""
    from growthnav.bigquery import CustomerRegistry

    registry = CustomerRegistry()
    customer = registry.get_customer(customer_id)

    if customer:
        return f"""Customer: {customer.customer_name}
ID: {customer.customer_id}
Industry: {customer.industry.value}
Dataset: {customer.dataset}
Status: {customer.status.value}
Tags: {', '.join(customer.tags) if customer.tags else 'None'}
"""
    return f"Customer {customer_id} not found"


@mcp.resource("industries://list")
def list_industries() -> str:
    """List available industry categories."""
    from growthnav.bigquery.registry import Industry

    return "\n".join(f"- {i.value}" for i in Industry)


# =============================================================================
# Prompts
# =============================================================================


@mcp.prompt()
def analyze_customer_data(customer_id: str, analysis_type: str = "overview") -> str:
    """
    Prompt for analyzing customer data.

    Args:
        customer_id: Customer to analyze
        analysis_type: Type of analysis (overview, performance, trends)
    """
    return f"""Analyze the data for customer "{customer_id}".

Analysis type: {analysis_type}

Steps:
1. First, get the customer configuration using get_customer("{customer_id}")
2. Query their dataset to understand available tables
3. Run relevant queries based on the analysis type
4. Summarize findings with actionable insights

For {analysis_type} analysis, focus on:
{"- Overall metrics and KPIs" if analysis_type == "overview" else ""}
{"- Performance vs benchmarks and goals" if analysis_type == "performance" else ""}
{"- Week-over-week and month-over-month changes" if analysis_type == "trends" else ""}
"""


@mcp.prompt()
def generate_monthly_report(customer_id: str) -> str:
    """Prompt for generating a monthly report."""
    return f"""Generate a monthly performance report for customer "{customer_id}".

Steps:
1. Query the customer's performance data for the past month
2. Compare to previous month and same month last year
3. Identify top performers and areas needing attention
4. Generate a PDF report using the customer_report template
5. Create a Google Sheets dashboard with detailed data
6. Optionally create a Google Slides presentation for stakeholders

Include:
- Executive summary
- Key metrics (spend, conversions, ROAS)
- Campaign performance breakdown
- Recommendations for next month
"""


# =============================================================================
# Entry Point
# =============================================================================


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
