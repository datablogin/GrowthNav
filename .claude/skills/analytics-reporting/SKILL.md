# Analytics Reporting Skill

## Overview

This skill guides you through creating analytics reports for GrowthNav customers using the shared reporting infrastructure. GrowthNav provides PDF, Google Sheets, and Google Slides generation capabilities.

## When to Use This Skill

- Generating PDF reports for customer presentations
- Creating Google Sheets dashboards for data analysis
- Building Google Slides presentations for executive summaries
- Exporting data in various formats (HTML, PDF, Sheets, Slides)

## Available MCP Tools

### BigQuery Data Retrieval

Before generating reports, query customer data:

```
query_bigquery(
    customer_id="topgolf",
    sql="SELECT * FROM daily_metrics WHERE date >= '2025-01-01' LIMIT 1000"
)
```

### PDF Report Generation

```
generate_pdf_report(
    template="customer_report",
    data={
        "customer_name": "Topgolf",
        "report_date": "January 2025",
        "metrics": {...}
    },
    output_path="/path/to/output.pdf"  # optional
)
```

**Returns**: `{success: true, size_bytes: N}` or `{success: false, error: "..."}`

### Google Sheets Dashboard

```
create_sheets_dashboard(
    title="Monthly Performance Dashboard - January 2025",
    data=[
        {"date": "2025-01-01", "conversions": 150, "revenue": 15000},
        {"date": "2025-01-02", "conversions": 175, "revenue": 17500}
    ],
    share_with=["analyst@company.com", "manager@company.com"]
)
```

**Returns**: `{success: true, url: "https://docs.google.com/spreadsheets/d/..."}`

### Multi-Tab Sheets Dashboard

```
create_sheets_dashboard(
    title="Q4 Performance Report",
    tabs={
        "Summary": [{"metric": "Total Revenue", "value": 150000}],
        "Daily Detail": [...daily data...],
        "Campaign Breakdown": [...campaign data...]
    },
    share_with=["executive@company.com"]
)
```

### Google Slides Presentation

```
create_slides_presentation(
    title="Q4 Executive Summary",
    slides=[
        {"title": "Overview", "body": "Key highlights for Q4 2024..."},
        {"title": "Performance Metrics", "body": ["Revenue: $1.5M", "Conversions: 12,500", "ROAS: 4.2x"]},
        {"title": "Top Campaigns", "body": "Analysis of best performers..."}
    ],
    share_with=["ceo@company.com"]
)
```

**Returns**: `{success: true, url: "https://docs.google.com/presentation/d/..."}`

## Report Templates

### Available PDF Templates

Templates are located in `growthnav/reporting/templates/`:

| Template | Purpose | Required Data |
|----------|---------|---------------|
| `customer_report` | Monthly customer report | customer_name, metrics, date_range |
| `audit_summary` | Campaign audit results | customer_name, findings, recommendations |
| `performance_dashboard` | KPI dashboard | metrics, charts_data |

### Custom Templates

Create new templates using Jinja2 syntax:

```html
<!DOCTYPE html>
<html>
<head><title>{{ customer_name }} Report</title></head>
<body>
    <h1>{{ report_title }}</h1>
    <p>Generated: {{ generated_at }}</p>

    <h2>Metrics</h2>
    <table>
        {% for metric in metrics %}
        <tr>
            <td>{{ metric.name }}</td>
            <td>{{ metric.value | format_currency }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
```

### Template Filters

Available Jinja2 filters:
- `format_currency`: `{{ 1234.56 | format_currency }}` → `$1,234.56`
- `format_percent`: `{{ 0.1234 | format_percent }}` → `12.3%`
- `format_number`: `{{ 1234567 | format_number }}` → `1,234,567`

## Workflow Examples

### Monthly Customer Report

```python
# 1. Query customer metrics
metrics = query_bigquery(
    customer_id="topgolf",
    sql="""
        SELECT
            SUM(conversions) as total_conversions,
            SUM(revenue) as total_revenue,
            AVG(roas) as avg_roas
        FROM daily_metrics
        WHERE date BETWEEN '2025-01-01' AND '2025-01-31'
    """
)

# 2. Generate PDF report
pdf_result = generate_pdf_report(
    template="monthly_summary",
    data={
        "customer_name": "Topgolf",
        "month": "January 2025",
        "metrics": metrics["rows"][0]
    }
)

# 3. Create detailed Sheets dashboard
sheets_url = create_sheets_dashboard(
    title="Topgolf - January 2025 Performance",
    data=query_bigquery(
        customer_id="topgolf",
        sql="SELECT * FROM daily_metrics WHERE date >= '2025-01-01'"
    )["rows"],
    share_with=["marketing@topgolf.com"]
)
```

### Executive Presentation

```python
# 1. Get high-level metrics
summary = query_bigquery(customer_id="topgolf", sql="...")

# 2. Create slides with key insights
presentation = create_slides_presentation(
    title="Topgolf Q4 2024 Review",
    slides=[
        {
            "title": "Q4 Performance Highlights",
            "body": [
                f"Revenue: ${summary['revenue']:,.0f}",
                f"Conversions: {summary['conversions']:,}",
                f"ROAS: {summary['roas']:.1f}x"
            ]
        },
        {
            "title": "Channel Performance",
            "body": "Google Ads drove 65% of conversions..."
        },
        {
            "title": "Recommendations for Q1",
            "body": [
                "Increase budget for top campaigns",
                "Test new audience segments",
                "Optimize mobile landing pages"
            ]
        }
    ],
    share_with=["executive@topgolf.com"]
)
```

## Best Practices

### Data Preparation

1. **Always use LIMIT**: Prevent runaway queries
2. **Validate data**: Check for nulls before rendering
3. **Include timestamps**: Show data freshness

```python
# Good: Limit results and include timestamp
data = query_bigquery(
    customer_id="topgolf",
    sql="""
        SELECT *, CURRENT_TIMESTAMP() as query_time
        FROM daily_metrics
        ORDER BY date DESC
        LIMIT 365
    """
)
```

### Google Workspace Integration

1. **Permissions**: Share with appropriate access levels
2. **Folder organization**: Use `folder_id` to organize in Drive
3. **Rate limits**: Add delays between bulk operations

### Report Formatting

1. **Consistent branding**: Use company colors and logos
2. **Data freshness**: Always show when data was pulled
3. **Action items**: End reports with clear next steps

## Troubleshooting

### PDF Generation Fails

**Error**: "WeasyPrint not available"
- WeasyPrint requires system dependencies (cairo, pango)
- Use HTML export as fallback

**Error**: "Template not found"
- Check template name matches file in templates directory
- Template extension should be `.html.j2`

### Sheets/Slides Permission Errors

**Error**: "Permission denied"
- Verify service account has appropriate permissions
- Check GOOGLE_APPLICATION_CREDENTIALS is set
- Ensure service account has Sheets/Slides API access

### Empty Reports

- Verify BigQuery query returns data
- Check date ranges in queries
- Confirm customer_id matches registry

## Related Resources

- MCP Resource: `customer://{customer_id}/config` - Get customer configuration
- MCP Resource: `template://pdf/{template_name}` - List available templates
- MCP Prompt: `generate_monthly_report` - Guided monthly report workflow
