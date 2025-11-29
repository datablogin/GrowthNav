"""Tests for MCP server tools, resources, and prompts."""

from __future__ import annotations

from unittest.mock import Mock, mock_open, patch

# =============================================================================
# BigQuery Tool Tests
# =============================================================================


@patch("growthnav.bigquery.TenantBigQueryClient")
def test_query_bigquery(mock_client_class):
    """Test BigQuery query execution."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    query_bigquery = mcp._tool_manager._tools["query_bigquery"].fn

    # Setup mock
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_result = Mock()
    mock_result.rows = [{"id": 1, "name": "test"}]
    mock_result.total_rows = 1
    mock_result.bytes_processed = 1024
    mock_result.cache_hit = False
    mock_client.query.return_value = mock_result

    # Execute
    result = query_bigquery(
        customer_id="topgolf",
        sql="SELECT * FROM transactions",
        max_results=100,
    )

    # Verify
    mock_client_class.assert_called_once_with(customer_id="topgolf")
    mock_client.query.assert_called_once_with(
        "SELECT * FROM transactions",
        max_results=100,
    )
    assert result == {
        "rows": [{"id": 1, "name": "test"}],
        "total_rows": 1,
        "bytes_processed": 1024,
        "cache_hit": False,
    }


@patch("growthnav.bigquery.TenantBigQueryClient")
def test_query_bigquery_default_max_results(mock_client_class):
    """Test BigQuery query with default max_results."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    query_bigquery = mcp._tool_manager._tools["query_bigquery"].fn

    # Setup mock
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_result = Mock()
    mock_result.rows = []
    mock_result.total_rows = 0
    mock_result.bytes_processed = 0
    mock_result.cache_hit = True
    mock_client.query.return_value = mock_result

    # Execute with defaults
    query_bigquery(
        customer_id="acme",
        sql="SELECT 1",
    )

    # Verify default max_results
    mock_client.query.assert_called_once_with(
        "SELECT 1",
        max_results=10000,
    )


@patch("growthnav.bigquery.TenantBigQueryClient")
def test_estimate_query_cost(mock_client_class):
    """Test query cost estimation."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    estimate_query_cost = mcp._tool_manager._tools["estimate_query_cost"].fn

    # Setup mock
    mock_client = Mock()
    mock_client_class.return_value = mock_client
    mock_client.estimate_cost.return_value = {
        "bytes_processed": 1024 * 1024 * 100,
        "cost_usd": 0.50,
    }

    # Execute
    result = estimate_query_cost(
        customer_id="topgolf",
        sql="SELECT * FROM large_table",
    )

    # Verify
    mock_client_class.assert_called_once_with(customer_id="topgolf")
    mock_client.estimate_cost.assert_called_once_with("SELECT * FROM large_table")
    assert result == {
        "bytes_processed": 1024 * 1024 * 100,
        "cost_usd": 0.50,
    }


@patch("growthnav.bigquery.TenantBigQueryClient")
def test_get_table_schema(mock_client_class):
    """Test table schema retrieval."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    get_table_schema = mcp._tool_manager._tools["get_table_schema"].fn

    # Setup mock
    mock_client = Mock()
    mock_client_class.return_value = mock_client
    mock_client.get_table_schema.return_value = [
        {
            "name": "transaction_id",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Unique transaction ID",
        },
        {
            "name": "amount",
            "type": "FLOAT64",
            "mode": "NULLABLE",
            "description": "Transaction amount",
        },
    ]

    # Execute
    result = get_table_schema(
        customer_id="topgolf",
        table_name="transactions",
    )

    # Verify
    mock_client_class.assert_called_once_with(customer_id="topgolf")
    mock_client.get_table_schema.assert_called_once_with("transactions")
    assert len(result) == 2
    assert result[0]["name"] == "transaction_id"
    assert result[1]["name"] == "amount"


# =============================================================================
# Customer Registry Tool Tests
# =============================================================================


@patch("growthnav.bigquery.CustomerRegistry")
def test_get_customer_found(mock_registry_class):
    """Test get_customer when customer exists."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    get_customer = mcp._tool_manager._tools["get_customer"].fn

    # Setup mock
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry

    mock_customer = Mock()
    mock_customer.customer_id = "topgolf"
    mock_customer.customer_name = "TopGolf"
    mock_customer.industry.value = "golf"
    mock_customer.dataset = "growthnav_topgolf"
    mock_customer.status.value = "active"
    mock_customer.tags = ["enterprise", "premium"]

    mock_registry.get_customer.return_value = mock_customer

    # Execute
    result = get_customer("topgolf")

    # Verify
    mock_registry.get_customer.assert_called_once_with("topgolf")
    assert result == {
        "customer_id": "topgolf",
        "customer_name": "TopGolf",
        "industry": "golf",
        "dataset": "growthnav_topgolf",
        "status": "active",
        "tags": ["enterprise", "premium"],
    }


@patch("growthnav.bigquery.CustomerRegistry")
def test_get_customer_not_found(mock_registry_class):
    """Test get_customer when customer doesn't exist."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    get_customer = mcp._tool_manager._tools["get_customer"].fn

    # Setup mock
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry
    mock_registry.get_customer.return_value = None

    # Execute
    result = get_customer("nonexistent")

    # Verify
    mock_registry.get_customer.assert_called_once_with("nonexistent")
    assert result is None


@patch("growthnav.bigquery.registry.Industry")
@patch("growthnav.bigquery.CustomerRegistry")
def test_list_customers_by_industry(mock_registry_class, mock_industry_class):
    """Test listing customers by industry."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    list_customers_by_industry = mcp._tool_manager._tools["list_customers_by_industry"].fn

    # Setup mocks
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry

    # Mock Industry enum
    mock_industry = Mock()
    mock_industry_class.return_value = mock_industry

    # Mock customer objects
    mock_customer1 = Mock()
    mock_customer1.customer_id = "topgolf"
    mock_customer1.customer_name = "TopGolf"
    mock_customer1.dataset = "growthnav_topgolf"
    mock_customer1.tags = ["enterprise"]

    mock_customer2 = Mock()
    mock_customer2.customer_id = "pga"
    mock_customer2.customer_name = "PGA Tour"
    mock_customer2.dataset = "growthnav_pga"
    mock_customer2.tags = []

    mock_registry.get_customers_by_industry.return_value = [
        mock_customer1,
        mock_customer2,
    ]

    # Execute
    result = list_customers_by_industry("golf")

    # Verify
    mock_industry_class.assert_called_once_with("golf")
    mock_registry.get_customers_by_industry.assert_called_once_with(mock_industry)
    assert len(result) == 2
    assert result[0]["customer_id"] == "topgolf"
    assert result[1]["customer_id"] == "pga"


# =============================================================================
# Reporting Tool Tests
# =============================================================================


@patch("growthnav.reporting.PDFGenerator")
def test_generate_pdf_report_success(mock_pdf_class):
    """Test successful PDF report generation."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    generate_pdf_report = mcp._tool_manager._tools["generate_pdf_report"].fn

    # Setup mock
    mock_pdf = Mock()
    mock_pdf_class.return_value = mock_pdf
    mock_pdf.is_available.return_value = True
    mock_pdf.generate.return_value = b"PDF content here"

    # Execute without output_path
    result = generate_pdf_report(
        template="customer_report",
        data={"customer": "TopGolf", "revenue": 1000000},
    )

    # Verify
    mock_pdf.is_available.assert_called_once()
    mock_pdf.generate.assert_called_once_with(
        {"customer": "TopGolf", "revenue": 1000000},
        "customer_report",
    )
    assert result == {
        "success": True,
        "size_bytes": 16,
    }


@patch("builtins.open", new_callable=mock_open)
@patch("growthnav.reporting.PDFGenerator")
def test_generate_pdf_report_with_output_path(mock_pdf_class, mock_file):
    """Test PDF report generation with file output."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    generate_pdf_report = mcp._tool_manager._tools["generate_pdf_report"].fn

    # Setup mock
    mock_pdf = Mock()
    mock_pdf_class.return_value = mock_pdf
    mock_pdf.is_available.return_value = True
    mock_pdf.generate.return_value = b"PDF content"

    # Execute with output_path
    result = generate_pdf_report(
        template="audit_summary",
        data={"metrics": [1, 2, 3]},
        output_path="/tmp/report.pdf",
    )

    # Verify file was written
    mock_file.assert_called_once_with("/tmp/report.pdf", "wb")
    mock_file().write.assert_called_once_with(b"PDF content")
    assert result == {
        "success": True,
        "path": "/tmp/report.pdf",
        "size_bytes": 11,
    }


@patch("growthnav.reporting.PDFGenerator")
def test_generate_pdf_report_unavailable(mock_pdf_class):
    """Test PDF generation when WeasyPrint is unavailable."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    generate_pdf_report = mcp._tool_manager._tools["generate_pdf_report"].fn

    # Setup mock
    mock_pdf = Mock()
    mock_pdf_class.return_value = mock_pdf
    mock_pdf.is_available.return_value = False

    # Execute
    result = generate_pdf_report(
        template="test",
        data={},
    )

    # Verify
    assert result == {
        "success": False,
        "error": "WeasyPrint not available. Install with: pip install weasyprint",
    }
    mock_pdf.generate.assert_not_called()


@patch("growthnav.reporting.PDFGenerator")
def test_generate_pdf_report_error(mock_pdf_class):
    """Test PDF generation error handling."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    generate_pdf_report = mcp._tool_manager._tools["generate_pdf_report"].fn

    # Setup mock
    mock_pdf = Mock()
    mock_pdf_class.return_value = mock_pdf
    mock_pdf.is_available.return_value = True
    mock_pdf.generate.side_effect = ValueError("Invalid template")

    # Execute
    result = generate_pdf_report(
        template="invalid",
        data={},
    )

    # Verify
    assert result == {
        "success": False,
        "error": "Invalid template",
    }


@patch("pandas.DataFrame")
@patch("growthnav.reporting.SheetsExporter")
def test_create_sheets_dashboard_success(mock_sheets_class, mock_dataframe):
    """Test successful Sheets dashboard creation."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    create_sheets_dashboard = mcp._tool_manager._tools["create_sheets_dashboard"].fn

    # Setup mocks
    mock_sheets = Mock()
    mock_sheets_class.return_value = mock_sheets
    mock_sheets.create_dashboard.return_value = "https://docs.google.com/spreadsheets/d/123"

    mock_df = Mock()
    mock_dataframe.return_value = mock_df

    # Execute
    data = [{"metric": "revenue", "value": 1000}]
    result = create_sheets_dashboard(
        title="Q4 Dashboard",
        data=data,
        share_with=["user@example.com"],
    )

    # Verify
    mock_dataframe.assert_called_once_with(data)
    mock_sheets.create_dashboard.assert_called_once_with(
        "Q4 Dashboard",
        mock_df,
        share_with=["user@example.com"],
    )
    assert result == {
        "success": True,
        "url": "https://docs.google.com/spreadsheets/d/123",
    }


@patch("pandas.DataFrame")
@patch("growthnav.reporting.SheetsExporter")
def test_create_sheets_dashboard_error(mock_sheets_class, mock_dataframe):
    """Test Sheets dashboard creation error handling."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    create_sheets_dashboard = mcp._tool_manager._tools["create_sheets_dashboard"].fn

    # Setup mock to raise error
    mock_sheets_class.side_effect = RuntimeError("Auth failed")

    # Execute
    result = create_sheets_dashboard(
        title="Test",
        data=[],
    )

    # Verify
    assert result == {
        "success": False,
        "error": "Auth failed",
    }


@patch("growthnav.reporting.slides.SlideLayout")
@patch("growthnav.reporting.slides.SlideContent")
@patch("growthnav.reporting.SlidesGenerator")
def test_create_slides_presentation_success(
    mock_generator_class, mock_content_class, mock_layout_class
):
    """Test successful Slides presentation creation."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    create_slides_presentation = mcp._tool_manager._tools["create_slides_presentation"].fn

    # Setup mocks
    mock_generator = Mock()
    mock_generator_class.return_value = mock_generator
    mock_generator.create_presentation.return_value = "https://docs.google.com/presentation/d/456"

    # Mock SlideLayout enum
    mock_layout = Mock()
    mock_layout_class.return_value = mock_layout

    # Mock SlideContent creation
    mock_slide1 = Mock()
    mock_slide2 = Mock()
    mock_content_class.side_effect = [mock_slide1, mock_slide2]

    # Execute
    slides_data = [
        {"title": "Introduction", "body": "Welcome", "layout": "TITLE_ONLY"},
        {"title": "Results", "body": "Great results", "notes": "Presenter notes"},
    ]
    result = create_slides_presentation(
        title="Q4 Review",
        slides=slides_data,
        share_with=["team@example.com"],
    )

    # Verify
    assert mock_content_class.call_count == 2
    mock_generator.create_presentation.assert_called_once()
    args = mock_generator.create_presentation.call_args
    assert args[0][0] == "Q4 Review"
    assert len(args[0][1]) == 2
    assert args[1] == {"share_with": ["team@example.com"]}

    assert result == {
        "success": True,
        "url": "https://docs.google.com/presentation/d/456",
    }


@patch("growthnav.reporting.SlidesGenerator")
def test_create_slides_presentation_error(mock_generator_class):
    """Test Slides presentation creation error handling."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    create_slides_presentation = mcp._tool_manager._tools["create_slides_presentation"].fn

    # Setup mock to raise error
    mock_generator_class.side_effect = Exception("API error")

    # Execute
    result = create_slides_presentation(
        title="Test",
        slides=[],
    )

    # Verify
    assert result == {
        "success": False,
        "error": "API error",
    }


# =============================================================================
# Conversion Tool Tests
# =============================================================================


@patch("growthnav.conversions.POSNormalizer")
def test_normalize_pos_data(mock_normalizer_class):
    """Test POS data normalization."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    normalize_pos_data = mcp._tool_manager._tools["normalize_pos_data"].fn

    # Setup mocks
    mock_normalizer = Mock()
    mock_normalizer_class.return_value = mock_normalizer

    mock_conversion1 = Mock()
    mock_conversion1.to_dict.return_value = {
        "conversion_id": "conv_1",
        "customer_id": "topgolf",
        "conversion_type": "purchase",
        "value": 100.0,
    }

    mock_conversion2 = Mock()
    mock_conversion2.to_dict.return_value = {
        "conversion_id": "conv_2",
        "customer_id": "topgolf",
        "conversion_type": "purchase",
        "value": 250.0,
    }

    mock_normalizer.normalize.return_value = [mock_conversion1, mock_conversion2]

    # Execute
    transactions = [
        {"transaction_id": "tx1", "amount": 100},
        {"transaction_id": "tx2", "amount": 250},
    ]
    result = normalize_pos_data(
        customer_id="topgolf",
        transactions=transactions,
    )

    # Verify
    mock_normalizer_class.assert_called_once_with(customer_id="topgolf")
    mock_normalizer.normalize.assert_called_once_with(transactions)
    assert len(result) == 2
    assert result[0]["conversion_id"] == "conv_1"
    assert result[1]["value"] == 250.0


@patch("growthnav.conversions.schema.ConversionType")
@patch("growthnav.conversions.CRMNormalizer")
def test_normalize_crm_data(mock_normalizer_class, mock_conversion_type_class):
    """Test CRM data normalization."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    normalize_crm_data = mcp._tool_manager._tools["normalize_crm_data"].fn

    # Setup mocks
    mock_normalizer = Mock()
    mock_normalizer_class.return_value = mock_normalizer

    mock_conversion_type = Mock()
    mock_conversion_type_class.return_value = mock_conversion_type

    mock_conversion = Mock()
    mock_conversion.to_dict.return_value = {
        "conversion_id": "lead_1",
        "customer_id": "acme",
        "conversion_type": "lead",
        "value": 0.0,
    }

    mock_normalizer.normalize.return_value = [mock_conversion]

    # Execute
    leads = [{"lead_id": "l1", "email": "user@example.com"}]
    result = normalize_crm_data(
        customer_id="acme",
        leads=leads,
        conversion_type="lead",
    )

    # Verify
    mock_conversion_type_class.assert_called_once_with("lead")
    mock_normalizer_class.assert_called_once_with(
        customer_id="acme",
        conversion_type=mock_conversion_type,
    )
    mock_normalizer.normalize.assert_called_once_with(leads)
    assert len(result) == 1
    assert result[0]["conversion_type"] == "lead"


@patch("growthnav.conversions.schema.ConversionType")
@patch("growthnav.conversions.CRMNormalizer")
def test_normalize_crm_data_default_type(mock_normalizer_class, mock_conversion_type_class):
    """Test CRM data normalization with default conversion type."""
    from growthnav_mcp.server import mcp

    # Get the underlying function
    normalize_crm_data = mcp._tool_manager._tools["normalize_crm_data"].fn

    # Setup mocks
    mock_normalizer = Mock()
    mock_normalizer_class.return_value = mock_normalizer
    mock_normalizer.normalize.return_value = []

    mock_conversion_type = Mock()
    mock_conversion_type_class.return_value = mock_conversion_type

    # Execute with default conversion_type
    normalize_crm_data(
        customer_id="test",
        leads=[],
    )

    # Verify default type is "lead"
    mock_conversion_type_class.assert_called_once_with("lead")


# =============================================================================
# Resource Tests
# =============================================================================


@patch("growthnav.bigquery.CustomerRegistry")
def test_get_customer_resource_found(mock_registry_class):
    """Test customer resource when customer exists."""
    from growthnav_mcp.server import mcp

    # Get the underlying function from resource templates
    get_customer_resource = mcp._resource_manager._templates["customer://{customer_id}"].fn

    # Setup mock
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry

    mock_customer = Mock()
    mock_customer.customer_name = "TopGolf"
    mock_customer.customer_id = "topgolf"
    mock_customer.industry.value = "golf"
    mock_customer.dataset = "growthnav_topgolf"
    mock_customer.status.value = "active"
    mock_customer.tags = ["enterprise", "premium"]

    mock_registry.get_customer.return_value = mock_customer

    # Execute
    result = get_customer_resource("topgolf")

    # Verify
    expected = """Customer: TopGolf
ID: topgolf
Industry: golf
Dataset: growthnav_topgolf
Status: active
Tags: enterprise, premium
"""
    assert result == expected


@patch("growthnav.bigquery.CustomerRegistry")
def test_get_customer_resource_not_found(mock_registry_class):
    """Test customer resource when customer doesn't exist."""
    from growthnav_mcp.server import mcp

    # Get the underlying function from resource templates
    get_customer_resource = mcp._resource_manager._templates["customer://{customer_id}"].fn

    # Setup mock
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry
    mock_registry.get_customer.return_value = None

    # Execute
    result = get_customer_resource("nonexistent")

    # Verify
    assert result == "Customer nonexistent not found"


@patch("growthnav.bigquery.CustomerRegistry")
def test_get_customer_resource_no_tags(mock_registry_class):
    """Test customer resource formatting with no tags."""
    from growthnav_mcp.server import mcp

    # Get the underlying function from resource templates
    get_customer_resource = mcp._resource_manager._templates["customer://{customer_id}"].fn

    # Setup mock
    mock_registry = Mock()
    mock_registry_class.return_value = mock_registry

    mock_customer = Mock()
    mock_customer.customer_name = "Acme Corp"
    mock_customer.customer_id = "acme"
    mock_customer.industry.value = "retail"
    mock_customer.dataset = "growthnav_acme"
    mock_customer.status.value = "trial"
    mock_customer.tags = []

    mock_registry.get_customer.return_value = mock_customer

    # Execute
    result = get_customer_resource("acme")

    # Verify
    assert "Tags: None" in result


@patch("growthnav.bigquery.registry.Industry")
def test_list_industries(mock_industry_class):
    """Test industries resource."""
    from growthnav_mcp.server import mcp

    # Get the underlying function from static resources
    list_industries = mcp._resource_manager._resources["industries://list"].fn

    # Setup mock Industry enum
    mock_golf = Mock()
    mock_golf.value = "golf"

    mock_medical = Mock()
    mock_medical.value = "medical"

    mock_restaurant = Mock()
    mock_restaurant.value = "restaurant"

    mock_industry_class.__iter__ = Mock(
        return_value=iter([mock_golf, mock_medical, mock_restaurant])
    )

    # Execute
    result = list_industries()

    # Verify
    assert "- golf" in result
    assert "- medical" in result
    assert "- restaurant" in result


# =============================================================================
# Prompt Tests
# =============================================================================


def test_analyze_customer_data_prompt_overview():
    """Test analyze_customer_data prompt with overview type."""
    from growthnav_mcp.server import mcp

    # Get the prompt function from the prompt manager
    prompt_fn = mcp._prompt_manager._prompts["analyze_customer_data"].fn

    # Execute
    result = prompt_fn(
        customer_id="topgolf",
        analysis_type="overview",
    )

    # Verify
    assert 'customer "topgolf"' in result
    assert "Analysis type: overview" in result
    assert 'get_customer("topgolf")' in result
    assert "Overall metrics and KPIs" in result


def test_analyze_customer_data_prompt_performance():
    """Test analyze_customer_data prompt with performance type."""
    from growthnav_mcp.server import mcp

    # Get the prompt function from the prompt manager
    prompt_fn = mcp._prompt_manager._prompts["analyze_customer_data"].fn

    # Execute
    result = prompt_fn(
        customer_id="acme",
        analysis_type="performance",
    )

    # Verify
    assert 'customer "acme"' in result
    assert "Analysis type: performance" in result
    assert "Performance vs benchmarks and goals" in result


def test_analyze_customer_data_prompt_trends():
    """Test analyze_customer_data prompt with trends type."""
    from growthnav_mcp.server import mcp

    # Get the prompt function from the prompt manager
    prompt_fn = mcp._prompt_manager._prompts["analyze_customer_data"].fn

    # Execute
    result = prompt_fn(
        customer_id="test",
        analysis_type="trends",
    )

    # Verify
    assert 'customer "test"' in result
    assert "Analysis type: trends" in result
    assert "Week-over-week and month-over-month changes" in result


def test_analyze_customer_data_prompt_default():
    """Test analyze_customer_data prompt with default type."""
    from growthnav_mcp.server import mcp

    # Get the prompt function from the prompt manager
    prompt_fn = mcp._prompt_manager._prompts["analyze_customer_data"].fn

    # Execute with default
    result = prompt_fn(customer_id="default")

    # Verify default is overview
    assert "Analysis type: overview" in result
    assert "Overall metrics and KPIs" in result


def test_generate_monthly_report_prompt():
    """Test generate_monthly_report prompt."""
    from growthnav_mcp.server import mcp

    # Get the prompt function from the prompt manager
    prompt_fn = mcp._prompt_manager._prompts["generate_monthly_report"].fn

    # Execute
    result = prompt_fn(customer_id="topgolf")

    # Verify
    assert 'customer "topgolf"' in result
    assert "monthly performance report" in result
    assert "Executive summary" in result
    assert "Key metrics (spend, conversions, ROAS)" in result
    assert "Campaign performance breakdown" in result
    assert "Recommendations for next month" in result
    assert "PDF report" in result
    assert "Google Sheets dashboard" in result
    assert "Google Slides presentation" in result
