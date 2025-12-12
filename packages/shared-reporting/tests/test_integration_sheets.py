"""
Real integration tests for Google Sheets exporter.

These tests hit actual Google Sheets API - no mocks.

Prerequisites:
- GCP authentication via service account with Google Workspace access
- Set RUN_SHEETS_INTEGRATION_TESTS=1 to enable these tests
- Permissions: Google Sheets API and Google Drive API enabled

Run with:
    RUN_SHEETS_INTEGRATION_TESTS=1 uv run pytest packages/shared-reporting/tests/test_integration_sheets.py -v
"""

import os
import time
from collections.abc import Generator
from datetime import datetime

import gspread
import pandas as pd
import pytest
from growthnav.reporting.sheets import SheetsExporter

from conftest import (
    CLEANUP_DELAY,
    RATE_LIMIT_DELAY,
    TEST_SHARE_EMAIL,
    get_cleanup_credentials,
)

# Skip all tests unless explicitly enabled via environment variable
# These tests require Google Workspace access which service accounts may not have
pytestmark = pytest.mark.skipif(
    not os.getenv("RUN_SHEETS_INTEGRATION_TESTS"),
    reason="Set RUN_SHEETS_INTEGRATION_TESTS=1 to run Google Sheets integration tests",
)


@pytest.fixture(autouse=True)
def rate_limit_delay() -> Generator[None, None, None]:
    """Add delay between tests to avoid rate limiting."""
    yield
    time.sleep(RATE_LIMIT_DELAY)


@pytest.fixture
def exporter() -> SheetsExporter:
    """Create a SheetsExporter instance."""
    return SheetsExporter()


@pytest.fixture
def created_spreadsheets() -> Generator[list[str], None, None]:
    """Track created spreadsheets for cleanup."""
    spreadsheet_ids: list[str] = []

    yield spreadsheet_ids

    # Cleanup: delete all created spreadsheets
    if spreadsheet_ids:
        creds = get_cleanup_credentials()
        if creds is None:
            print("Warning: Could not get credentials for cleanup")
            return

        client = gspread.authorize(creds)

        for spreadsheet_id in spreadsheet_ids:
            try:
                spreadsheet = client.open_by_key(spreadsheet_id)
                client.del_spreadsheet(spreadsheet_id)
                print(f"Cleaned up spreadsheet: {spreadsheet.title}")
                if spreadsheet_id != spreadsheet_ids[-1]:
                    time.sleep(CLEANUP_DELAY)
            except Exception as e:
                print(f"Failed to cleanup spreadsheet {spreadsheet_id}: {e}")


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "customer": ["Topgolf", "ClubCorp", "PGA Tour"],
        "revenue": [100000, 150000, 200000],
        "conversions": [250, 350, 450],
        "date": [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
    })


@pytest.fixture
def sample_list_data() -> list[dict[str, str | float]]:
    """Create sample list of dicts for testing."""
    return [
        {"metric": "CTR", "value": 0.025, "status": "Good"},
        {"metric": "CPC", "value": 1.50, "status": "Average"},
        {"metric": "ROAS", "value": 4.2, "status": "Excellent"},
    ]


class TestSheetsExporterIntegration:
    """Real integration tests for SheetsExporter."""

    @pytest.mark.integration
    def test_create_dashboard_returns_url(self, exporter, sample_dataframe, created_spreadsheets):
        """Create a simple dashboard and verify URL is returned."""
        url = exporter.create_dashboard(
            title=f"Integration Test - Simple Dashboard - {datetime.now().isoformat()}",
            data=sample_dataframe,
        )

        # Extract spreadsheet ID from URL for cleanup
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify URL format and type
        assert isinstance(url, str)
        assert url.startswith("https://docs.google.com/spreadsheets/d/")
        assert len(spreadsheet_id) > 0

        # Verify we can open the spreadsheet
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        assert spreadsheet is not None
        assert "Integration Test" in spreadsheet.title

    @pytest.mark.integration
    def test_create_dashboard_with_dataframe(self, exporter, sample_dataframe, created_spreadsheets):
        """Create dashboard from pandas DataFrame and verify data is written."""
        url = exporter.create_dashboard(
            title=f"Integration Test - DataFrame Dashboard - {datetime.now().isoformat()}",
            data=sample_dataframe,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Open spreadsheet and verify data
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")

        # Get all values
        values = worksheet.get_all_values()

        # Verify headers (first row)
        headers = values[0]
        assert "customer" in headers
        assert "revenue" in headers
        assert "conversions" in headers
        assert "date" in headers

        # Verify we have data rows (header + 3 data rows)
        assert len(values) == 4

        # Verify first data row
        first_data_row = values[1]
        assert "Topgolf" in first_data_row
        assert "100000" in first_data_row or 100000 in first_data_row

    @pytest.mark.integration
    def test_create_dashboard_with_list_of_dicts(self, exporter, sample_list_data, created_spreadsheets):
        """Create dashboard from list of dicts."""
        url = exporter.create_dashboard(
            title=f"Integration Test - List Data - {datetime.now().isoformat()}",
            data=sample_list_data,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify data was written
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        values = worksheet.get_all_values()

        # Verify headers
        headers = values[0]
        assert "metric" in headers
        assert "value" in headers
        assert "status" in headers

        # Verify data rows
        assert len(values) == 4  # Header + 3 rows

    @pytest.mark.integration
    def test_create_multi_tab_dashboard(self, exporter, sample_dataframe, created_spreadsheets):
        """Create multi-tab dashboard with multiple sheets."""
        tabs = {
            "Revenue": pd.DataFrame({
                "month": ["Jan", "Feb", "Mar"],
                "revenue": [100000, 120000, 150000],
            }),
            "Conversions": pd.DataFrame({
                "month": ["Jan", "Feb", "Mar"],
                "conversions": [250, 300, 375],
            }),
            "Metrics": pd.DataFrame({
                "metric": ["CTR", "CPC", "ROAS"],
                "value": [0.025, 1.50, 4.2],
            }),
        }

        url = exporter.create_multi_tab_dashboard(
            title=f"Integration Test - Multi Tab - {datetime.now().isoformat()}",
            tabs=tabs,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify all tabs exist
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheets = spreadsheet.worksheets()
        worksheet_titles = [ws.title for ws in worksheets]

        assert "Revenue" in worksheet_titles
        assert "Conversions" in worksheet_titles
        assert "Metrics" in worksheet_titles

        # Verify data in first tab
        revenue_sheet = spreadsheet.worksheet("Revenue")
        values = revenue_sheet.get_all_values()
        assert values[0] == ["month", "revenue"]
        assert len(values) == 4  # Header + 3 rows

    @pytest.mark.integration
    def test_dashboard_data_is_written(self, exporter, sample_dataframe, created_spreadsheets):
        """Verify actual data appears correctly in the sheet."""
        url = exporter.create_dashboard(
            title=f"Integration Test - Data Verification - {datetime.now().isoformat()}",
            data=sample_dataframe,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Get data from sheet
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")

        # Get all records (converts to list of dicts)
        records = worksheet.get_all_records()

        # Verify we have 3 records
        assert len(records) == 3

        # Verify first record
        first_record = records[0]
        assert first_record["customer"] == "Topgolf"
        assert first_record["revenue"] == 100000
        assert first_record["conversions"] == 250

        # Verify second record
        second_record = records[1]
        assert second_record["customer"] == "ClubCorp"
        assert second_record["revenue"] == 150000
        assert second_record["conversions"] == 350

    @pytest.mark.integration
    @pytest.mark.rate_limited
    @pytest.mark.flaky(reruns=2, reruns_delay=10)
    def test_share_with_users(
        self,
        exporter: SheetsExporter,
        sample_dataframe: pd.DataFrame,
        created_spreadsheets: list[str],
    ) -> None:
        """Test sharing spreadsheet with users."""
        url = exporter.create_dashboard(
            title=f"Integration Test - Sharing - {datetime.now().isoformat()}",
            data=sample_dataframe,
            share_with=[TEST_SHARE_EMAIL],
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify spreadsheet was created and shared
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)

        # Get permissions (this requires Drive API access)
        permissions = spreadsheet.list_permissions()

        # Verify the test email has access
        shared_emails = [p.get("emailAddress") for p in permissions if "emailAddress" in p]
        assert TEST_SHARE_EMAIL in shared_emails

    @pytest.mark.integration
    def test_update_existing_sheet(self, exporter, sample_dataframe, created_spreadsheets):
        """Test updating data in an existing sheet."""
        # Create initial dashboard
        url = exporter.create_dashboard(
            title=f"Integration Test - Update Sheet - {datetime.now().isoformat()}",
            data=sample_dataframe,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Create new data to update with
        new_data = pd.DataFrame({
            "customer": ["Updated Customer 1", "Updated Customer 2"],
            "revenue": [999999, 888888],
            "conversions": [100, 200],
        })

        # Update the sheet
        exporter.update_sheet(
            spreadsheet_id=spreadsheet_id,
            data=new_data,
            sheet_name="Data",
        )

        # Verify updated data
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        records = worksheet.get_all_records()

        # Verify we have 2 records (not 3 from original)
        assert len(records) == 2

        # Verify updated data
        assert records[0]["customer"] == "Updated Customer 1"
        assert records[0]["revenue"] == 999999
        assert records[1]["customer"] == "Updated Customer 2"
        assert records[1]["revenue"] == 888888

    @pytest.mark.integration
    def test_create_dashboard_with_special_characters(self, exporter, created_spreadsheets):
        """Test dashboard with special characters and edge cases."""
        special_data = pd.DataFrame({
            "name": ["Test & Co.", "O'Reilly", 'Quote "Test"'],
            "value": [100, 200, 300],
            "notes": ["Special: <>&", "Unicode: 🎯", "Mixed: ABC123!@#"],
        })

        url = exporter.create_dashboard(
            title=f"Integration Test - Special Chars - {datetime.now().isoformat()}",
            data=special_data,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify data was written correctly
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        records = worksheet.get_all_records()

        assert len(records) == 3
        assert records[0]["name"] == "Test & Co."
        assert records[1]["name"] == "O'Reilly"

    @pytest.mark.integration
    def test_create_dashboard_with_datetime_serialization(self, exporter, created_spreadsheets):
        """Test that datetime values are properly serialized."""
        datetime_data = pd.DataFrame({
            "event": ["Launch", "Update", "Review"],
            "date": [
                datetime(2024, 1, 15, 10, 30, 0),
                datetime(2024, 2, 20, 14, 45, 0),
                datetime(2024, 3, 25, 9, 15, 0),
            ],
            "status": ["Complete", "In Progress", "Planned"],
        })

        url = exporter.create_dashboard(
            title=f"Integration Test - Datetime - {datetime.now().isoformat()}",
            data=datetime_data,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify datetime was serialized (as ISO format string)
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        values = worksheet.get_all_values()

        # Check that dates are serialized
        assert "2024-01-15T10:30:00" in values[1]
        assert "2024-02-20T14:45:00" in values[2]
        assert "2024-03-25T09:15:00" in values[3]

    @pytest.mark.integration
    def test_create_dashboard_with_nan_values(self, exporter, created_spreadsheets):
        """Test that NaN values are handled properly."""
        nan_data = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, None, 87.3],
            "notes": ["Good", None, "Excellent"],
        })

        url = exporter.create_dashboard(
            title=f"Integration Test - NaN Values - {datetime.now().isoformat()}",
            data=nan_data,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify NaN values are converted to empty strings
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        values = worksheet.get_all_values()

        # Bob's row (index 2) should have empty strings for None values
        bob_row = values[2]
        score_index = values[0].index("score")
        notes_index = values[0].index("notes")

        # NaN/None should be empty string
        assert bob_row[score_index] == ""
        assert bob_row[notes_index] == ""


class TestSheetsExporterErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.integration
    def test_create_dashboard_with_empty_dataframe(self, exporter, created_spreadsheets):
        """Test creating dashboard with empty DataFrame."""
        empty_df = pd.DataFrame(columns=["col1", "col2", "col3"])

        url = exporter.create_dashboard(
            title=f"Integration Test - Empty DataFrame - {datetime.now().isoformat()}",
            data=empty_df,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify only headers are present
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("Data")
        values = worksheet.get_all_values()

        # Should have only headers
        assert len(values) == 1
        assert values[0] == ["col1", "col2", "col3"]

    @pytest.mark.integration
    def test_worksheet_renamed_to_data(self, exporter, sample_dataframe, created_spreadsheets):
        """Verify that the first worksheet is renamed to 'Data'."""
        url = exporter.create_dashboard(
            title=f"Integration Test - Worksheet Rename - {datetime.now().isoformat()}",
            data=sample_dataframe,
        )

        # Extract spreadsheet ID
        spreadsheet_id = url.split("/d/")[1].split("/")[0]
        created_spreadsheets.append(spreadsheet_id)

        # Verify worksheet is named "Data"
        spreadsheet = exporter.client.open_by_key(spreadsheet_id)
        worksheets = spreadsheet.worksheets()
        worksheet_titles = [ws.title for ws in worksheets]

        assert "Data" in worksheet_titles
        # Default "Sheet1" should not exist
        assert "Sheet1" not in worksheet_titles
