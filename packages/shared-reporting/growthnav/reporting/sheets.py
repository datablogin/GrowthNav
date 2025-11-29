"""
SheetsExporter - Google Sheets integration for dashboard creation.

Based on existing PaidSocialNav implementation with improvements:
- Batch operations to avoid rate limits
- Conditional formatting for metrics
- Template-based sheet structure
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd


class SheetsExporter:
    """
    Export data to Google Sheets dashboards.

    Example:
        sheets = SheetsExporter(credentials_path="service_account.json")
        url = sheets.create_dashboard(
            title="Customer Dashboard",
            data=df,
            share_with=["user@example.com"]
        )
    """

    # Google Sheets API rate limits
    REQUESTS_PER_MINUTE = 60
    REQUESTS_PER_100_SECONDS = 100

    def __init__(
        self,
        credentials_path: str | None = None,
    ):
        """
        Initialize Sheets exporter.

        Args:
            credentials_path: Path to service account JSON
                             (default: GOOGLE_APPLICATION_CREDENTIALS env var)
        """
        self.credentials_path = credentials_path or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
        self._client = None

    @property
    def client(self):
        """Lazy initialization of gspread client."""
        if self._client is None:
            import gspread
            from google.oauth2.service_account import Credentials

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ]

            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=scopes,
            )
            self._client = gspread.authorize(creds)

        return self._client

    def create_dashboard(
        self,
        title: str,
        data: pd.DataFrame | list[dict[str, Any]],
        share_with: list[str] | None = None,
        folder_id: str | None = None,
    ) -> str:
        """
        Create a new Google Sheets dashboard.

        Args:
            title: Spreadsheet title
            data: Data to populate (DataFrame or list of dicts)
            share_with: Email addresses to share with
            folder_id: Google Drive folder ID to create in

        Returns:
            URL of created spreadsheet
        """
        # Convert to DataFrame if needed
        if isinstance(data, list):
            data = pd.DataFrame(data)

        # Create spreadsheet
        spreadsheet = self.client.create(title, folder_id=folder_id)

        # Get first worksheet
        worksheet = spreadsheet.sheet1
        worksheet.update_title("Data")

        # Write data using batch update
        self._batch_update_from_dataframe(worksheet, data)

        # Share with specified users
        if share_with:
            for email in share_with:
                spreadsheet.share(email, perm_type="user", role="reader")

        return spreadsheet.url

    def create_multi_tab_dashboard(
        self,
        title: str,
        tabs: dict[str, pd.DataFrame],
        share_with: list[str] | None = None,
    ) -> str:
        """
        Create a multi-tab dashboard.

        Args:
            title: Spreadsheet title
            tabs: Dict of {tab_name: DataFrame}
            share_with: Email addresses to share with

        Returns:
            URL of created spreadsheet
        """
        spreadsheet = self.client.create(title)

        # Create tabs
        for i, (tab_name, data) in enumerate(tabs.items()):
            if i == 0:
                # Rename first sheet
                worksheet = spreadsheet.sheet1
                worksheet.update_title(tab_name)
            else:
                # Add new sheet
                worksheet = spreadsheet.add_worksheet(
                    title=tab_name,
                    rows=len(data) + 1,
                    cols=len(data.columns),
                )

            self._batch_update_from_dataframe(worksheet, data)

        # Share
        if share_with:
            for email in share_with:
                spreadsheet.share(email, perm_type="user", role="reader")

        return spreadsheet.url

    def update_sheet(
        self,
        spreadsheet_id: str,
        data: pd.DataFrame,
        sheet_name: str = "Data",
    ) -> None:
        """
        Update an existing sheet with new data.

        Args:
            spreadsheet_id: ID of existing spreadsheet
            data: New data to write
            sheet_name: Name of worksheet to update
        """
        spreadsheet = self.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Clear and update
        worksheet.clear()
        self._batch_update_from_dataframe(worksheet, data)

    def _batch_update_from_dataframe(
        self,
        worksheet,
        data: pd.DataFrame,
    ) -> None:
        """
        Batch update worksheet from DataFrame.

        Uses single update call to avoid rate limits.
        """
        # Prepare data with headers
        values = [data.columns.tolist()] + data.values.tolist()

        # Convert non-serializable values
        values = [
            [self._serialize_value(v) for v in row]
            for row in values
        ]

        # Single batch update
        worksheet.update(values, "A1")

    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if pd.isna(value):
            return ""
        if hasattr(value, "isoformat"):  # datetime
            return value.isoformat()
        if isinstance(value, (list, dict)):
            import json
            return json.dumps(value)
        return value
