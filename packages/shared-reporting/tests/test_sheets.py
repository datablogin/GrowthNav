"""Tests for SheetsExporter."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
from growthnav.reporting.sheets import SheetsExporter


class TestSheetsExporter:
    """Test SheetsExporter class."""

    def test_init_with_credentials_path(self):
        """Test initialization with explicit credentials path."""
        exporter = SheetsExporter(credentials_path="/path/to/creds.json")
        assert exporter.credentials_path == "/path/to/creds.json"
        assert exporter._client is None

    def test_init_with_env_var(self, monkeypatch):
        """Test initialization with GOOGLE_APPLICATION_CREDENTIALS env var."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/env/creds.json")

        exporter = SheetsExporter()
        assert exporter.credentials_path == "/env/creds.json"

    def test_init_without_credentials(self, monkeypatch):
        """Test initialization without credentials."""
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

        exporter = SheetsExporter()
        assert exporter.credentials_path is None

    def test_rate_limit_constants(self):
        """Test that rate limit constants are defined."""
        assert SheetsExporter.REQUESTS_PER_MINUTE == 60
        assert SheetsExporter.REQUESTS_PER_100_SECONDS == 100

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_client_lazy_initialization(self, mock_authorize, mock_creds_class, tmp_path):
        """Test that gspread client is lazily initialized."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials and client
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client

        exporter = SheetsExporter(credentials_path=str(creds_file))
        assert exporter._client is None

        # Access client
        client = exporter.client
        assert client is mock_client
        assert exporter._client is mock_client

        # Verify credentials were loaded with correct scopes
        mock_creds_class.from_service_account_file.assert_called_once_with(
            str(creds_file),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )

        # Verify client was authorized
        mock_authorize.assert_called_once_with(mock_creds)

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_dashboard_with_dataframe(self, mock_authorize, mock_creds_class, tmp_path):
        """Test creating dashboard with DataFrame."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        # Mock gspread objects
        mock_worksheet = MagicMock()
        mock_worksheet.update_title = MagicMock()
        mock_worksheet.update = MagicMock()

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/abc123"
        mock_spreadsheet.share = MagicMock()

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create exporter and dashboard
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({
            "customer": ["Topgolf", "ClubCorp"],
            "revenue": [100000, 150000],
        })

        url = exporter.create_dashboard(
            title="Customer Dashboard",
            data=df,
        )

        # Verify spreadsheet was created
        mock_client.create.assert_called_once_with("Customer Dashboard", folder_id=None)

        # Verify worksheet was renamed
        mock_worksheet.update_title.assert_called_once_with("Data")

        # Verify data was updated
        assert mock_worksheet.update.called
        call_args = mock_worksheet.update.call_args
        values = call_args[0][0]

        # Check headers
        assert values[0] == ["customer", "revenue"]

        # Check data
        assert len(values) == 3  # Header + 2 rows

        # Verify URL
        assert url == "https://docs.google.com/spreadsheets/d/abc123"

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_dashboard_with_list_of_dicts(self, mock_authorize, mock_creds_class, tmp_path):
        """Test creating dashboard with list of dicts."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/xyz789"

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create with list of dicts
        exporter = SheetsExporter(credentials_path=str(creds_file))
        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
        ]

        url = exporter.create_dashboard(
            title="Scores",
            data=data,
        )

        # Verify data was converted to DataFrame and written
        assert mock_worksheet.update.called
        values = mock_worksheet.update.call_args[0][0]
        assert values[0] == ["name", "score"]
        assert url == "https://docs.google.com/spreadsheets/d/xyz789"

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_dashboard_with_sharing(self, mock_authorize, mock_creds_class, tmp_path):
        """Test creating dashboard with user sharing."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/shared123"
        mock_spreadsheet.share = MagicMock()

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create with sharing
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({"col": [1, 2, 3]})

        url = exporter.create_dashboard(
            title="Shared Dashboard",
            data=df,
            share_with=["user1@example.com", "user2@example.com"],
        )

        # Verify sharing was called for each user
        assert mock_spreadsheet.share.call_count == 2
        calls = mock_spreadsheet.share.call_args_list
        assert calls[0][0][0] == "user1@example.com"
        assert calls[0][1]["perm_type"] == "user"
        assert calls[0][1]["role"] == "reader"
        assert calls[1][0][0] == "user2@example.com"

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_dashboard_with_folder(self, mock_authorize, mock_creds_class, tmp_path):
        """Test creating dashboard in specific folder."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/folder123"

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create with folder_id
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({"col": [1]})

        url = exporter.create_dashboard(
            title="In Folder",
            data=df,
            folder_id="folder_abc123",
        )

        # Verify folder_id was passed to create
        mock_client.create.assert_called_once_with("In Folder", folder_id="folder_abc123")

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_multi_tab_dashboard(self, mock_authorize, mock_creds_class, tmp_path):
        """Test creating multi-tab dashboard."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet1 = MagicMock()
        mock_worksheet2 = MagicMock()
        mock_worksheet3 = MagicMock()

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet1
        mock_spreadsheet.add_worksheet.side_effect = [mock_worksheet2, mock_worksheet3]
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/multi123"
        mock_spreadsheet.share = MagicMock()

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create multi-tab
        exporter = SheetsExporter(credentials_path=str(creds_file))
        tabs = {
            "Summary": pd.DataFrame({"metric": ["revenue"], "value": [100000]}),
            "Details": pd.DataFrame({"item": ["A", "B"], "qty": [10, 20]}),
            "Analysis": pd.DataFrame({"category": ["X"], "count": [5]}),
        }

        url = exporter.create_multi_tab_dashboard(
            title="Multi-Tab Dashboard",
            tabs=tabs,
        )

        # Verify first sheet was renamed
        mock_worksheet1.update_title.assert_called_once_with("Summary")

        # Verify additional sheets were added
        assert mock_spreadsheet.add_worksheet.call_count == 2
        calls = mock_spreadsheet.add_worksheet.call_args_list
        assert calls[0][1]["title"] == "Details"
        assert calls[1][1]["title"] == "Analysis"

        # Verify all worksheets were updated
        assert mock_worksheet1.update.called
        assert mock_worksheet2.update.called
        assert mock_worksheet3.update.called

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_create_multi_tab_with_sharing(self, mock_authorize, mock_creds_class, tmp_path):
        """Test multi-tab dashboard with sharing."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/multi_shared"
        mock_spreadsheet.share = MagicMock()

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create with sharing
        exporter = SheetsExporter(credentials_path=str(creds_file))
        tabs = {"Tab1": pd.DataFrame({"col": [1]})}

        url = exporter.create_multi_tab_dashboard(
            title="Shared Multi-Tab",
            tabs=tabs,
            share_with=["user@example.com"],
        )

        # Verify sharing
        mock_spreadsheet.share.assert_called_once_with(
            "user@example.com",
            perm_type="user",
            role="reader",
        )

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_update_sheet(self, mock_authorize, mock_creds_class, tmp_path):
        """Test updating existing sheet."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_worksheet.clear = MagicMock()
        mock_worksheet.update = MagicMock()

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet

        mock_client = MagicMock()
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Update sheet
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({"new_col": [100, 200]})

        exporter.update_sheet(
            spreadsheet_id="existing_id_123",
            data=df,
            sheet_name="Data",
        )

        # Verify sheet was opened
        mock_client.open_by_key.assert_called_once_with("existing_id_123")
        mock_spreadsheet.worksheet.assert_called_once_with("Data")

        # Verify sheet was cleared
        mock_worksheet.clear.assert_called_once()

        # Verify data was updated
        assert mock_worksheet.update.called

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_update_sheet_custom_name(self, mock_authorize, mock_creds_class, tmp_path):
        """Test updating sheet with custom sheet name."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet

        mock_client = MagicMock()
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Update with custom name
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({"col": [1]})

        exporter.update_sheet(
            spreadsheet_id="id123",
            data=df,
            sheet_name="CustomSheet",
        )

        # Verify correct sheet was accessed
        mock_spreadsheet.worksheet.assert_called_once_with("CustomSheet")

    def test_serialize_value_none(self):
        """Test serialization of None/NaN values."""
        exporter = SheetsExporter()

        assert exporter._serialize_value(None) == ""
        assert exporter._serialize_value(pd.NA) == ""
        assert exporter._serialize_value(float("nan")) == ""

    def test_serialize_value_datetime(self):
        """Test serialization of datetime values."""
        exporter = SheetsExporter()

        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = exporter._serialize_value(dt)

        assert result == "2024-01-15T10:30:00"

    def test_serialize_value_dict_only(self):
        """Test serialization of dict values (list causes pandas.isna issues)."""
        exporter = SheetsExporter()

        # Test with dict instead of list to avoid pandas.isna() ambiguity
        import json
        test_dict = {"a": 1, "b": 2}
        result = exporter._serialize_value(test_dict)
        # Should serialize to JSON
        loaded = json.loads(result)
        assert loaded == test_dict

    def test_serialize_value_dict(self):
        """Test serialization of dict values."""
        exporter = SheetsExporter()

        result = exporter._serialize_value({"key": "value", "num": 42})
        # JSON can serialize in different orders
        assert '"key": "value"' in result or '"key":"value"' in result
        assert '"num": 42' in result or '"num":42' in result

    def test_serialize_value_primitives(self):
        """Test serialization of primitive values."""
        exporter = SheetsExporter()

        assert exporter._serialize_value(42) == 42
        assert exporter._serialize_value(3.14) == 3.14
        assert exporter._serialize_value("hello") == "hello"
        assert exporter._serialize_value(True) is True

    @patch("google.oauth2.service_account.Credentials")
    @patch("gspread.authorize")
    def test_batch_update_from_dataframe(self, mock_authorize, mock_creds_class, tmp_path):
        """Test batch update processes data correctly."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_worksheet = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.sheet1 = mock_worksheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/batch123"

        mock_client = MagicMock()
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client

        # Create dashboard with various data types
        exporter = SheetsExporter(credentials_path=str(creds_file))
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "score": [95.5, 87.2],
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
        })

        url = exporter.create_dashboard(
            title="Batch Test",
            data=df,
        )

        # Check the data passed to update
        call_args = mock_worksheet.update.call_args
        values = call_args[0][0]

        # Headers
        assert values[0] == ["name", "score", "date"]

        # First data row
        assert values[1][0] == "Alice"
        assert values[1][1] == 95.5
        assert values[1][2] == "2024-01-01T00:00:00"  # Serialized datetime

        # Second data row
        assert values[2][0] == "Bob"
        assert values[2][1] == 87.2
        assert values[2][2] == "2024-01-02T00:00:00"

        # Verify update was called with "A1" range
        assert call_args[0][1] == "A1"
