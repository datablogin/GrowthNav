"""Tests for SlidesGenerator."""

from unittest.mock import MagicMock, patch

from growthnav.reporting.slides import SlideContent, SlideLayout, SlidesGenerator


class TestSlideLayout:
    """Test SlideLayout enum."""

    def test_enum_values(self):
        """Test that all expected layouts are defined."""
        assert SlideLayout.TITLE.value == "TITLE"
        assert SlideLayout.TITLE_AND_BODY.value == "TITLE_AND_BODY"
        assert SlideLayout.TITLE_AND_TWO_COLUMNS.value == "TITLE_AND_TWO_COLUMNS"
        assert SlideLayout.TITLE_ONLY.value == "TITLE_ONLY"
        assert SlideLayout.BLANK.value == "BLANK"
        assert SlideLayout.SECTION_HEADER.value == "SECTION_HEADER"

    def test_enum_is_string(self):
        """Test that SlideLayout inherits from str."""
        assert isinstance(SlideLayout.TITLE, str)
        assert SlideLayout.TITLE == "TITLE"


class TestSlideContent:
    """Test SlideContent dataclass."""

    def test_minimal_content(self):
        """Test creating SlideContent with minimal fields."""
        content = SlideContent(title="Test Slide")

        assert content.title == "Test Slide"
        assert content.body is None
        assert content.layout == SlideLayout.TITLE_AND_BODY
        assert content.image_url is None
        assert content.chart_data is None
        assert content.notes is None

    def test_full_content(self):
        """Test creating SlideContent with all fields."""
        content = SlideContent(
            title="Executive Summary",
            body=["Point 1", "Point 2", "Point 3"],
            layout=SlideLayout.TITLE_AND_TWO_COLUMNS,
            image_url="https://example.com/chart.png",
            chart_data={"type": "bar", "data": [1, 2, 3]},
            notes="Speaker notes here",
        )

        assert content.title == "Executive Summary"
        assert content.body == ["Point 1", "Point 2", "Point 3"]
        assert content.layout == SlideLayout.TITLE_AND_TWO_COLUMNS
        assert content.image_url == "https://example.com/chart.png"
        assert content.chart_data == {"type": "bar", "data": [1, 2, 3]}
        assert content.notes == "Speaker notes here"

    def test_body_as_string(self):
        """Test SlideContent with body as string."""
        content = SlideContent(
            title="Test",
            body="This is a single paragraph of text.",
        )

        assert content.body == "This is a single paragraph of text."

    def test_body_as_list(self):
        """Test SlideContent with body as list."""
        content = SlideContent(
            title="Test",
            body=["Item 1", "Item 2"],
        )

        assert isinstance(content.body, list)
        assert len(content.body) == 2


class TestSlidesGenerator:
    """Test SlidesGenerator class."""

    def test_init_with_credentials_path(self):
        """Test initialization with explicit credentials path."""
        generator = SlidesGenerator(credentials_path="/path/to/creds.json")

        assert generator.credentials_path == "/path/to/creds.json"
        assert generator._service is None
        assert generator._drive_service is None

    def test_init_with_env_var(self, monkeypatch):
        """Test initialization with GOOGLE_APPLICATION_CREDENTIALS env var."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/env/creds.json")

        generator = SlidesGenerator()
        assert generator.credentials_path == "/env/creds.json"

    def test_init_without_credentials(self, monkeypatch):
        """Test initialization without credentials."""
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

        generator = SlidesGenerator()
        assert generator.credentials_path is None

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_service_lazy_initialization(self, mock_creds_class, mock_build, tmp_path):
        """Test that Slides API service is lazily initialized."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials and service
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        generator = SlidesGenerator(credentials_path=str(creds_file))
        assert generator._service is None

        # Access service
        service = generator.service
        assert service is mock_service
        assert generator._service is mock_service

        # Verify credentials were loaded with correct scopes
        mock_creds_class.from_service_account_file.assert_called_once_with(
            str(creds_file),
            scopes=[
                "https://www.googleapis.com/auth/presentations",
                "https://www.googleapis.com/auth/drive.file",
            ],
        )

        # Verify service was built
        mock_build.assert_called_once_with("slides", "v1", credentials=mock_creds)

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_drive_service_lazy_initialization(self, mock_creds_class, mock_build, tmp_path):
        """Test that Drive API service is lazily initialized."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials and service
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds
        mock_drive_service = MagicMock()
        mock_build.return_value = mock_drive_service

        generator = SlidesGenerator(credentials_path=str(creds_file))
        assert generator._drive_service is None

        # Access drive service
        drive_service = generator.drive_service
        assert drive_service is mock_drive_service
        assert generator._drive_service is mock_drive_service

        # Verify credentials were loaded
        mock_creds_class.from_service_account_file.assert_called_once_with(
            str(creds_file),
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )

        # Verify Drive service was built
        mock_build.assert_called_once_with("drive", "v3", credentials=mock_creds)

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_basic(self, mock_creds_class, mock_build, tmp_path):
        """Test creating basic presentation."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        # Mock Slides API responses
        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_123"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_service = MagicMock()
        mock_service.presentations.return_value = mock_presentations

        # Mock build to return different services
        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create presentation
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [
            SlideContent(title="Slide 1", body="Content 1"),
            SlideContent(title="Slide 2", body=["Point A", "Point B"]),
        ]

        url = generator.create_presentation(
            title="Test Presentation",
            slides=slides,
        )

        # Verify presentation was created
        mock_presentations.create.assert_called_once_with(
            body={"title": "Test Presentation"}
        )

        # Verify batch update was called
        assert mock_presentations.batchUpdate.called

        # Verify URL
        assert url == "https://docs.google.com/presentation/d/pres_123"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_with_sharing(self, mock_creds_class, mock_build, tmp_path):
        """Test creating presentation with user sharing."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        # Mock Slides API
        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_shared"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_slides_service = MagicMock()
        mock_slides_service.presentations.return_value = mock_presentations

        # Mock Drive API
        mock_permissions = MagicMock()
        mock_permissions.create.return_value.execute.return_value = {}

        mock_drive_service = MagicMock()
        mock_drive_service.permissions.return_value = mock_permissions

        # Mock build to return correct services
        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_slides_service
            elif service_name == "drive":
                return mock_drive_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create with sharing
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [SlideContent(title="Shared Slide")]

        generator.create_presentation(
            title="Shared Presentation",
            slides=slides,
            share_with=["user1@example.com", "user2@example.com"],
        )

        # Verify sharing was called
        assert mock_permissions.create.call_count == 2
        calls = mock_permissions.create.call_args_list

        # First user
        assert calls[0][1]["fileId"] == "pres_shared"
        assert calls[0][1]["body"]["emailAddress"] == "user1@example.com"
        assert calls[0][1]["body"]["type"] == "user"
        assert calls[0][1]["body"]["role"] == "reader"
        assert calls[0][1]["sendNotificationEmail"] is False

        # Second user
        assert calls[1][1]["fileId"] == "pres_shared"
        assert calls[1][1]["body"]["emailAddress"] == "user2@example.com"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_batch_requests(self, mock_creds_class, mock_build, tmp_path):
        """Test that batch update requests are constructed correctly."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_batch"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_service = MagicMock()
        mock_service.presentations.return_value = mock_presentations

        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create presentation
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [
            SlideContent(
                title="Test Slide",
                body=["Bullet 1", "Bullet 2"],
                notes="Speaker notes",
            ),
        ]

        generator.create_presentation(
            title="Batch Test",
            slides=slides,
        )

        # Verify batch update was called with requests
        assert mock_presentations.batchUpdate.called
        call_args = mock_presentations.batchUpdate.call_args
        assert call_args[1]["presentationId"] == "pres_batch"
        requests = call_args[1]["body"]["requests"]

        # Should have requests for: create slide, title, body, notes
        assert len(requests) >= 1

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_with_body_list(self, mock_creds_class, mock_build, tmp_path):
        """Test creating presentation with body as list (bullet points)."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_list"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_service = MagicMock()
        mock_service.presentations.return_value = mock_presentations

        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create with list body
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [
            SlideContent(
                title="Bullet Points",
                body=["First point", "Second point", "Third point"],
            ),
        ]

        generator.create_presentation(
            title="List Test",
            slides=slides,
        )

        # Verify batch update was called
        assert mock_presentations.batchUpdate.called

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_with_body_string(self, mock_creds_class, mock_build, tmp_path):
        """Test creating presentation with body as string."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_string"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_service = MagicMock()
        mock_service.presentations.return_value = mock_presentations

        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create with string body
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [
            SlideContent(
                title="Paragraph",
                body="This is a single paragraph of text content.",
            ),
        ]

        generator.create_presentation(
            title="String Test",
            slides=slides,
        )

        # Verify batch update was called
        assert mock_presentations.batchUpdate.called

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_presentation_multiple_layouts(self, mock_creds_class, mock_build, tmp_path):
        """Test creating presentation with different slide layouts."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_presentations = MagicMock()
        mock_presentations.create.return_value.execute.return_value = {
            "presentationId": "pres_layouts"
        }
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_service = MagicMock()
        mock_service.presentations.return_value = mock_presentations

        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create with different layouts
        generator = SlidesGenerator(credentials_path=str(creds_file))
        slides = [
            SlideContent(title="Title Slide", layout=SlideLayout.TITLE),
            SlideContent(title="Content", body="Text", layout=SlideLayout.TITLE_AND_BODY),
            SlideContent(title="Section", layout=SlideLayout.SECTION_HEADER),
            SlideContent(title="Blank", layout=SlideLayout.BLANK),
        ]

        url = generator.create_presentation(
            title="Layouts Test",
            slides=slides,
        )

        # Verify presentation was created
        assert url == "https://docs.google.com/presentation/d/pres_layouts"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_from_template(self, mock_creds_class, mock_build, tmp_path):
        """Test creating presentation from template."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        # Mock Slides API
        mock_presentations = MagicMock()
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_slides_service = MagicMock()
        mock_slides_service.presentations.return_value = mock_presentations

        # Mock Drive API
        mock_files = MagicMock()
        mock_files.copy.return_value.execute.return_value = {
            "id": "copied_pres_123"
        }

        mock_drive_service = MagicMock()
        mock_drive_service.files.return_value = mock_files

        # Mock build
        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_slides_service
            elif service_name == "drive":
                return mock_drive_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create from template
        generator = SlidesGenerator(credentials_path=str(creds_file))
        url = generator.create_from_template(
            template_id="template_abc123",
            title="From Template",
            replacements={"customer_name": "Topgolf", "date": "2024-01-15"},
        )

        # Verify template was copied
        mock_files.copy.assert_called_once_with(
            fileId="template_abc123",
            body={"name": "From Template"},
        )

        # Verify replacements were made
        assert mock_presentations.batchUpdate.called
        call_args = mock_presentations.batchUpdate.call_args
        assert call_args[1]["presentationId"] == "copied_pres_123"
        requests = call_args[1]["body"]["requests"]

        # Should have replaceAllText requests
        assert len(requests) == 2  # Two replacements

        # Verify URL
        assert url == "https://docs.google.com/presentation/d/copied_pres_123"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_from_template_with_sharing(self, mock_creds_class, mock_build, tmp_path):
        """Test creating from template with sharing."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock credentials
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        # Mock Slides API
        mock_presentations = MagicMock()
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_slides_service = MagicMock()
        mock_slides_service.presentations.return_value = mock_presentations

        # Mock Drive API
        mock_files = MagicMock()
        mock_files.copy.return_value.execute.return_value = {"id": "copied_shared_123"}

        mock_permissions = MagicMock()
        mock_permissions.create.return_value.execute.return_value = {}

        mock_drive_service = MagicMock()
        mock_drive_service.files.return_value = mock_files
        mock_drive_service.permissions.return_value = mock_permissions

        # Mock build
        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_slides_service
            elif service_name == "drive":
                return mock_drive_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create from template with sharing
        generator = SlidesGenerator(credentials_path=str(creds_file))
        generator.create_from_template(
            template_id="template_xyz",
            title="Shared Template",
            replacements={"name": "Test"},
            share_with=["user@example.com"],
        )

        # Verify sharing was called
        mock_permissions.create.assert_called_once()
        call_args = mock_permissions.create.call_args
        assert call_args[1]["fileId"] == "copied_shared_123"
        assert call_args[1]["body"]["emailAddress"] == "user@example.com"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_create_from_template_placeholder_format(self, mock_creds_class, mock_build, tmp_path):
        """Test that placeholders use double curly braces format."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_presentations = MagicMock()
        mock_presentations.batchUpdate.return_value.execute.return_value = {}

        mock_slides_service = MagicMock()
        mock_slides_service.presentations.return_value = mock_presentations

        mock_files = MagicMock()
        mock_files.copy.return_value.execute.return_value = {"id": "copied_pres"}

        mock_drive_service = MagicMock()
        mock_drive_service.files.return_value = mock_files

        def build_side_effect(service_name, version, credentials):
            if service_name == "slides":
                return mock_slides_service
            elif service_name == "drive":
                return mock_drive_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Create from template
        generator = SlidesGenerator(credentials_path=str(creds_file))
        generator.create_from_template(
            template_id="template_id",
            title="Test",
            replacements={"company": "Acme Corp"},
        )

        # Check replacement format
        call_args = mock_presentations.batchUpdate.call_args
        requests = call_args[1]["body"]["requests"]

        # Should look for {{company}}
        replace_request = requests[0]
        assert "replaceAllText" in replace_request
        assert "{{company}}" in replace_request["replaceAllText"]["containsText"]["text"]
        assert replace_request["replaceAllText"]["replaceText"] == "Acme Corp"
        assert replace_request["replaceAllText"]["containsText"]["matchCase"] is True

    def test_create_slide_request(self):
        """Test _create_slide_request helper method."""
        generator = SlidesGenerator(credentials_path="/fake/path")

        request = generator._create_slide_request("slide_0", SlideLayout.TITLE_AND_BODY)

        assert "createSlide" in request
        assert request["createSlide"]["objectId"] == "slide_0"
        assert request["createSlide"]["slideLayoutReference"]["predefinedLayout"] == "TITLE_AND_BODY"

    def test_insert_text_request(self):
        """Test _insert_text_request helper method."""
        generator = SlidesGenerator(credentials_path="/fake/path")

        request = generator._insert_text_request("slide_1", "Test Title", "TITLE")

        assert "insertText" in request
        assert request["insertText"]["objectId"] == "slide_1_title"
        assert request["insertText"]["text"] == "Test Title"

    def test_add_speaker_notes_request(self):
        """Test _add_speaker_notes_request helper method."""
        generator = SlidesGenerator(credentials_path="/fake/path")

        request = generator._add_speaker_notes_request("slide_2", "These are notes")

        assert "insertText" in request
        assert request["insertText"]["objectId"] == "slide_2_notes"
        assert request["insertText"]["text"] == "These are notes"

    @patch("growthnav.reporting.slides.build")
    @patch("growthnav.reporting.slides.Credentials")
    def test_share_presentation(self, mock_creds_class, mock_build, tmp_path):
        """Test _share_presentation helper method."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Mock setup
        mock_creds = MagicMock()
        mock_creds_class.from_service_account_file.return_value = mock_creds

        mock_permissions = MagicMock()
        mock_permissions.create.return_value.execute.return_value = {}

        mock_drive_service = MagicMock()
        mock_drive_service.permissions.return_value = mock_permissions

        def build_side_effect(service_name, version, credentials):
            if service_name == "drive":
                return mock_drive_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        # Test sharing
        generator = SlidesGenerator(credentials_path=str(creds_file))
        generator._share_presentation("pres_123", "user@example.com", role="writer")

        # Verify permission was created
        mock_permissions.create.assert_called_once_with(
            fileId="pres_123",
            body={
                "type": "user",
                "role": "writer",
                "emailAddress": "user@example.com",
            },
            sendNotificationEmail=False,
        )
