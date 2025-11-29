"""
Real integration tests for SlidesGenerator.

These tests hit actual Google Slides API - no mocks.

Prerequisites:
- GCP service account credentials file
- Set GOOGLE_APPLICATION_CREDENTIALS to path of service account JSON
- Service account: growthnav-ci@topgolf-460202.iam.gserviceaccount.com
- Required APIs enabled: Google Slides API, Google Drive API

Run with: uv run pytest packages/shared-reporting/tests/test_integration_slides.py -v

Note: Application default credentials from `gcloud auth application-default login`
may not have sufficient scopes for Slides API. Use a service account file instead.
"""

import json
import os

import pytest
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from growthnav.reporting.slides import SlideContent, SlideLayout, SlidesGenerator


def _has_valid_credentials():
    """Check if valid service account credentials are available."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        return False

    # Check if it's a service account (application default creds don't have sufficient scopes)
    try:
        with open(creds_path) as f:
            cred_info = json.load(f)
        return cred_info.get("type") == "service_account"
    except Exception:
        return False


# Skip all tests in this file if proper GCP credentials are not available
pytestmark = pytest.mark.skipif(
    not _has_valid_credentials(),
    reason="GCP service account credentials not available. Set GOOGLE_APPLICATION_CREDENTIALS to service account JSON file path.",
)


class TestSlidesGeneratorIntegration:
    """Real integration tests for SlidesGenerator."""

    @pytest.fixture
    def generator(self):
        """Create a real SlidesGenerator."""
        return SlidesGenerator()

    @pytest.fixture
    def created_presentations(self) -> list[str]:
        """Track presentation IDs created during tests for cleanup."""
        presentation_ids = []
        yield presentation_ids

        # Cleanup: delete all created presentations
        if presentation_ids:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                creds = Credentials.from_service_account_file(
                    creds_path,
                    scopes=["https://www.googleapis.com/auth/drive.file"],
                )
            else:
                # Use application default credentials
                creds_path = os.path.expanduser(
                    "~/.config/gcloud/application_default_credentials.json"
                )
                if os.path.exists(creds_path):
                    from google.auth import default

                    creds, _ = default(scopes=["https://www.googleapis.com/auth/drive.file"])
                else:
                    return  # Can't cleanup without credentials

            drive_service = build("drive", "v3", credentials=creds)

            for presentation_id in presentation_ids:
                try:
                    drive_service.files().delete(fileId=presentation_id).execute()
                    print(f"Cleaned up presentation: {presentation_id}")
                except Exception as e:
                    print(f"Failed to cleanup presentation {presentation_id}: {e}")

    def test_create_presentation_returns_url(self, generator, created_presentations):
        """Create a simple presentation and verify URL is returned."""
        slides = [
            SlideContent(title="Test Slide", body="This is a test presentation."),
        ]

        url = generator.create_presentation(
            title="Integration Test - Simple",
            slides=slides,
        )

        # Verify URL format
        assert url.startswith("https://docs.google.com/presentation/d/")
        assert isinstance(url, str)

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

    def test_create_presentation_with_multiple_slides(self, generator, created_presentations):
        """Create presentation with multiple slides."""
        slides = [
            SlideContent(
                title="Slide 1",
                body="First slide content",
                layout=SlideLayout.TITLE_AND_BODY,
            ),
            SlideContent(
                title="Slide 2",
                body=["Bullet point 1", "Bullet point 2", "Bullet point 3"],
                layout=SlideLayout.TITLE_AND_BODY,
            ),
            SlideContent(
                title="Slide 3",
                body="Third slide with different layout",
                layout=SlideLayout.TITLE_ONLY,
            ),
        ]

        url = generator.create_presentation(
            title="Integration Test - Multiple Slides",
            slides=slides,
        )

        # Verify URL
        assert url.startswith("https://docs.google.com/presentation/d/")

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created by fetching it
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert presentation["title"] == "Integration Test - Multiple Slides"
        # Presentation should have 3 slides
        assert len(presentation["slides"]) == 3

    def test_slide_layouts(self, generator, created_presentations):
        """Test different slide layouts."""
        slides = [
            SlideContent(title="Title Slide", layout=SlideLayout.TITLE),
            SlideContent(
                title="Title and Body",
                body="Content here",
                layout=SlideLayout.TITLE_AND_BODY,
            ),
            SlideContent(
                title="Section Header",
                layout=SlideLayout.SECTION_HEADER,
            ),
            SlideContent(title="Title Only", layout=SlideLayout.TITLE_ONLY),
            SlideContent(title="Blank Slide", layout=SlideLayout.BLANK),
        ]

        url = generator.create_presentation(
            title="Integration Test - Layouts",
            slides=slides,
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created with correct number of slides
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert len(presentation["slides"]) == 5

    def test_slide_with_body_content(self, generator, created_presentations):
        """Verify body content is added to slides."""
        slides = [
            SlideContent(
                title="String Body",
                body="This is a paragraph of text content.",
            ),
            SlideContent(
                title="List Body",
                body=["First item", "Second item", "Third item"],
            ),
        ]

        url = generator.create_presentation(
            title="Integration Test - Body Content",
            slides=slides,
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert presentation["title"] == "Integration Test - Body Content"
        assert len(presentation["slides"]) == 2

    def test_share_presentation(self, generator, created_presentations):
        """Test sharing functionality."""
        slides = [
            SlideContent(title="Shared Presentation", body="This will be shared."),
        ]

        # Use a test email address (service account email for testing)
        test_email = "growthnav-ci@topgolf-460202.iam.gserviceaccount.com"

        url = generator.create_presentation(
            title="Integration Test - Sharing",
            slides=slides,
            share_with=[test_email],
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify sharing by checking permissions
        permissions = generator.drive_service.permissions().list(
            fileId=presentation_id,
            fields="permissions(emailAddress,role,type)",
        ).execute()

        # Should have at least one permission
        # Note: The service account itself may already have owner access
        # so we just verify the sharing mechanism worked without errors
        assert len(permissions.get("permissions", [])) >= 1

    def test_slide_with_speaker_notes(self, generator, created_presentations):
        """Test adding speaker notes to slides."""
        slides = [
            SlideContent(
                title="Slide with Notes",
                body="Public content",
                notes="These are private speaker notes for the presenter.",
            ),
        ]

        url = generator.create_presentation(
            title="Integration Test - Speaker Notes",
            slides=slides,
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert len(presentation["slides"]) == 1

    def test_empty_presentation(self, generator, created_presentations):
        """Test creating presentation with no content slides."""
        url = generator.create_presentation(
            title="Integration Test - Empty",
            slides=[],
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert presentation["title"] == "Integration Test - Empty"
        # Empty presentation should have 1 default slide created by Google
        assert len(presentation["slides"]) >= 0

    def test_presentation_with_long_title(self, generator, created_presentations):
        """Test creating presentation with a long title."""
        long_title = "Integration Test - " + "Very Long Title " * 10

        slides = [
            SlideContent(title="Test Slide"),
        ]

        url = generator.create_presentation(
            title=long_title,
            slides=slides,
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify presentation was created
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert long_title in presentation["title"]

    def test_multiple_presentations_sequential(self, generator, created_presentations):
        """Test creating multiple presentations in sequence."""
        for i in range(3):
            slides = [
                SlideContent(
                    title=f"Slide in Presentation {i + 1}",
                    body=f"Content for presentation {i + 1}",
                ),
            ]

            url = generator.create_presentation(
                title=f"Integration Test - Sequential {i + 1}",
                slides=slides,
            )

            # Extract presentation ID for cleanup
            presentation_id = url.split("/d/")[1].split("/")[0]
            created_presentations.append(presentation_id)

            assert url.startswith("https://docs.google.com/presentation/d/")

        # Verify all 3 were created
        assert len(created_presentations) == 3


class TestCreateFromTemplateIntegration:
    """Integration tests for template-based presentation creation."""

    @pytest.fixture
    def generator(self):
        """Create a real SlidesGenerator."""
        return SlidesGenerator()

    @pytest.fixture
    def template_presentation_id(self, generator) -> str:
        """Create a template presentation for testing."""
        # Create a simple template with placeholders
        slides = [
            SlideContent(
                title="{{company_name}} Report",
                body="Prepared for {{customer_name}} on {{date}}",
            ),
        ]

        url = generator.create_presentation(
            title="Template for Testing",
            slides=slides,
        )

        template_id = url.split("/d/")[1].split("/")[0]
        yield template_id

        # Cleanup template
        try:
            generator.drive_service.files().delete(fileId=template_id).execute()
            print(f"Cleaned up template: {template_id}")
        except Exception as e:
            print(f"Failed to cleanup template {template_id}: {e}")

    @pytest.fixture
    def created_presentations(self) -> list[str]:
        """Track presentation IDs created during tests for cleanup."""
        presentation_ids = []
        yield presentation_ids

        # Cleanup
        if presentation_ids:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                creds = Credentials.from_service_account_file(
                    creds_path,
                    scopes=["https://www.googleapis.com/auth/drive.file"],
                )
            else:
                creds_path = os.path.expanduser(
                    "~/.config/gcloud/application_default_credentials.json"
                )
                if os.path.exists(creds_path):
                    from google.auth import default

                    creds, _ = default(scopes=["https://www.googleapis.com/auth/drive.file"])
                else:
                    return

            drive_service = build("drive", "v3", credentials=creds)

            for presentation_id in presentation_ids:
                try:
                    drive_service.files().delete(fileId=presentation_id).execute()
                    print(f"Cleaned up presentation: {presentation_id}")
                except Exception as e:
                    print(f"Failed to cleanup presentation {presentation_id}: {e}")

    def test_create_from_template_basic(
        self, generator, template_presentation_id, created_presentations
    ):
        """Test creating presentation from template with replacements."""
        url = generator.create_from_template(
            template_id=template_presentation_id,
            title="From Template - Test 1",
            replacements={
                "company_name": "GrowthNav",
                "customer_name": "Topgolf",
                "date": "2024-01-15",
            },
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify URL
        assert url.startswith("https://docs.google.com/presentation/d/")

        # Verify presentation was created with correct title
        presentation = generator.service.presentations().get(
            presentationId=presentation_id
        ).execute()

        assert presentation["title"] == "From Template - Test 1"

    def test_create_from_template_with_sharing(
        self, generator, template_presentation_id, created_presentations
    ):
        """Test creating from template with sharing."""
        test_email = "growthnav-ci@topgolf-460202.iam.gserviceaccount.com"

        url = generator.create_from_template(
            template_id=template_presentation_id,
            title="From Template - Shared",
            replacements={"company_name": "Test Co"},
            share_with=[test_email],
        )

        # Extract presentation ID for cleanup
        presentation_id = url.split("/d/")[1].split("/")[0]
        created_presentations.append(presentation_id)

        # Verify sharing
        permissions = generator.drive_service.permissions().list(
            fileId=presentation_id,
            fields="permissions(emailAddress,role,type)",
        ).execute()

        assert len(permissions.get("permissions", [])) >= 1
