"""
SlidesGenerator - Google Slides presentation generation.

P1 Priority Feature:
- Template-based slide creation
- Chart embedding from matplotlib/plotly
- Consistent branding across presentations
- Domain-wide delegation support for service accounts

Build/Buy/Borrow Options Considered:
1. google-api-python-client (Build) - Direct API, full control
2. python-pptx + convert (Borrow) - PowerPoint, then convert
3. slides-api-wrapper libs (Buy/Borrow) - Higher-level abstractions

Chosen: google-api-python-client for maximum control and native Slides support.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

import google.auth
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Default impersonation email for domain-wide delegation
DEFAULT_IMPERSONATE_EMAIL = "access@roimediapartners.com"


class SlideLayout(str, Enum):
    """Common slide layouts."""

    TITLE = "TITLE"
    TITLE_AND_BODY = "TITLE_AND_BODY"
    TITLE_AND_TWO_COLUMNS = "TITLE_AND_TWO_COLUMNS"
    TITLE_ONLY = "TITLE_ONLY"
    BLANK = "BLANK"
    SECTION_HEADER = "SECTION_HEADER"


@dataclass
class SlideContent:
    """Content for a single slide."""

    title: str
    body: str | list[str] | None = None
    layout: SlideLayout = SlideLayout.TITLE_AND_BODY
    image_url: str | None = None
    chart_data: dict[str, Any] | None = None
    notes: str | None = None


class SlidesGenerator:
    """
    Generate Google Slides presentations.

    Example:
        slides = SlidesGenerator(credentials_path="service_account.json")

        presentation_url = slides.create_presentation(
            title="Monthly Performance Report",
            slides=[
                SlideContent(
                    title="Executive Summary",
                    body=["Key insight 1", "Key insight 2"],
                ),
                SlideContent(
                    title="Performance Metrics",
                    chart_data={"type": "bar", "data": {...}},
                ),
            ]
        )

    For domain-wide delegation:
        slides = SlidesGenerator(
            credentials_path="service_account.json",
            impersonate_email="user@yourdomain.com"
        )
    """

    def __init__(
        self,
        credentials_path: str | None = None,
        impersonate_email: str | None = None,
    ):
        """
        Initialize Slides generator.

        Args:
            credentials_path: Path to service account JSON or authorized user credentials
            impersonate_email: Email to impersonate for domain-wide delegation
                              (default: GROWTHNAV_IMPERSONATE_EMAIL env var or
                               access@roimediapartners.com)
        """
        self.credentials_path = credentials_path or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
        self.impersonate_email = impersonate_email or os.getenv(
            "GROWTHNAV_IMPERSONATE_EMAIL", DEFAULT_IMPERSONATE_EMAIL
        )
        self._service = None
        self._drive_service = None

    def _load_credentials(self, scopes: list[str]):
        """Load credentials from file or use application default credentials."""
        if self.credentials_path and os.path.exists(self.credentials_path):
            # Read the JSON file to determine credential type
            try:
                with open(self.credentials_path) as f:
                    cred_info = json.load(f)
            except (OSError, json.JSONDecodeError):
                # If we can't read the file, fall back to default credentials
                creds, _ = google.auth.default(scopes=scopes)
                return creds

            cred_type = cred_info.get("type")

            if cred_type == "service_account":
                # Service account credentials with domain-wide delegation
                return Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=scopes,
                    subject=self.impersonate_email,  # Impersonate user
                )
            elif cred_type == "authorized_user":
                # Authorized user credentials (from gcloud auth application-default login)
                # For authorized_user, we should use google.auth.default which will handle scopes properly
                creds, _ = google.auth.default(scopes=scopes)
                return creds
            else:
                # Unknown or missing type - fall back to default credentials
                # This handles mock test cases where the file might be empty or invalid
                creds, _ = google.auth.default(scopes=scopes)
                return creds
        else:
            # Use application default credentials
            creds, _ = google.auth.default(scopes=scopes)
            return creds

    @property
    def service(self):
        """Lazy initialization of Slides API service."""
        if self._service is None:
            scopes = [
                "https://www.googleapis.com/auth/presentations",
                "https://www.googleapis.com/auth/drive.file",
            ]
            creds = self._load_credentials(scopes)
            self._service = build("slides", "v1", credentials=creds)
        return self._service

    @property
    def drive_service(self):
        """Lazy initialization of Drive API service for sharing."""
        if self._drive_service is None:
            scopes = ["https://www.googleapis.com/auth/drive.file"]
            creds = self._load_credentials(scopes)
            self._drive_service = build("drive", "v3", credentials=creds)
        return self._drive_service

    def create_presentation(
        self,
        title: str,
        slides: list[SlideContent],
        share_with: list[str] | None = None,
    ) -> str:
        """
        Create a new Google Slides presentation.

        Args:
            title: Presentation title
            slides: List of SlideContent objects
            share_with: Email addresses to share with

        Returns:
            URL of created presentation
        """
        # Create blank presentation
        presentation = self.service.presentations().create(
            body={"title": title}
        ).execute()

        presentation_id = presentation["presentationId"]

        # Create slides one at a time to get placeholder IDs
        for i, slide_content in enumerate(slides):
            slide_id = f"slide_{i}"

            # Create the slide first
            create_request = {
                "requests": [self._create_slide_request(slide_id, slide_content.layout)]
            }
            self.service.presentations().batchUpdate(
                presentationId=presentation_id,
                body=create_request,
            ).execute()

            # Get the slide to find placeholder IDs
            presentation_state = self.service.presentations().get(
                presentationId=presentation_id
            ).execute()

            # Find our newly created slide
            slide_data = None
            for slide in presentation_state.get("slides", []):
                if slide.get("objectId") == slide_id:
                    slide_data = slide
                    break

            if not slide_data:
                continue

            # Find placeholder object IDs
            title_placeholder_id = None
            body_placeholder_id = None
            notes_id = slide_data.get("slideProperties", {}).get(
                "notesPage", {}
            ).get("notesProperties", {}).get("speakerNotesObjectId")

            for element in slide_data.get("pageElements", []):
                shape = element.get("shape", {})
                placeholder = shape.get("placeholder", {})
                placeholder_type = placeholder.get("type")

                if placeholder_type in ("TITLE", "CENTERED_TITLE"):
                    title_placeholder_id = element.get("objectId")
                elif placeholder_type in ("BODY", "SUBTITLE"):
                    body_placeholder_id = element.get("objectId")

            # Build text insertion requests
            text_requests = []

            if title_placeholder_id and slide_content.title:
                text_requests.append({
                    "insertText": {
                        "objectId": title_placeholder_id,
                        "text": slide_content.title,
                    }
                })

            if body_placeholder_id and slide_content.body:
                body_text = (
                    "\n".join(f"• {item}" for item in slide_content.body)
                    if isinstance(slide_content.body, list)
                    else slide_content.body
                )
                text_requests.append({
                    "insertText": {
                        "objectId": body_placeholder_id,
                        "text": body_text,
                    }
                })

            if notes_id and slide_content.notes:
                text_requests.append({
                    "insertText": {
                        "objectId": notes_id,
                        "text": slide_content.notes,
                    }
                })

            # Execute text insertions
            if text_requests:
                self.service.presentations().batchUpdate(
                    presentationId=presentation_id,
                    body={"requests": text_requests},
                ).execute()

        # Share with users
        if share_with:
            for email in share_with:
                self._share_presentation(presentation_id, email)

        return f"https://docs.google.com/presentation/d/{presentation_id}"

    def _create_slide_request(
        self,
        slide_id: str,
        layout: SlideLayout,
    ) -> dict[str, Any]:
        """Create request to add a new slide."""
        return {
            "createSlide": {
                "objectId": slide_id,
                "slideLayoutReference": {
                    "predefinedLayout": layout.value,
                },
            }
        }

    def _share_presentation(
        self,
        presentation_id: str,
        email: str,
        role: str = "reader",
    ) -> None:
        """Share presentation with a user."""
        self.drive_service.permissions().create(
            fileId=presentation_id,
            body={
                "type": "user",
                "role": role,
                "emailAddress": email,
            },
            sendNotificationEmail=False,
        ).execute()

    def create_from_template(
        self,
        template_id: str,
        title: str,
        replacements: dict[str, str],
        share_with: list[str] | None = None,
    ) -> str:
        """
        Create presentation from an existing template.

        Args:
            template_id: ID of template presentation
            title: New presentation title
            replacements: Dict of {placeholder: value} to replace
            share_with: Email addresses to share with

        Returns:
            URL of created presentation
        """
        # Copy template
        copy_response = self.drive_service.files().copy(
            fileId=template_id,
            body={"name": title},
        ).execute()

        presentation_id = copy_response["id"]

        # Replace placeholders
        requests = []
        for placeholder, value in replacements.items():
            requests.append({
                "replaceAllText": {
                    "containsText": {
                        "text": f"{{{{{placeholder}}}}}",  # {{placeholder}}
                        "matchCase": True,
                    },
                    "replaceText": value,
                }
            })

        if requests:
            self.service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={"requests": requests},
            ).execute()

        # Share
        if share_with:
            for email in share_with:
                self._share_presentation(presentation_id, email)

        return f"https://docs.google.com/presentation/d/{presentation_id}"
