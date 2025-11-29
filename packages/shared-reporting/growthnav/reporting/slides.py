"""
SlidesGenerator - Google Slides presentation generation.

P1 Priority Feature:
- Template-based slide creation
- Chart embedding from matplotlib/plotly
- Consistent branding across presentations

Build/Buy/Borrow Options Considered:
1. google-api-python-client (Build) - Direct API, full control
2. python-pptx + convert (Borrow) - PowerPoint, then convert
3. slides-api-wrapper libs (Buy/Borrow) - Higher-level abstractions

Chosen: google-api-python-client for maximum control and native Slides support.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


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
    """

    def __init__(
        self,
        credentials_path: str | None = None,
    ):
        """
        Initialize Slides generator.

        Args:
            credentials_path: Path to service account JSON
        """
        self.credentials_path = credentials_path or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
        self._service = None
        self._drive_service = None

    @property
    def service(self):
        """Lazy initialization of Slides API service."""
        if self._service is None:
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=[
                    "https://www.googleapis.com/auth/presentations",
                    "https://www.googleapis.com/auth/drive.file",
                ],
            )
            self._service = build("slides", "v1", credentials=creds)
        return self._service

    @property
    def drive_service(self):
        """Lazy initialization of Drive API service for sharing."""
        if self._drive_service is None:
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/drive.file"],
            )
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

        # Build batch update requests
        requests = []

        for i, slide_content in enumerate(slides):
            # Create slide
            slide_id = f"slide_{i}"
            requests.append(self._create_slide_request(slide_id, slide_content.layout))

            # Add title
            requests.append(
                self._insert_text_request(
                    slide_id,
                    slide_content.title,
                    placeholder_type="TITLE",
                )
            )

            # Add body content
            if slide_content.body:
                body_text = (
                    "\n".join(f"• {item}" for item in slide_content.body)
                    if isinstance(slide_content.body, list)
                    else slide_content.body
                )
                requests.append(
                    self._insert_text_request(
                        slide_id,
                        body_text,
                        placeholder_type="BODY",
                    )
                )

            # Add speaker notes
            if slide_content.notes:
                requests.append(
                    self._add_speaker_notes_request(slide_id, slide_content.notes)
                )

        # Execute batch update
        if requests:
            self.service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={"requests": requests},
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

    def _insert_text_request(
        self,
        slide_id: str,
        text: str,
        placeholder_type: str,
    ) -> dict[str, Any]:
        """Create request to insert text into a placeholder."""
        # Note: This is simplified - real implementation needs to find
        # placeholder IDs from the slide layout
        return {
            "insertText": {
                "objectId": f"{slide_id}_{placeholder_type.lower()}",
                "text": text,
            }
        }

    def _add_speaker_notes_request(
        self,
        slide_id: str,
        notes: str,
    ) -> dict[str, Any]:
        """Create request to add speaker notes."""
        return {
            "insertText": {
                "objectId": f"{slide_id}_notes",
                "text": notes,
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
