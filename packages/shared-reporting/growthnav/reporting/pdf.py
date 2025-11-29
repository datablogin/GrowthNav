"""
PDFGenerator - HTML/CSS to PDF conversion using WeasyPrint.

Based on existing PaidSocialNav implementation with improvements:
- Template registry for reusable report formats
- Consistent styling via CSS
- Chart embedding support
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


class PDFGenerator:
    """
    Generate PDF reports from Jinja2 templates.

    Example:
        pdf = PDFGenerator()
        pdf_bytes = pdf.generate(
            data={"customer": "Topgolf", "metrics": [...]},
            template="customer_report"
        )
    """

    def __init__(
        self,
        templates_dir: Path | str | None = None,
    ):
        """
        Initialize PDF generator.

        Args:
            templates_dir: Custom templates directory (default: package templates)
        """
        if templates_dir is None:
            templates_dir = Path(__file__).parent / "templates"

        self.templates_dir = Path(templates_dir)
        self._env: Environment | None = None

    @property
    def env(self) -> Environment:
        """Lazy initialization of Jinja2 environment."""
        if self._env is None:
            self._env = Environment(
                loader=FileSystemLoader(str(self.templates_dir)),
                autoescape=select_autoescape(["html", "xml"]),
            )
        return self._env

    def generate(
        self,
        data: dict[str, Any],
        template: str,
        css: str | None = None,
    ) -> bytes:
        """
        Generate PDF from template and data.

        Args:
            data: Template context data
            template: Template name (without extension, e.g., "customer_report")
            css: Optional custom CSS to inject

        Returns:
            PDF file as bytes
        """
        # Import here to avoid requiring weasyprint for non-PDF usage
        try:
            from weasyprint import CSS, HTML
        except ImportError as e:
            raise ImportError(
                "WeasyPrint is required for PDF generation. "
                "Install with: pip install weasyprint"
            ) from e

        # Render HTML template
        html_content = self.render_html(data, template)

        # Inject custom CSS if provided
        if css:
            html_content = html_content.replace(
                "</head>",
                f"<style>{css}</style></head>",
            )

        # Convert to PDF
        html = HTML(string=html_content, base_url=str(self.templates_dir))
        pdf_buffer = io.BytesIO()
        html.write_pdf(pdf_buffer)

        return pdf_buffer.getvalue()

    def render_html(
        self,
        data: dict[str, Any],
        template: str,
    ) -> str:
        """
        Render template to HTML string.

        Args:
            data: Template context data
            template: Template name (without extension)

        Returns:
            Rendered HTML string
        """
        template_file = f"{template}.html.j2"
        tmpl = self.env.get_template(template_file)
        return tmpl.render(**data)

    def is_available(self) -> bool:
        """Check if WeasyPrint is available."""
        try:
            import weasyprint  # noqa: F401
            return True
        except ImportError:
            return False
