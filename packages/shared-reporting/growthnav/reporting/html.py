"""
HTMLRenderer - Jinja2-based HTML template rendering.

Shared by PDF generator and standalone HTML output.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


class HTMLRenderer:
    """
    Render HTML from Jinja2 templates.

    Example:
        renderer = HTMLRenderer()
        html = renderer.render(
            template="customer_report",
            data={"customer": "Topgolf", "metrics": [...]}
        )
    """

    def __init__(
        self,
        templates_dir: Path | str | None = None,
    ):
        """
        Initialize HTML renderer.

        Args:
            templates_dir: Custom templates directory
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
            # Register custom filters
            self._env.filters["format_currency"] = self._format_currency
            self._env.filters["format_percent"] = self._format_percent
            self._env.filters["format_number"] = self._format_number

        return self._env

    def render(
        self,
        template: str,
        data: dict[str, Any],
    ) -> str:
        """
        Render template to HTML string.

        Args:
            template: Template name (without extension)
            data: Template context data

        Returns:
            Rendered HTML string
        """
        template_file = f"{template}.html.j2"
        tmpl = self.env.get_template(template_file)
        return tmpl.render(**data)

    def render_string(
        self,
        template_string: str,
        data: dict[str, Any],
    ) -> str:
        """
        Render template from string.

        Args:
            template_string: Jinja2 template as string
            data: Template context data

        Returns:
            Rendered HTML string
        """
        tmpl = self.env.from_string(template_string)
        return tmpl.render(**data)

    def list_templates(self) -> list[str]:
        """List available templates."""
        return [
            p.stem.replace(".html", "")
            for p in self.templates_dir.glob("*.html.j2")
        ]

    @staticmethod
    def _format_currency(value: float, symbol: str = "$") -> str:
        """Format number as currency."""
        return f"{symbol}{value:,.2f}"

    @staticmethod
    def _format_percent(value: float, decimals: int = 1) -> str:
        """Format number as percentage."""
        return f"{value * 100:.{decimals}f}%"

    @staticmethod
    def _format_number(value: float, decimals: int = 0) -> str:
        """Format number with thousands separator."""
        return f"{value:,.{decimals}f}"
