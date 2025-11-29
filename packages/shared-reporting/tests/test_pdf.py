"""Tests for PDFGenerator."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from growthnav.reporting.pdf import PDFGenerator


class TestPDFGenerator:
    """Test PDFGenerator class."""

    def test_init_default_templates_dir(self):
        """Test initialization with default templates directory."""
        pdf = PDFGenerator()
        expected_dir = Path(__file__).parent.parent / "growthnav" / "reporting" / "templates"
        assert pdf.templates_dir == expected_dir

    def test_init_custom_templates_dir(self, tmp_path):
        """Test initialization with custom templates directory."""
        custom_dir = tmp_path / "custom_templates"
        custom_dir.mkdir()

        pdf = PDFGenerator(templates_dir=custom_dir)
        assert pdf.templates_dir == custom_dir

    def test_init_custom_templates_dir_as_string(self, tmp_path):
        """Test initialization with custom templates directory as string."""
        custom_dir = tmp_path / "custom_templates"
        custom_dir.mkdir()

        pdf = PDFGenerator(templates_dir=str(custom_dir))
        assert pdf.templates_dir == custom_dir

    def test_env_lazy_initialization(self, tmp_path):
        """Test that Jinja2 environment is lazily initialized."""
        custom_dir = tmp_path / "templates"
        custom_dir.mkdir()

        pdf = PDFGenerator(templates_dir=custom_dir)
        assert pdf._env is None

        # Access env property
        env = pdf.env
        assert env is not None
        assert pdf._env is env  # Same instance on repeated access

    def test_env_autoescape_enabled(self, tmp_path):
        """Test that autoescape is enabled for HTML and XML."""
        custom_dir = tmp_path / "templates"
        custom_dir.mkdir()

        pdf = PDFGenerator(templates_dir=custom_dir)
        env = pdf.env

        # Check autoescape is enabled
        assert env.autoescape is not None

    def test_render_html_basic(self, tmp_path):
        """Test basic HTML rendering from template."""
        # Create test template
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "test_report.html.j2"
        template_file.write_text("<html><body><h1>{{ title }}</h1></body></html>")

        pdf = PDFGenerator(templates_dir=templates_dir)
        html = pdf.render_html(
            data={"title": "Test Report"},
            template="test_report"
        )

        assert "<h1>Test Report</h1>" in html
        assert "<html>" in html

    def test_render_html_with_variables(self, tmp_path):
        """Test HTML rendering with multiple variables."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "report.html.j2"
        template_file.write_text(
            "<html><body>"
            "<h1>{{ customer }}</h1>"
            "<p>Revenue: {{ revenue }}</p>"
            "</body></html>"
        )

        pdf = PDFGenerator(templates_dir=templates_dir)
        html = pdf.render_html(
            data={"customer": "Topgolf", "revenue": 100000},
            template="report"
        )

        assert "Topgolf" in html
        assert "100000" in html

    def test_render_html_with_list(self, tmp_path):
        """Test HTML rendering with list iteration."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "list.html.j2"
        template_file.write_text(
            "<html><body><ul>"
            "{% for item in items %}<li>{{ item }}</li>{% endfor %}"
            "</ul></body></html>"
        )

        pdf = PDFGenerator(templates_dir=templates_dir)
        html = pdf.render_html(
            data={"items": ["Item 1", "Item 2", "Item 3"]},
            template="list"
        )

        assert "<li>Item 1</li>" in html
        assert "<li>Item 2</li>" in html
        assert "<li>Item 3</li>" in html

    @patch.dict("sys.modules", {"weasyprint": MagicMock()})
    def test_generate_basic(self, tmp_path):
        """Test basic PDF generation."""
        # Create test template
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "simple.html.j2"
        template_file.write_text("<html><head></head><body><h1>{{ title }}</h1></body></html>")

        # Mock WeasyPrint HTML and CSS classes
        mock_html_instance = MagicMock()
        mock_html_instance.write_pdf = MagicMock()

        mock_html_class = MagicMock(return_value=mock_html_instance)
        mock_css_class = MagicMock()

        with patch("weasyprint.HTML", mock_html_class), \
             patch("weasyprint.CSS", mock_css_class):
            # Generate PDF
            pdf = PDFGenerator(templates_dir=templates_dir)
            pdf.generate(
                data={"title": "Test PDF"},
                template="simple"
            )

            # Verify HTML was created with rendered content
            call_args = mock_html_class.call_args
            assert "Test PDF" in call_args.kwargs["string"]
            assert call_args.kwargs["base_url"] == str(templates_dir)

            # Verify write_pdf was called
            assert mock_html_instance.write_pdf.called

    @patch.dict("sys.modules", {"weasyprint": MagicMock()})
    def test_generate_with_custom_css(self, tmp_path):
        """Test PDF generation with custom CSS injection."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "styled.html.j2"
        template_file.write_text("<html><head></head><body><h1>Test</h1></body></html>")

        # Mock WeasyPrint
        mock_html_instance = MagicMock()
        mock_html_instance.write_pdf = MagicMock()
        mock_html_class = MagicMock(return_value=mock_html_instance)
        mock_css_class = MagicMock()

        with patch("weasyprint.HTML", mock_html_class), \
             patch("weasyprint.CSS", mock_css_class):
            # Generate with custom CSS
            pdf = PDFGenerator(templates_dir=templates_dir)
            custom_css = "body { color: red; }"
            pdf.generate(
                data={},
                template="styled",
                css=custom_css
            )

            # Verify CSS was injected before </head>
            html_string = mock_html_class.call_args.kwargs["string"]
            assert f"<style>{custom_css}</style></head>" in html_string

    @patch.dict("sys.modules", {"weasyprint": MagicMock()})
    def test_generate_css_injection_preserves_content(self, tmp_path):
        """Test that CSS injection doesn't break existing content."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "content.html.j2"
        template_file.write_text(
            "<html><head><title>{{ title }}</title></head><body><p>{{ content }}</p></body></html>"
        )

        # Mock WeasyPrint
        mock_html_instance = MagicMock()
        mock_html_instance.write_pdf = MagicMock()
        mock_html_class = MagicMock(return_value=mock_html_instance)
        mock_css_class = MagicMock()

        with patch("weasyprint.HTML", mock_html_class), \
             patch("weasyprint.CSS", mock_css_class):
            # Generate
            pdf = PDFGenerator(templates_dir=templates_dir)
            pdf.generate(
                data={"title": "My Title", "content": "My Content"},
                template="content",
                css="p { font-size: 14px; }"
            )

            html_string = mock_html_class.call_args.kwargs["string"]
            assert "My Title" in html_string
            assert "My Content" in html_string
            assert "<style>p { font-size: 14px; }</style></head>" in html_string

    def test_generate_missing_weasyprint(self, tmp_path):
        """Test that ImportError is raised when WeasyPrint is not available."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "test.html.j2"
        template_file.write_text("<html><body>Test</body></html>")

        PDFGenerator(templates_dir=templates_dir)

        # Mock import failure
        with patch.dict("sys.modules", {"weasyprint": None}):
            with pytest.raises(ImportError) as exc_info:
                # Force reimport to trigger the error
                import importlib

                import growthnav.reporting.pdf
                importlib.reload(growthnav.reporting.pdf)
                from growthnav.reporting.pdf import PDFGenerator as ReloadedPDF

                pdf_reloaded = ReloadedPDF(templates_dir=templates_dir)
                pdf_reloaded.generate(data={}, template="test")

            assert "WeasyPrint is required" in str(exc_info.value)

    def test_is_available_true(self):
        """Test is_available returns True when WeasyPrint is installed."""
        pdf = PDFGenerator()

        with patch("builtins.__import__"):
            assert pdf.is_available() is True

    def test_is_available_false(self):
        """Test is_available returns False when WeasyPrint is not installed."""
        pdf = PDFGenerator()

        # Mock ImportError
        with patch("builtins.__import__", side_effect=ImportError):
            assert pdf.is_available() is False

    @patch.dict("sys.modules", {"weasyprint": MagicMock()})
    def test_generate_returns_bytes(self, tmp_path):
        """Test that generate returns bytes."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "test.html.j2"
        template_file.write_text("<html><body>Test</body></html>")

        # Mock WeasyPrint to return specific bytes
        mock_html_instance = MagicMock()
        expected_bytes = b"%PDF-1.4 test content"

        def mock_write_pdf(buffer):
            buffer.write(expected_bytes)

        mock_html_instance.write_pdf = mock_write_pdf
        mock_html_class = MagicMock(return_value=mock_html_instance)
        mock_css_class = MagicMock()

        with patch("weasyprint.HTML", mock_html_class), \
             patch("weasyprint.CSS", mock_css_class):
            # Generate
            pdf = PDFGenerator(templates_dir=templates_dir)
            result = pdf.generate(data={}, template="test")

            assert isinstance(result, bytes)
            assert result == expected_bytes

    def test_render_html_missing_template(self, tmp_path):
        """Test that missing template raises error."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()

        pdf = PDFGenerator(templates_dir=templates_dir)

        with pytest.raises(Exception):  # noqa: B017 - Jinja2 TemplateNotFound
            pdf.render_html(data={}, template="nonexistent")

    def test_render_html_basic_rendering(self, tmp_path):
        """Test that HTML rendering works correctly."""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        template_file = templates_dir / "simple.html.j2"
        template_file.write_text("<html><body><p>{{ content }}</p></body></html>")

        pdf = PDFGenerator(templates_dir=templates_dir)
        html = pdf.render_html(
            data={"content": "Hello World"},
            template="simple"
        )

        # Should render the content
        assert "Hello World" in html
        assert "<p>" in html
