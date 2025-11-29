"""
GrowthNav Reporting - Shared reporting framework for multiple output formats.

Supports:
- PDF generation via WeasyPrint
- Google Sheets via gspread
- Google Slides via Slides API (P1 priority)
- HTML via Jinja2 templates

Usage:
    from growthnav.reporting import PDFGenerator, SheetsExporter, SlidesGenerator

    # Generate PDF
    pdf = PDFGenerator()
    pdf_bytes = pdf.generate(data, template="customer_report")

    # Export to Google Sheets
    sheets = SheetsExporter(credentials_path="service_account.json")
    url = sheets.create_dashboard("Customer Dashboard", data)

    # Generate Google Slides (P1)
    slides = SlidesGenerator(credentials_path="service_account.json")
    url = slides.create_presentation("Monthly Report", slides_data)
"""

from growthnav.reporting.html import HTMLRenderer
from growthnav.reporting.pdf import PDFGenerator
from growthnav.reporting.sheets import SheetsExporter
from growthnav.reporting.slides import SlidesGenerator

__all__ = [
    "PDFGenerator",
    "SheetsExporter",
    "SlidesGenerator",
    "HTMLRenderer",
]
