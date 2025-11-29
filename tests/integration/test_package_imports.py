"""Integration tests for package imports."""

import pytest


class TestAllPackagesImportable:
    """Test that all GrowthNav packages can be imported together."""

    def test_bigquery_package_imports(self):
        """BigQuery package classes should be importable."""
        from growthnav.bigquery import TenantBigQueryClient
        from growthnav.bigquery import CustomerRegistry
        from growthnav.bigquery import QueryValidator
        from growthnav.bigquery import Customer
        from growthnav.bigquery import Industry
        from growthnav.bigquery import CustomerStatus

        assert TenantBigQueryClient is not None
        assert CustomerRegistry is not None
        assert QueryValidator is not None

    def test_reporting_package_imports(self):
        """Reporting package classes should be importable."""
        from growthnav.reporting import PDFGenerator
        from growthnav.reporting import SheetsExporter
        from growthnav.reporting import SlidesGenerator
        from growthnav.reporting import HTMLRenderer

        assert PDFGenerator is not None
        assert SheetsExporter is not None
        assert SlidesGenerator is not None

    def test_conversions_package_imports(self):
        """Conversions package classes should be importable."""
        from growthnav.conversions import Conversion
        from growthnav.conversions import POSNormalizer
        from growthnav.conversions import CRMNormalizer
        from growthnav.conversions import LoyaltyNormalizer
        from growthnav.conversions import ConversionType
        from growthnav.conversions import ConversionSource

        assert Conversion is not None
        assert POSNormalizer is not None

    def test_mcp_server_imports(self):
        """MCP server should be importable."""
        from growthnav_mcp.server import mcp
        from growthnav_mcp.server import query_bigquery
        from growthnav_mcp.server import generate_pdf_report

        assert mcp is not None


class TestCrossPackageIntegration:
    """Test that packages work together."""

    def test_enums_from_different_packages(self):
        """Enums from different packages should be usable together."""
        from growthnav.bigquery.registry import Industry
        from growthnav.conversions.schema import ConversionType

        assert Industry.GOLF.value == "golf"
        assert ConversionType.PURCHASE.value == "purchase"

    def test_conversion_with_customer_context(self):
        """Conversions should work with customer registry types."""
        from growthnav.bigquery.registry import Industry
        from growthnav.conversions import Conversion, ConversionType

        # Create a conversion for a customer in the golf industry
        conversion = Conversion(
            customer_id="topgolf",
            conversion_type=ConversionType.PURCHASE,
            value=150.00,
        )

        assert conversion.customer_id == "topgolf"
        assert conversion.value == 150.00

    def test_normalizer_produces_valid_conversions(self):
        """Normalizers should produce valid Conversion objects."""
        from growthnav.conversions import POSNormalizer, Conversion

        normalizer = POSNormalizer(customer_id="test_customer")
        data = [{"order_id": "TXN-001", "total_amount": 100.00}]

        conversions = normalizer.normalize(data)

        assert len(conversions) == 1
        assert isinstance(conversions[0], Conversion)
        assert conversions[0].customer_id == "test_customer"
