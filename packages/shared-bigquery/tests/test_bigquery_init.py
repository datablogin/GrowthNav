"""Tests for growthnav.bigquery package exports."""



class TestPublicAPI:
    """Test suite for public API exports."""

    def test_tenant_bigquery_client_import(self):
        """Test TenantBigQueryClient is importable from growthnav.bigquery."""
        from growthnav.bigquery import TenantBigQueryClient

        assert TenantBigQueryClient is not None
        assert hasattr(TenantBigQueryClient, "query")
        assert hasattr(TenantBigQueryClient, "dataset_id")

    def test_customer_registry_import(self):
        """Test CustomerRegistry is importable from growthnav.bigquery."""
        from growthnav.bigquery import CustomerRegistry

        assert CustomerRegistry is not None
        assert hasattr(CustomerRegistry, "get_customer")
        assert hasattr(CustomerRegistry, "get_customers_by_industry")

    def test_customer_import(self):
        """Test Customer is importable from growthnav.bigquery."""
        from growthnav.bigquery import Customer

        assert Customer is not None

    def test_query_validator_import(self):
        """Test QueryValidator is importable from growthnav.bigquery."""
        from growthnav.bigquery import QueryValidator

        assert QueryValidator is not None
        assert hasattr(QueryValidator, "validate")
        assert hasattr(QueryValidator, "sanitize_identifier")

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        import growthnav.bigquery as bq

        assert hasattr(bq, "__all__")
        expected_exports = [
            "TenantBigQueryClient",
            "CustomerRegistry",
            "Customer",
            "QueryValidator",
        ]

        for export in expected_exports:
            assert export in bq.__all__, f"{export} not in __all__"

    def test_wildcard_import(self):
        """Test wildcard import works correctly."""
        # This simulates: from growthnav.bigquery import *
        import growthnav.bigquery as bq

        namespace = {}
        for name in bq.__all__:
            namespace[name] = getattr(bq, name)

        assert "TenantBigQueryClient" in namespace
        assert "CustomerRegistry" in namespace
        assert "Customer" in namespace
        assert "QueryValidator" in namespace

    def test_submodules_not_in_all(self):
        """Test that submodules are not in __all__."""
        import growthnav.bigquery as bq

        # Internal modules should not be in __all__
        assert "client" not in bq.__all__
        assert "registry" not in bq.__all__
        assert "validation" not in bq.__all__

    def test_direct_submodule_import(self):
        """Test that submodules can be imported directly if needed."""
        from growthnav.bigquery.client import BigQueryConfig, QueryResult
        from growthnav.bigquery.registry import CustomerStatus, Industry
        from growthnav.bigquery.validation import ValidationResult, ValidationSeverity

        # These should be importable but not in the main __all__
        assert BigQueryConfig is not None
        assert QueryResult is not None
        assert Industry is not None
        assert CustomerStatus is not None
        assert ValidationResult is not None
        assert ValidationSeverity is not None
