"""Tests for growthnav.conversions public API."""



def test_import_schema_classes():
    """Test that schema classes are importable from top level."""
    from growthnav.conversions import (
        AttributionModel,
        Conversion,
        ConversionSource,
        ConversionType,
    )

    # Verify they are the correct types
    assert hasattr(ConversionSource, "POS")
    assert hasattr(ConversionType, "PURCHASE")
    assert hasattr(AttributionModel, "LAST_CLICK")
    assert hasattr(Conversion, "to_dict")


def test_import_normalizer_classes():
    """Test that normalizer classes are importable from top level."""
    from growthnav.conversions import (
        ConversionNormalizer,
        CRMNormalizer,
        LoyaltyNormalizer,
        POSNormalizer,
    )

    # Verify they are the correct types
    assert hasattr(ConversionNormalizer, "normalize")
    assert hasattr(POSNormalizer, "normalize")
    assert hasattr(CRMNormalizer, "normalize")
    assert hasattr(LoyaltyNormalizer, "normalize")


def test_import_attribution_classes():
    """Test that attribution classes are importable from top level."""
    from growthnav.conversions import (
        AttributionResult,
        attribute_conversions,
    )

    # Verify they are the correct types
    assert callable(attribute_conversions)
    # AttributionResult is a dataclass, check it can be instantiated
    from growthnav.conversions import Conversion
    result = AttributionResult(conversion=Conversion(customer_id="test"), attributed=False)
    assert hasattr(result, "conversion")


def test_all_exports():
    """Test that __all__ contains expected exports."""
    from growthnav.conversions import __all__

    expected_exports = [
        # Schema
        "Conversion",
        "ConversionSource",
        "ConversionType",
        "AttributionModel",
        # Normalizers
        "ConversionNormalizer",
        "POSNormalizer",
        "CRMNormalizer",
        "LoyaltyNormalizer",
        # Attribution
        "attribute_conversions",
        "AttributionResult",
    ]

    for export in expected_exports:
        assert export in __all__, f"{export} not in __all__"


def test_import_all_from_module():
    """Test that 'from growthnav.conversions import *' works correctly."""
    # This would normally be done via: from growthnav.conversions import *
    # But we'll test it more explicitly
    import growthnav.conversions as conversions

    # Test that all expected classes/functions exist
    assert hasattr(conversions, "Conversion")
    assert hasattr(conversions, "ConversionSource")
    assert hasattr(conversions, "ConversionType")
    assert hasattr(conversions, "AttributionModel")
    assert hasattr(conversions, "ConversionNormalizer")
    assert hasattr(conversions, "POSNormalizer")
    assert hasattr(conversions, "CRMNormalizer")
    assert hasattr(conversions, "LoyaltyNormalizer")
    assert hasattr(conversions, "attribute_conversions")
    assert hasattr(conversions, "AttributionResult")


def test_normalizer_inheritance():
    """Test that normalizers inherit from ConversionNormalizer."""
    from growthnav.conversions import (
        ConversionNormalizer,
        CRMNormalizer,
        LoyaltyNormalizer,
        POSNormalizer,
    )

    assert issubclass(POSNormalizer, ConversionNormalizer)
    assert issubclass(CRMNormalizer, ConversionNormalizer)
    assert issubclass(LoyaltyNormalizer, ConversionNormalizer)


def test_enum_accessibility():
    """Test that enum values are accessible."""
    from growthnav.conversions import (
        AttributionModel,
        ConversionSource,
        ConversionType,
    )

    # ConversionSource
    assert ConversionSource.POS == "pos"
    assert ConversionSource.CRM == "crm"
    assert ConversionSource.LOYALTY == "loyalty"

    # ConversionType
    assert ConversionType.PURCHASE == "purchase"
    assert ConversionType.LEAD == "lead"
    assert ConversionType.SIGNUP == "signup"

    # AttributionModel
    assert AttributionModel.LAST_CLICK == "last_click"
    assert AttributionModel.FIRST_CLICK == "first_click"
    assert AttributionModel.LINEAR == "linear"


def test_package_has_docstring():
    """Test that the package has a docstring."""
    import growthnav.conversions

    assert growthnav.conversions.__doc__ is not None
    assert len(growthnav.conversions.__doc__) > 0
    assert "GrowthNav Conversions" in growthnav.conversions.__doc__
