"""Tests for growthnav.reporting package exports."""



class TestPackageExports:
    """Test that all expected classes are exported from the package."""

    def test_pdfgenerator_exported(self):
        """Test that PDFGenerator is exported."""
        from growthnav.reporting import PDFGenerator

        assert PDFGenerator is not None

    def test_sheetsexporter_exported(self):
        """Test that SheetsExporter is exported."""
        from growthnav.reporting import SheetsExporter

        assert SheetsExporter is not None

    def test_slidesgenerator_exported(self):
        """Test that SlidesGenerator is exported."""
        from growthnav.reporting import SlidesGenerator

        assert SlidesGenerator is not None

    def test_htmlrenderer_exported(self):
        """Test that HTMLRenderer is exported."""
        from growthnav.reporting import HTMLRenderer

        assert HTMLRenderer is not None

    def test_all_contains_exports(self):
        """Test that __all__ contains all expected exports."""
        import growthnav.reporting

        assert hasattr(growthnav.reporting, "__all__")
        expected = ["PDFGenerator", "SheetsExporter", "SlidesGenerator", "HTMLRenderer"]
        assert set(growthnav.reporting.__all__) == set(expected)

    def test_import_all_classes_together(self):
        """Test importing all classes in one statement."""
        from growthnav.reporting import (
            HTMLRenderer,
            PDFGenerator,
            SheetsExporter,
            SlidesGenerator,
        )

        assert PDFGenerator is not None
        assert SheetsExporter is not None
        assert SlidesGenerator is not None
        assert HTMLRenderer is not None

    def test_no_unexpected_exports(self):
        """Test that only expected items are in __all__."""
        import growthnav.reporting

        # Should only export the main classes
        assert len(growthnav.reporting.__all__) == 4

    def test_pdfgenerator_is_class(self):
        """Test that PDFGenerator is a class."""
        from growthnav.reporting import PDFGenerator

        assert isinstance(PDFGenerator, type)

    def test_sheetsexporter_is_class(self):
        """Test that SheetsExporter is a class."""
        from growthnav.reporting import SheetsExporter

        assert isinstance(SheetsExporter, type)

    def test_slidesgenerator_is_class(self):
        """Test that SlidesGenerator is a class."""
        from growthnav.reporting import SlidesGenerator

        assert isinstance(SlidesGenerator, type)

    def test_htmlrenderer_is_class(self):
        """Test that HTMLRenderer is a class."""
        from growthnav.reporting import HTMLRenderer

        assert isinstance(HTMLRenderer, type)

    def test_slides_content_not_exported(self):
        """Test that SlideContent is not in __all__ (internal use)."""
        import growthnav.reporting

        assert "SlideContent" not in growthnav.reporting.__all__

    def test_slides_layout_not_exported(self):
        """Test that SlideLayout is not in __all__ (internal use)."""
        import growthnav.reporting

        assert "SlideLayout" not in growthnav.reporting.__all__

    def test_can_access_slidecontent_via_module(self):
        """Test that SlideContent can still be accessed via slides module."""
        from growthnav.reporting.slides import SlideContent

        assert SlideContent is not None

    def test_can_access_slidelayout_via_module(self):
        """Test that SlideLayout can still be accessed via slides module."""
        from growthnav.reporting.slides import SlideLayout

        assert SlideLayout is not None

    def test_package_has_docstring(self):
        """Test that package has documentation."""
        import growthnav.reporting

        assert growthnav.reporting.__doc__ is not None
        assert len(growthnav.reporting.__doc__) > 0
